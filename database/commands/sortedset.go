package commands

import (
	"godis/database/engine"
	"godis/datastruct/sortedset"
	"godis/interface/database"
	"godis/interface/redis"
	"godis/redis/protocol"
	"strconv"
	"strings"
)

func init() {
	engine.RegisterCommand("ZAdd", execZAdd, writeFirstKey, -4, engine.FlagWrite)
	engine.RegisterCommand("ZCard", execZCard, readFirstKey, 2, engine.FlagReadOnly)
	engine.RegisterCommand("ZScore", execZScore, readFirstKey, 3, engine.FlagReadOnly)
	engine.RegisterCommand("ZCount", execZCount, readFirstKey, 4, engine.FlagReadOnly)
	engine.RegisterCommand("ZIncrBy", execZIncrBy, writeFirstKey, 4, engine.FlagWrite)
	engine.RegisterCommand("ZRange", execZRange, readFirstKey, -4, engine.FlagReadOnly)
	engine.RegisterCommand("ZRevRange", execZRevRange, readFirstKey, -4, engine.FlagReadOnly)
	engine.RegisterCommand("ZRangeByScore", execZRangeByScore, readFirstKey, -4, engine.FlagReadOnly)
	engine.RegisterCommand("ZRevRangeByScore", execZRevRangeByScore, readFirstKey, -4, engine.FlagReadOnly)
	engine.RegisterCommand("ZRank", execZRank, readFirstKey, 3, engine.FlagReadOnly)
	engine.RegisterCommand("ZRevRank", execZRevRank, readFirstKey, 3, engine.FlagReadOnly)
	engine.RegisterCommand("ZRem", execZRem, writeFirstKey, -3, engine.FlagWrite)
	engine.RegisterCommand("ZRemRangeByRank", execRemRangeByRank, writeFirstKey, 4, engine.FlagWrite)
	engine.RegisterCommand("ZRemRangeByScore", execRemRangeByScore, writeFirstKey, 4, engine.FlagWrite)
}

func execZAdd(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	if len(args)%2 != 1 {
		return protocol.MakeSyntaxErrReply(), nil
	}

	key := string(args[0])
	size := (len(args) - 1) / 2
	elements := make([]*sortedset.Element, size)

	for i := 0; i < size; i++ {
		scoreValue := args[2*i+1]
		member := string(args[2*i+2])
		score, err := strconv.ParseFloat(string(scoreValue), 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not a valid float"), nil
		}
		elements[i] = &sortedset.Element{
			Member: member,
			Score:  score,
		}
	}

	sortedSet, _, errReply := getOrInitSortedSet(db, key)
	if errReply != nil {
		return errReply, nil
	}

	i := 0
	for _, e := range elements {
		if sortedSet.Add(e.Member, e.Score) {
			i++
		}
	}

	return protocol.MakeIntReply(int64(i)), &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: nil,
	}
}

func execZCard(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])

	sortedSet, errReply := getAsSortedSet(db, key)
	if errReply != nil {
		return errReply, nil
	}

	if sortedSet == nil {
		return protocol.MakeIntReply(0), nil
	}
	length := sortedSet.Len()
	return protocol.MakeIntReply(length), nil
}

func execZScore(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	member := string(args[1])

	sortedSet, errReply := getAsSortedSet(db, key)
	if errReply != nil {
		return errReply, nil
	}

	if sortedSet == nil {
		return protocol.MakeNullBulkReply(), nil
	}

	element, ok := sortedSet.Get(member)
	if !ok {
		return protocol.MakeNullBulkReply(), nil
	}

	score := strconv.FormatFloat(element.Score, 'f', -1, 64)
	return protocol.MakeBulkReply([]byte(score)), nil
}

func execZCount(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	min, err := sortedset.ParseScoreBorder(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply(err.Error()), nil
	}

	max, err := sortedset.ParseScoreBorder(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply(err.Error()), nil
	}

	sortedSet, errReply := getAsSortedSet(db, key)
	if errReply != nil {
		return errReply, nil
	}

	if sortedSet == nil {
		return protocol.MakeIntReply(0), nil
	}

	return protocol.MakeIntReply(sortedSet.Count(min, max)), nil
}

func execZIncrBy(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	rawDelta := string(args[1])
	field := string(args[2])
	delta, err := strconv.ParseFloat(rawDelta, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not a valid float"), nil
	}

	sortedSet, _, errReply := getOrInitSortedSet(db, key)
	if errReply != nil {
		return errReply, nil
	}

	element, exists := sortedSet.Get(field)
	if !exists {
		sortedSet.Add(field, delta)

		return protocol.MakeBulkReply(args[1]), &engine.AofExpireCtx{NeedAof: true}
	}

	score := element.Score + delta
	sortedSet.Add(field, score)
	bytes := []byte(strconv.FormatFloat(score, 'f', -1, 64))

	return protocol.MakeBulkReply(bytes), &engine.AofExpireCtx{NeedAof: true}
}

func execZRange(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	if len(args) != 3 && len(args) != 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrange' command"), nil
	}
	withScores := false
	if len(args) == 4 {
		if strings.ToUpper(string(args[3])) != "WITHSCORES" {
			return protocol.MakeErrReply("syntax error"), nil
		}
		withScores = true
	}
	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range"), nil
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range"), nil
	}
	return range0(db, key, start, stop, withScores, false)
}
func execZRevRange(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	if len(args) != 3 && len(args) != 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrange' command"), nil
	}
	withScores := false
	if len(args) == 4 {
		if strings.ToUpper(string(args[3])) != "WITHSCORES" {
			return protocol.MakeErrReply("syntax error"), nil
		}
		withScores = true
	}
	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range"), nil
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range"), nil
	}
	return range0(db, key, start, stop, withScores, true)
}

func execZRangeByScore(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrangebyscore' command"), nil
	}
	key := string(args[0])

	min, err := sortedset.ParseScoreBorder(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply(err.Error()), nil
	}

	max, err := sortedset.ParseScoreBorder(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply(err.Error()), nil
	}
	withScores := false
	var offset int64 = 0
	var limit int64 = -1
	if len(args) > 3 {
		for i := 3; i < len(args); {
			s := string(args[i])
			if strings.ToUpper(s) == "WITHSCORES" {
				withScores = true
				i++
			} else if strings.ToUpper(s) == "LIMIT" {
				if len(args) < i+3 {
					return protocol.MakeErrReply("ERR syntax error"), nil
				}
				offset, err = strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return protocol.MakeErrReply("ERR value is not an integer or out of range"), nil
				}
				limit, err = strconv.ParseInt(string(args[i+2]), 10, 64)
				if err != nil {
					return protocol.MakeErrReply("ERR value is not an integer or out of range"), nil
				}
				i += 3
			} else {
				return protocol.MakeErrReply("ERR syntax error"), nil
			}
		}
	}
	return rangeByScore0(db, key, min, max, offset, limit, withScores, false)
}
func execZRevRangeByScore(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrangebyscore' command"), nil
	}
	key := string(args[0])

	min, err := sortedset.ParseScoreBorder(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply(err.Error()), nil
	}

	max, err := sortedset.ParseScoreBorder(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply(err.Error()), nil
	}
	withScores := false
	var offset int64 = 0
	var limit int64 = -1
	if len(args) > 3 {
		for i := 3; i < len(args); {
			s := string(args[i])
			if strings.ToUpper(s) == "WITHSCORES" {
				withScores = true
				i++
			} else if strings.ToUpper(s) == "LIMIT" {
				if len(args) < i+3 {
					return protocol.MakeErrReply("ERR syntax error"), nil
				}
				offset, err = strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return protocol.MakeErrReply("ERR value is not an integer or out of range"), nil
				}
				limit, err = strconv.ParseInt(string(args[i+2]), 10, 64)
				if err != nil {
					return protocol.MakeErrReply("ERR value is not an integer or out of range"), nil
				}
				i += 3
			} else {
				return protocol.MakeErrReply("ERR syntax error"), nil
			}
		}
	}
	return rangeByScore0(db, key, min, max, offset, limit, withScores, true)
}

func execZRank(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	member := string(args[1])

	sortedSet, errReply := getAsSortedSet(db, key)
	if errReply != nil {
		return errReply, nil
	}

	if sortedSet == nil {
		return protocol.MakeNullBulkReply(), nil
	}

	rank := sortedSet.GetRank(member, false)
	if rank < 0 {
		return protocol.MakeNullBulkReply(), nil
	}

	return protocol.MakeIntReply(rank), nil
}

func execZRevRank(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	member := string(args[1])

	sortedSet, errReply := getAsSortedSet(db, key)
	if errReply != nil {
		return errReply, nil
	}

	if sortedSet == nil {
		return protocol.MakeNullBulkReply(), nil
	}

	rank := sortedSet.GetRank(member, true)
	if rank < 0 {
		return protocol.MakeNullBulkReply(), nil
	}

	return protocol.MakeIntReply(rank), nil
}

func execZRem(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	fields := make([]string, len(args)-1)
	fieldArgs := args[1:]
	for i, v := range fieldArgs {
		fields[i] = string(v)
	}

	sortedSet, errReply := getAsSortedSet(db, key)
	if errReply != nil {
		return errReply, nil
	}
	if sortedSet == nil {
		return protocol.MakeIntReply(0), nil
	}

	var deleted int64 = 0
	for _, field := range fields {
		if sortedSet.Remove(field) {
			deleted++
		}
	}
	return protocol.MakeIntReply(deleted), &engine.AofExpireCtx{NeedAof: true}
}

func execRemRangeByRank(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range"), nil
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range"), nil
	}

	// get data
	sortedSet, errReply := getAsSortedSet(db, key)
	if errReply != nil {
		return errReply, nil
	}
	if sortedSet == nil {
		return protocol.MakeIntReply(0), nil
	}

	// compute index
	size := sortedSet.Len() // assert: size > 0
	if start < -1*size {
		start = 0
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return protocol.MakeIntReply(0), nil
	}
	if stop < -1*size {
		stop = 0
	} else if stop < 0 {
		stop = size + stop + 1
	} else if stop < size {
		stop = stop + 1
	} else {
		stop = size
	}
	if stop < start {
		stop = start
	}

	// assert: start in [0, size - 1], stop in [start, size]
	removed := sortedSet.RemoveByRank(start, stop)

	return protocol.MakeIntReply(removed), &engine.AofExpireCtx{NeedAof: true}
}

func execRemRangeByScore(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	if len(args) != 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zremrangebyscore' command"), nil
	}
	key := string(args[0])

	min, err := sortedset.ParseScoreBorder(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply(err.Error()), nil
	}

	max, err := sortedset.ParseScoreBorder(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply(err.Error()), nil
	}

	// get data
	sortedSet, errReply := getAsSortedSet(db, key)
	if errReply != nil {
		return errReply, nil
	}
	if sortedSet == nil {
		return protocol.MakeEmptyMultiBulkReply(), nil
	}

	removed := sortedSet.RemoveByScore(min, max)

	return protocol.MakeIntReply(removed), &engine.AofExpireCtx{NeedAof: true}
}

// todo

func getOrInitSortedSet(db *engine.DB, key string) (sortedSet *sortedset.SortedSet, inited bool, errReply protocol.ErrorReply) {
	sortedSet, errReply = getAsSortedSet(db, key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited = false
	if sortedSet == nil {
		sortedSet = sortedset.MakeSortedSet()
		db.PutEntity(key, &database.DataEntity{
			Data: sortedSet,
		})
		inited = true
	}
	return sortedSet, inited, nil
}

func getAsSortedSet(db *engine.DB, key string) (set *sortedset.SortedSet, errorReply protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	set, ok := entity.Data.(*sortedset.SortedSet)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return set, nil
}

func range0(db *engine.DB, key string, start int64, stop int64, withScores bool, desc bool) (redis.Reply, *engine.AofExpireCtx) {
	sortedSet, errReply := getAsSortedSet(db, key)
	if errReply != nil {
		return errReply, nil
	}
	if sortedSet == nil {
		return protocol.MakeEmptyMultiBulkReply(), nil
	}

	size := sortedSet.Len()
	if start < -1*size {
		start = 0
	} else if start < 0 {
		start = start + size
	} else if start >= size {
		return protocol.MakeEmptyMultiBulkReply(), nil
	}
	if stop < -1*size {
		stop = 0
	} else if stop < 0 {
		stop = size + stop + 1
	} else if stop < size {
		stop = stop + 1
	} else {
		stop = size
	}
	if stop < start {
		stop = start
	}

	slice := sortedSet.Range(start, stop, desc)
	if withScores {
		result := make([][]byte, len(slice)*2)
		i := 0
		for _, element := range slice {
			result[i] = []byte(element.Member)
			i++
			scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
			result[i] = []byte(scoreStr)
			i++
		}
		return protocol.MakeMultiBulkReply(result), nil
	}
	result := make([][]byte, len(slice))
	i := 0
	for _, element := range slice {
		result[i] = []byte(element.Member)
		i++
	}
	return protocol.MakeMultiBulkReply(result), nil
}

func rangeByScore0(db *engine.DB, key string, min *sortedset.ScoreBorder, max *sortedset.ScoreBorder, offset int64, limit int64, withScores bool, desc bool) (redis.Reply, *engine.AofExpireCtx) {
	sortedSet, errReply := getAsSortedSet(db, key)
	if errReply != nil {
		return errReply, nil
	}
	if sortedSet == nil {
		return protocol.MakeEmptyMultiBulkReply(), nil
	}

	slice := sortedSet.RangeByScore(min, max, offset, limit, desc)
	if withScores {
		result := make([][]byte, len(slice)*2)
		i := 0
		for _, element := range slice {
			result[i] = []byte(element.Member)
			i++
			scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
			result[i] = []byte(scoreStr)
			i++
		}
		return protocol.MakeMultiBulkReply(result), nil
	}
	result := make([][]byte, len(slice))
	i := 0
	for _, element := range slice {
		result[i] = []byte(element.Member)
		i++
	}
	return protocol.MakeMultiBulkReply(result), nil
}
