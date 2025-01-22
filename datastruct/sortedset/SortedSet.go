package sortedset

type SortedSet struct {
	dict     map[string]*Element
	skiplist *skiplist
}

func MakeSortedSet() *SortedSet {
	return &SortedSet{
		dict:     make(map[string]*Element),
		skiplist: makeskiplist(),
	}
}

// Add puts member into set,  and returns whether has inserted new node
func (ss *SortedSet) Add(member string, score float64) bool {
	element, ok := ss.dict[member]

	ss.dict[member] = &Element{
		Member: member,
		Score:  score,
	}
	if ok {
		if score != element.Score {
			ss.skiplist.remove(member, element.Score)
			ss.skiplist.insert(member, score)
		}
		return false
	}
	ss.skiplist.insert(member, score)
	return true
}

// RemoveByRank removes member ranking within [start, stop)
// sort by ascending order and rank starts from 0
func (ss *SortedSet) RemoveByRank(start int64, stop int64) int64 {
	removed := ss.skiplist.RemoveRangeByRank(start+1, stop+1)
	for _, element := range removed {
		delete(ss.dict, element.Member)
	}
	return int64(len(removed))
}

func (ss *SortedSet) Len() int64 {
	return int64(len(ss.dict))
}

func (ss *SortedSet) Get(member string) (element *Element, ok bool) {
	v, ok := ss.dict[member]
	if !ok {
		return nil, false
	}
	return v, ok
}

func (ss *SortedSet) Remove(member string) bool {
	v, ok := ss.dict[member]
	if !ok {
		return false
	}
	ss.skiplist.remove(member, v.Score)
	delete(ss.dict, member)
	return true
}

// 获取的是在数组中的排位而不是序号.
func (ss *SortedSet) GetRank(member string, desc bool) int64 {
	element, ok := ss.dict[member]
	if !ok {
		return -1
	}
	r := ss.skiplist.getRank(member, element.Score)
	if desc {
		r = ss.skiplist.length - r
	} else {
		r--
	}
	return r
}
