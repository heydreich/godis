package sortedset

import (
	"math/bits"
	"math/rand/v2"
)

const maxLevel = 16

// 对外的元素抽象
type Element struct {
	Member string
	Score  float64
}

type Node struct {
	Element           // 元素的名称和 score
	backward *Node    // 后向指针
	level    []*Level // 前向指针, level[0] 为最下层
}

// 节点中每一层的抽象
type Level struct {
	forward *Node // 指向同层中的下一个节点
	span    int64 // 到 forward 跳过的节点数
}

// 跳表的定义
type skiplist struct {
	header *Node
	tail   *Node
	length int64
	level  int16
}

func makeNode(level int16, score float64, member string) *Node {
	n := &Node{
		Element: Element{
			Score:  score,
			Member: member,
		},
		level: make([]*Level, level),
	}
	for i := range n.level {
		n.level[i] = new(Level)
	}
	return n
}

func makeskiplist() *skiplist {
	return &skiplist{
		level:  1,
		header: makeNode(maxLevel, 0, ""),
	}
}

func randomLevel() int16 {
	total := uint64(1)<<uint64(maxLevel) - 1
	k := rand.Uint64() % total
	return maxLevel - int16(bits.Len64(k)) + 1
}

func (sl *skiplist) getByRank(rank int64) *Node {
	var i int64 = 0
	n := sl.header
	for level := sl.level - 1; level >= 0; level-- {
		for n.level[level].forward != nil && (i+n.level[level].span) <= rank {
			i += n.level[level].span
			n = n.level[level].forward
		}
		for i == rank {
			return n
		}
	}
	return nil
}

func (sl *skiplist) getRank(member string, score float64) int64 {
	var rank int64 = 0
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.Score < score ||
				(x.level[i].forward.Score == score &&
					x.level[i].forward.Member <= member)) {
			rank += x.level[i].span
			x = x.level[i].forward
		}

		/* x might be equal to zsl->header, so test if obj is non-NULL */
		if x.Member == member {
			return rank
		}
	}
	return 0
}

func (sl *skiplist) hasInRange(min *ScoreBorder, max *ScoreBorder) bool {
	if min.Value > max.Value || (min.Value == max.Value && (min.Exclude || max.Exclude)) {
		return false
	}
	n := sl.tail
	if n == nil || !min.less(n.Score) {
		return false
	}
	n = sl.header.level[0].forward
	if n == nil || !max.greater(n.Score) {
		return false
	}
	return true
}

func (sl *skiplist) getFirstInScoreRange(min *ScoreBorder, max *ScoreBorder) *Node {
	if !sl.hasInRange(min, max) {
		return nil
	}

	n := sl.header
	// scan from top level
	for level := sl.level - 1; level >= 0; level-- {
		// if forward is not in range than move forward
		for n.level[level].forward != nil && !min.less(n.level[level].forward.Score) {
			n = n.level[level].forward
		}
	}
	n = n.level[0].forward
	if !max.greater(n.Score) {
		return nil
	}
	return n
}

func (sl *skiplist) getLastInScoreRange(min *ScoreBorder, max *ScoreBorder) *Node {
	if !sl.hasInRange(min, max) {
		return nil
	}

	n := sl.header
	// scan from top level
	for level := sl.level - 1; level >= 0; level-- {
		// if forward is not in range than move forward
		for n.level[level].forward != nil && max.greater(n.level[level].forward.Score) {
			n = n.level[level].forward
		}
	}

	if !min.less(n.Score) {
		return nil
	}
	return n
}

func (sl *skiplist) insert(member string, score float64) *Node {
	update := make([]*Node, maxLevel)
	rank := make([]int64, maxLevel)
	//用 update 数组记录每一层的前驱节点。并且用 rank 数组保存各层先驱节点的排名
	node := sl.header
	for i := sl.level - 1; i >= 0; i++ {
		if i == sl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		if node.level[i] != nil {
			for node.level[i].forward != nil &&
				(node.level[i].forward.Score < score ||
					(node.level[i].forward.Score == score && node.level[i].forward.Member < member)) {
				rank[i] += node.level[i].span
				node = node.level[i].forward
			}
		}
		update[i] = node
	}
	level := randomLevel()
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			rank[i] = 0
			update[i] = sl.header
			update[i].level[i].span = sl.length
		}
		sl.level = level
	}
	node = makeNode(level, score, member)
	for i := int16(0); i < level; i++ {
		node.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = node

		node.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = rank[0] - rank[i] + 1
	}

	for i := level; i < sl.level; i++ {
		update[i].level[i].span++
	}

	if update[0] == sl.header {
		node.backward = nil
	} else {
		node.backward = update[0]
	}
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node
	} else {
		sl.tail = node
	}
	sl.length++
	return node
}

func (sl *skiplist) RemoveRangeByRank(start int64, stop int64) []*Element {
	update := make([]*Node, maxLevel)
	removed := make([]*Element, 0, stop-start)

	node := sl.header
	var rank int64 = 0
	for i := sl.level - 1; i >= 0; i-- {
		if node.level[i] != nil {
			for node.level[i].forward != nil && (node.level[i].span+rank < start) {
				rank += node.level[i].span
				node = node.level[i].forward
			}
			update[i] = node
		}
	}
	rank++
	node = node.level[0].forward
	for node != nil && rank < stop {
		removed = append(removed, &node.Element)
		next := node.level[0].forward
		sl.removeNode(node, update)
		node = next
		rank++
	}
	return removed
}

func (sl *skiplist) removeNode(node *Node, update []*Node) {
	for i := int16(0); i < sl.level; i++ {
		if update[i].level[i].forward == node {
			update[i].level[i].forward = node.level[i].forward
			update[i].level[i].span += node.level[i].span - 1
		} else {
			update[i].level[i].span--
		}
	}

	if node.level[0].forward != nil {
		node.level[0].forward.backward = node.backward
	} else {
		sl.tail = node.backward
	}
	for sl.level > 1 && sl.header.level[sl.level-1].forward == nil {
		sl.level--
	}
	sl.length--
}

func (sl *skiplist) remove(member string, score float64) bool {
	update := make([]*Node, maxLevel)
	rank := make([]int64, maxLevel)
	//用 update 数组记录每一层的前驱节点。并且用 rank 数组保存各层先驱节点的排名
	node := sl.header
	for i := sl.level - 1; i >= 0; i++ {
		if i == sl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		if node.level[i] != nil {
			for node.level[i].forward != nil &&
				(node.level[i].forward.Score < score ||
					(node.level[i].forward.Score == score && node.level[i].forward.Member < member)) {
				rank[i] += node.level[i].span
				node = node.level[i].forward
			}
		}
		update[i] = node
	}
	node = node.level[0].forward
	if node != nil && score == node.Score && node.Member == member {
		sl.removeNode(node, update)
		return true
	}
	return false
}

func (sl *skiplist) RemoveRangeByScore(min *ScoreBorder, max *ScoreBorder, limit int) []*Element {
	update := make([]*Node, maxLevel)
	removed := make([]*Element, 0)

	node := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		if node.level[i] != nil {
			for node.level[i].forward != nil && (!min.less(node.Score)) {
				node = node.level[i].forward
			}
			update[i] = node
		}
	}
	node = node.level[0].forward
	for node != nil && max.greater(node.Score) {
		removed = append(removed, &node.Element)
		next := node.level[0].forward
		sl.removeNode(node, update)
		if limit > 0 && len(removed) == limit {
			break
		}
		node = next
	}
	return removed
}
