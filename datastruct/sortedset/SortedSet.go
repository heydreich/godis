package sortedset

import "strconv"

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

func (sortedSet *SortedSet) PopMin(count int) []*Element {
	first := sortedSet.skiplist.getFirstInScoreRange(negativeInfBorder, positiveInfBorder)
	if first == nil {
		return nil
	}
	border := &ScoreBorder{
		Value:   first.Score,
		Exclude: false,
	}
	removed := sortedSet.skiplist.RemoveRangeByScore(border, positiveInfBorder, count)
	for _, element := range removed {
		delete(sortedSet.dict, element.Member)
	}
	return removed
}

// RemoveByScore removes members which score within the given border
func (sortedSet *SortedSet) RemoveByScore(min *ScoreBorder, max *ScoreBorder) int64 {
	removed := sortedSet.skiplist.RemoveRangeByScore(min, max, 0)
	for _, element := range removed {
		delete(sortedSet.dict, element.Member)
	}
	return int64(len(removed))
}

func (sortedSet *SortedSet) ForEach(start int64, stop int64, desc bool, consumer func(element *Element) bool) {
	size := int64(sortedSet.Len())
	if start < 0 || start >= size {
		panic("illegal start " + strconv.FormatInt(start, 10))
	}
	if stop < start || stop > size {
		panic("illegal end " + strconv.FormatInt(stop, 10))
	}

	var node *Node
	if desc {
		node = sortedSet.skiplist.tail
		if start > 0 {
			node = sortedSet.skiplist.getByRank(int64(size - start))
		}
	} else {
		node = sortedSet.skiplist.header.level[0].forward
		if start > 0 {
			node = sortedSet.skiplist.getByRank(int64(start + 1))
		}
	}

	sliceSize := int(stop - start)
	for i := 0; i < sliceSize; i++ {
		if !consumer(&node.Element) {
			break
		}
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}

	}

}

func (sortedSet *SortedSet) Count(min *ScoreBorder, max *ScoreBorder) int64 {
	var i int64 = 0
	sortedSet.ForEach(0, sortedSet.Len(), false, func(element *Element) bool {
		gtMin := min.less(element.Score)
		if !gtMin {
			return true
		}
		ltMax := max.greater(element.Score)
		if !ltMax {
			return false
		}
		i++
		return true
	})
	return i
}

func (sortedSet *SortedSet) RangeByScore(min *ScoreBorder, max *ScoreBorder, offset int64, limit int64, desc bool) []*Element {
	if limit == 0 || offset < 0 {
		return make([]*Element, 0)
	}
	slice := make([]*Element, 0)
	sortedSet.ForEachByScore(min, max, offset, limit, desc, func(element *Element) bool {
		slice = append(slice, element)
		return true
	})
	return slice
}

// ForEachByScore visits members which score within the given border
func (sortedSet *SortedSet) ForEachByScore(min *ScoreBorder, max *ScoreBorder, offset int64, limit int64, desc bool, consumer func(element *Element) bool) {
	// find start node
	var node *Node
	if desc {
		node = sortedSet.skiplist.getLastInScoreRange(min, max)
	} else {
		node = sortedSet.skiplist.getFirstInScoreRange(min, max)
	}
	// 为了实现分页，offset就是新的一页的开头
	for node != nil && offset > 0 {
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
		offset--
	}

	// A negative limit returns all elements from the offset
	for i := 0; (i < int(limit) || limit < 0) && node != nil; i++ {
		if !consumer(&node.Element) {
			break
		}
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
		if node == nil {
			break
		}
		gtMin := min.less(node.Element.Score) // greater than min
		ltMax := max.greater(node.Element.Score)
		if !gtMin || !ltMax {
			break // break through score border
		}
	}
}

func (sortedSet *SortedSet) Range(start int64, stop int64, desc bool) []*Element {
	sliceSize := int(stop - start)
	slice := make([]*Element, sliceSize)
	i := 0
	sortedSet.ForEach(start, stop, desc, func(element *Element) bool {
		slice[i] = element
		i++
		return true
	})
	return slice
}
