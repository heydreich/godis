package list

import "container/list"

type iterator struct {
	node   *list.Element
	offset int
	ql     *QuickList
}

func (ql *QuickList) find(index int) *iterator {
	var node *list.Element
	var pageBeg int
	if index < ql.size/2 {
		node = ql.data.Front()
		pageBeg = 0
		for {
			page := node.Value.([]interface{})
			if pageBeg+len(page) > index {
				break
			}
			pageBeg += len(page)
			node = node.Next()
		}
	} else {
		node = ql.data.Back()
		pageBeg = ql.size
		for {
			page := node.Value.([]interface{})
			if pageBeg-len(page) <= index {
				pageBeg -= len(page)
				break
			}
			pageBeg -= len(page)
			node = node.Prev()
		}
	}
	pageOffset := index - pageBeg
	return &iterator{
		node:   node,
		offset: pageOffset,
		ql:     ql,
	}
}

func (iter *iterator) get() interface{} {
	return iter.page()[iter.offset]
}
func (iter *iterator) page() []interface{} {
	return iter.node.Value.([]interface{})
}

func (iter *iterator) next() bool {
	page := iter.page()
	if iter.offset < len(page)-1 {
		iter.offset += 1
		return true
	} else if iter.node == iter.ql.data.Back() {
		iter.offset = len(page)
		return false
	}
	iter.offset = 0
	iter.node = iter.node.Next()
	return true
}

func (iter *iterator) prev() bool {
	if iter.offset > 0 {
		iter.offset -= 1
		return true
	} else if iter.node == iter.ql.data.Front() {
		iter.offset = -1
		return false
	}
	iter.offset = len(iter.page()) - 1
	iter.node = iter.node.Prev()
	return true
}

func (iter *iterator) set(val interface{}) {
	iter.page()[iter.offset] = val
}

func (iter *iterator) remove() (val interface{}) {
	val = iter.get()
	page := iter.page()
	page = append(page[:iter.offset], page[iter.offset+1:]...)
	if len(page) > 0 {
		iter.node.Value = page
		if iter.offset == len(page) {
			if iter.node != iter.ql.data.Back() {
				iter.node = iter.node.Next()
				iter.offset = 0
			}
		}
	} else {
		if iter.node == iter.ql.data.Back() {
			iter.ql.data.Remove(iter.node)
			iter.node = nil
			iter.offset = 0
		} else {
			nextNode := iter.node.Next()
			iter.ql.data.Remove(iter.node)
			iter.node = nextNode
			iter.offset = 0
		}
	}

	iter.ql.size--
	return val
}

func (iter *iterator) atEnd() bool {
	if iter.ql.data.Len() == 0 {
		return true
	}
	if iter.node != iter.ql.data.Back() {
		return false
	}
	page := iter.page()
	return iter.offset == len(page)
}

// 是否在 -1 位置上
func (iter *iterator) atBegin() bool {
	if iter.ql.data.Len() == 0 {
		return true
	}
	if iter.node != iter.ql.data.Front() {
		return false
	}
	return iter.offset == -1
}
