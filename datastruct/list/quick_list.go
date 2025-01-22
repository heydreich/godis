package list

import "container/list"

const pageSize = 1024

type QuickList struct {
	data *list.List
	size int
}

func (ql *QuickList) Get(index int) (val interface{}) {
	if index < 0 || index >= ql.size {
		panic("`index` out of range")
	}
	iter := ql.find(index)
	return iter.get()
}

func (ql *QuickList) Set(index int, val interface{}) {
	if index < 0 || index >= ql.size {
		panic("`index` out of range")
	}

	iter := ql.find(index)
	iter.set(val)
}

func (ql *QuickList) Add(val interface{}) {
	ql.size++
	if ql.data.Len() == 0 {
		page := make([]interface{}, 0, pageSize)
		page = append(page, val)
		ql.data.PushBack(page)
		return
	}
	backNode := ql.data.Back()
	backPage := backNode.Value.([]interface{})
	if len(backPage) == pageSize {
		page := make([]interface{}, 0, pageSize)
		page = append(page, val)
		ql.data.PushBack(page)
		return
	}
	backPage = append(backPage, val)
	backNode.Value = backPage
}

func (ql *QuickList) Insert(index int, val interface{}) {
	if index < 0 || index > ql.size {
		panic("`index` out of range")
	}
	if index == ql.size {
		ql.Add(val)
		return
	}
	iter := ql.find(index)
	page := iter.page()

	if len(page) < pageSize {
		page = append(page[:iter.offset+1], page[iter.offset:]...)
		page[iter.offset] = val
		iter.node.Value = page
		ql.size++
		return
	}

	nextPage := make([]interface{}, 0, pageSize)
	nextPage = append(nextPage, page[pageSize/2:]...)
	page = page[:pageSize/2]
	if iter.offset < len(page) {
		page = append(page[:iter.offset+1], page[iter.offset:]...)
		page[iter.offset] = val
	} else {
		newOffset := iter.offset - pageSize/2
		page = append(page[:newOffset+1], page[newOffset:]...)
		page[newOffset] = val
	}
	iter.node.Value = page
	ql.data.InsertAfter(nextPage, iter.node)
	ql.size++
}

func (ql *QuickList) Remove(index int) (val interface{}) {
	if index < 0 || index >= ql.size {
		panic("`index` out of range")
	}
	iter := ql.find(index)
	return iter.remove()
}

func (ql *QuickList) RemoveLast() (val interface{}) {
	if ql.size == 0 {
		return nil
	}

	iter := ql.find(ql.size - 1)
	return iter.remove()
}
func (ql *QuickList) RemoveAllByVal(expected Expected) int {
	if ql.size == 0 {
		return 0
	}
	iter := ql.find(0)
	removed := 0
	for !iter.atEnd() {
		if expected(iter.get()) {
			iter.remove()
			removed++
		} else {
			iter.next()
		}
	}

	return removed
}
func (ql *QuickList) RemoveByVal(expected Expected, count int) int {
	if ql.size == 0 {
		return 0
	}
	iter := ql.find(0)
	removed := 0
	for !iter.atEnd() {
		if expected(iter.get()) {
			iter.remove()
			removed++
		} else {
			iter.next()
		}
		if removed == count {
			break
		}
	}

	return removed
}
func (ql *QuickList) ReverseRemoveByVal(expected Expected, count int) int {
	if ql.size == 0 {
		return 0
	}
	iter := ql.find(ql.size - 1)
	removed := 0
	for !iter.atBegin() {
		if expected(iter.get()) {
			iter.remove()
			removed++
			if removed == count {
				break
			}
		}
		iter.prev()
	}

	return removed
}
func (ql *QuickList) Len() int {
	return ql.size
}
func (ql *QuickList) ForEach(consumer Consumer) {
	if ql == nil {
		panic("list is nil")
	}

	if ql.Len() == 0 {
		return
	}

	iter := ql.find(0)
	i := 0
	for {
		goNext := consumer(i, iter.get())
		if !goNext {
			break
		}
		i++
		if !iter.next() {
			break
		}
	}
}
func (ql *QuickList) Contains(expected Expected) bool {
	if ql == nil {
		panic("list is nil")
	}

	if ql.Len() == 0 {
		return false
	}
	var contains = false
	iter := ql.find(0)
	for {
		ok := expected(iter.get())
		if ok {
			contains = true
			break
		}
		if !iter.next() {
			break
		}
	}
	return contains
}
func (ql *QuickList) Range(start int, stop int) []interface{} {
	if start < 0 || start >= ql.Len() {
		panic("`start` out of range")
	}
	if stop < start || stop > ql.Len() {
		panic("`stop` out of range")
	}
	if ql == nil {
		panic("list is nil")
	}

	if ql.Len() == 0 {
		return nil
	}

	var valSlice = make([]interface{}, 0, stop-start)
	iter := ql.find(start)
	for i := start; i < stop; i++ {
		valSlice = append(valSlice, iter.get())
		iter.next()
	}
	return valSlice
}
