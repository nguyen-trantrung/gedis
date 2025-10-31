package data

type node struct {
	value any
	prev  *node
	next  *node
}

type LinkedList struct {
	head *node
	tail *node
	size int
}

func NewLinkedList() *LinkedList {
	return &LinkedList{
		head: nil,
		tail: nil,
		size: 0,
	}
}

func (l *LinkedList) LeftPush(value any) {
	n := &node{value: value, prev: nil, next: l.head}
	if l.head != nil {
		l.head.prev = n
	}
	l.head = n
	if l.tail == nil {
		l.tail = n
	}
	l.size += 1
}

func (l *LinkedList) RightPush(value any) {
	n := &node{value: value, prev: l.tail, next: nil}
	if l.tail != nil {
		l.tail.next = n
	}
	l.tail = n
	if l.head == nil {
		l.head = n
	}
	l.size += 1
}

func (l *LinkedList) LeftPop() (any, bool) {
	if l.head == nil {
		return nil, false
	}
	value := l.head.value
	l.head = l.head.next
	if l.head != nil {
		l.head.prev = nil
	} else {
		l.tail = nil
	}
	l.size -= 1
	return value, true
}

func (l *LinkedList) RightPop() (any, bool) {
	if l.tail == nil {
		return nil, false
	}
	value := l.tail.value
	l.tail = l.tail.prev
	if l.tail != nil {
		l.tail.next = nil
	} else {
		l.head = nil
	}
	l.size -= 1
	return value, true
}

func (l *LinkedList) LeftRange(start, stop int) []any {
	if start < 0 {
		start = l.size + start
	}
	if stop < 0 {
		stop = l.size + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= l.size {
		stop = l.size - 1
	}
	if start > stop || l.head == nil {
		return []any{}
	}

	result := make([]any, 0, stop-start+1)
	curr := l.head
	idx := 0
	for curr != nil {
		if idx >= start && idx <= stop {
			result = append(result, curr.value)
		}
		if idx > stop {
			break
		}
		curr = curr.next
		idx += 1
	}
	return result
}

func (l *LinkedList) Len() int {
	return l.size
}

func (l *LinkedList) LeftIndex(index int) (any, bool) {
	if index < 0 {
		index = l.size + index
	}
	if index < 0 || index >= l.size {
		return nil, false
	}

	curr := l.head
	for i := 0; i < index && curr != nil; i += 1 {
		curr = curr.next
	}
	if curr == nil {
		return nil, false
	}
	return curr.value, true
}
