package data

type item struct {
	value any
}

type Queue struct {
	items []*item
}

func NewQueue() *Queue {
	return &Queue{
		items: make([]*item, 0),
	}
}

func (q *Queue) Enqueue(value any) {
	q.items = append(q.items, &item{value: value})
}

func (q *Queue) Dequeue() (any, bool) {
	if len(q.items) == 0 {
		return nil, false
	}
	it := q.items[0]
	q.items = q.items[1:]
	return it.value, true
}

func (q *Queue) Size() int {
	return len(q.items)
}

func (q *Queue) IsEmpty() bool {
	return len(q.items) == 0
}
