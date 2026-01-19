package data

import (
	"cmp"
	"math/rand"
	"time"
)

const MAX_LEVEL = 20

var seed = newSeededRand()

func newSeededRand() *rand.Rand {
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}

type cell[S cmp.Ordered] struct {
	prev *column[S]
	next *column[S]
}

type column[S cmp.Ordered] struct {
	cells []cell[S]
	value string
	score S
}

type SortedSet[S cmp.Ordered] struct {
	head   *column[S]
	tail   *column[S]
	scores map[string]S
}

func NewSortedSet[S cmp.Ordered]() *SortedSet[S] {
	set := &SortedSet[S]{
		head:   nil,
		tail:   nil,
		scores: make(map[string]S),
	}
	set.init()
	return set
}

func (s *SortedSet[S]) init() {
	var zero S
	s.head = &column[S]{value: "", score: zero}
	s.tail = &column[S]{value: "", score: zero}
	for lvl := 0; lvl < MAX_LEVEL; lvl += 1 {
		s.head.cells = append(s.head.cells, cell[S]{
			prev: nil,
			next: s.tail,
		})
		s.tail.cells = append(s.tail.cells, cell[S]{
			prev: s.head,
			next: nil,
		})
	}
}

func (s *SortedSet[S]) Len() int {
	return len(s.scores)
}

func (s *SortedSet[S]) IsEmpty() bool {
	return len(s.scores) == 0
}

func (s *SortedSet[S]) Insert(data string, score S) bool {
	return s.insert(data, score)
}

func (s *SortedSet[S]) insert(value string, score S) bool {
	oldScore, exists := s.scores[value]
	if exists && oldScore == score {
		return true
	}

	isUpdate := false

	col := &column[S]{nil, value, score}
	if exists {
		isUpdate = true
		s.remove(value, oldScore)
	}

	// level 0, add levels bottom up
	col.cells = append(col.cells, cell[S]{nil, nil})
	for len(col.cells) < MAX_LEVEL && s.shouldAddLevel() {
		col.cells = append(col.cells, cell[S]{nil, nil})
	}

	iter := s.head
	for lvl := MAX_LEVEL - 1; lvl >= 0; lvl -= 1 {
		// find a lower bound
		for iter.cells[lvl].next != s.tail && s.compare(iter.cells[lvl].next, col) < 0 {
			iter = iter.cells[lvl].next
		}
		// if the current column choose to be included on this level
		if lvl < len(col.cells) {
			next := iter.cells[lvl].next
			iter.cells[lvl].next = col
			next.cells[lvl].prev = col
			col.cells[lvl].next = next
			col.cells[lvl].prev = iter
		}
	}

	s.scores[value] = score
	return isUpdate
}

func (s *SortedSet[S]) Remove(value string) bool {
	score, exists := s.scores[value]
	if !exists {
		return false
	}
	return s.remove(value, score)
}

func (s *SortedSet[S]) remove(value string, score S) bool {
	col := &column[S]{nil, value, score}

	lbCol := s.lowerBound(score, value)
	if s.compare(lbCol, col) != 0 {
		return false
	}

	iter := s.head
	for lvl := MAX_LEVEL - 1; lvl >= 0; lvl -= 1 {
		// find upper bound
		for iter.cells[lvl].next != s.tail && s.compare(iter.cells[lvl].next, col) <= 0 {
			iter = iter.cells[lvl].next
		}

		// connect prev with next
		if iter == lbCol {
			prev := iter.cells[lvl].prev
			next := iter.cells[lvl].next
			prev.cells[lvl].next = next
			next.cells[lvl].prev = prev
		}
	}

	delete(s.scores, value)
	return true
}

func (s *SortedSet[S]) shouldAddLevel() bool {
	return seed.Int()%2 > 0
}

func (s *SortedSet[S]) compare(l *column[S], r *column[S]) int {
	return cmp.Or(
		cmp.Compare(l.score, r.score),
		cmp.Compare(l.value, r.value),
	)
}

// lowerBound finds the largest column with a score that is smaller/equal than the score
func (s *SortedSet[S]) lowerBound(score S, value string) *column[S] {
	col := s.head
	cmpCol := &column[S]{nil, value, score}
	for lvl := MAX_LEVEL - 1; lvl >= 0; lvl -= 1 {
		for col.cells[lvl].next != s.tail && s.compare(col.cells[lvl].next, cmpCol) < 0 {
			col = col.cells[lvl].next
		}
	}
	return col.cells[0].next
}

type Node[S cmp.Ordered] struct {
	Score S
	Value string
}

func (s *SortedSet[S]) Range(lidx int, ridx int) []Node[S] {
	return s.getRange(lidx, ridx)
}

func (s *SortedSet[S]) getRange(lidx int, ridx int) []Node[S] {
	if lidx >= ridx {
		return nil
	}
	result := make([]Node[S], 0, ridx-lidx+1)
	iter := s.head.cells[0].next
	for i := 0; i < len(s.scores); i += 1 {
		if iter == s.tail {
			break
		}
		if i >= lidx && i < ridx {
			result = append(result, Node[S]{
				Score: iter.score,
				Value: iter.value,
			})
		}
		iter = iter.cells[0].next
	}

	return result
}

func (s *SortedSet[S]) Score(value string) (S, bool) {
	score, exists := s.scores[value]
	return score, exists
}

func (s *SortedSet[S]) Rank(value string) (int, bool) {
	score, exists := s.scores[value]
	if !exists {
		return -1, false
	}
	return s.rank(value, score), true
}

func (s *SortedSet[S]) rank(value string, score S) int {
	col := s.lowerBound(score, value)
	if s.compare(col, &column[S]{nil, value, score}) != 0 {
		panic("rank called on non-existing member")
	}
	rank := 0
	for iter := s.head.cells[0].next; iter != col; iter = iter.cells[0].next {
		rank += 1
	}
	return rank
}

func (s *SortedSet[S]) RangeByScore(min S, max S) []Node[S] {
	return s.getRangeByScore(min, max)
}

func (s *SortedSet[S]) getRangeByScore(min S, max S) []Node[S] {
	result := make([]Node[S], 0)
	iter := s.lowerBound(min, "")
	for iter != s.tail && cmp.Compare(iter.score, max) <= 0 {
		result = append(result, Node[S]{
			Score: iter.score,
			Value: iter.value,
		})
		iter = iter.cells[0].next
	}
	return result
}

func (s *SortedSet[S]) ReverseRange(lidx int, ridx int) []Node[S] {
	return s.getReverseRange(lidx, ridx)
}

func (s *SortedSet[S]) getReverseRange(lidx int, ridx int) []Node[S] {
	if lidx >= ridx {
		return nil
	}
	result := make([]Node[S], 0, ridx-lidx+1)
	iter := s.tail.cells[0].prev
	total := len(s.scores)
	for i := total - 1; i >= 0; i -= 1 {
		if iter == s.head {
			break
		}
		if i >= lidx && i < ridx {
			result = append(result, Node[S]{
				Score: iter.score,
				Value: iter.value,
			})
		}
		iter = iter.cells[0].prev
	}
	return result
}

func (s *SortedSet[S]) ReverseRank(value string) (int, bool) {
	score, exists := s.scores[value]
	if !exists {
		return -1, false
	}
	return s.reverseRank(value, score), true
}

func (s *SortedSet[S]) reverseRank(value string, score S) int {
	col := s.lowerBound(score, value)
	if s.compare(col, &column[S]{nil, value, score}) != 0 {
		panic("reverseRank called on non-existing member")
	}
	rank := 0
	for iter := s.tail.cells[0].prev; iter != col; iter = iter.cells[0].prev {
		rank += 1
	}
	return rank
}

func (s *SortedSet[S]) IncrementScore(value string, delta S) (S, bool) {
	score, exists := s.scores[value]
	if exists {
		s.Remove(value)
	}
	newScore := score + delta
	s.Insert(value, newScore)
	return newScore, exists
}

func (s *SortedSet[S]) CountByScore(min S, max S) int {
	count := 0
	iter := s.lowerBound(min, "")
	for iter != s.tail && cmp.Compare(iter.score, max) <= 0 {
		count++
		iter = iter.cells[0].next
	}
	return count
}

func (s *SortedSet[S]) RemoveByScore(min S, max S) int {
	removed := 0
	iter := s.lowerBound(min, "")
	for iter != s.tail && cmp.Compare(iter.score, max) <= 0 {
		next := iter.cells[0].next
		s.remove(iter.value, iter.score)
		removed++
		iter = next
	}
	return removed
}
