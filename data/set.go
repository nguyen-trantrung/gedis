package data

import (
	"math/rand"
	"strings"
	"time"
)

const MAX_LEVEL = 20

var seed = newSeededRand()

func newSeededRand() *rand.Rand {
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}

type cell struct {
	prev *column
	next *column
}

type column struct {
	cells []cell
	value string
	score float64
}

type SortedSet struct {
	head   *column
	tail   *column
	scores map[string]float64
}

func NewSortedSet() *SortedSet {
	set := &SortedSet{
		head:   nil,
		tail:   nil,
		scores: make(map[string]float64),
	}
	set.init()
	return set
}

func (s *SortedSet) init() {
	s.head = &column{value: "", score: 0}
	s.tail = &column{value: "", score: 0}
	for lvl := 0; lvl < MAX_LEVEL; lvl += 1 {
		s.head.cells = append(s.head.cells, cell{
			prev: nil,
			next: s.tail,
		})
		s.tail.cells = append(s.tail.cells, cell{
			prev: s.head,
			next: nil,
		})
	}
}

func (s *SortedSet) Len() int {
	return len(s.scores)
}

func (s *SortedSet) IsEmpty() bool {
	return len(s.scores) == 0
}

func (s *SortedSet) Insert(data string, score float64) bool {
	return s.insert(data, score)
}

func (s *SortedSet) insert(value string, score float64) bool {
	oldScore, exists := s.scores[value]
	if exists && oldScore == score {
		return true
	}

	isUpdate := false

	col := &column{nil, value, score}
	if exists {
		isUpdate = true
		s.remove(value, oldScore)
	}

	// level 0, add levels bottom up
	col.cells = append(col.cells, cell{nil, nil})
	for len(col.cells) < MAX_LEVEL && s.shouldAddLevel() {
		col.cells = append(col.cells, cell{nil, nil})
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

func (s *SortedSet) Remove(value string) bool {
	score, exists := s.scores[value]
	if !exists {
		return false
	}
	return s.remove(value, score)
}

func (s *SortedSet) remove(value string, score float64) bool {
	col := &column{nil, value, score}

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
func (s *SortedSet) shouldAddLevel() bool {
	return seed.Int()%2 > 0
}

func (s *SortedSet) compare(l *column, r *column) int {
	if l.score > r.score {
		return 1
	}
	if l.score < r.score {
		return -1
	}
	return strings.Compare(l.value, r.value)
}

// lowerBound finds the largest column with a score that is smaller/equal than the score
func (s *SortedSet) lowerBound(score float64, value string) *column {
	col := s.head
	cmpCol := &column{nil, value, score}
	for lvl := MAX_LEVEL - 1; lvl >= 0; lvl -= 1 {
		for col.cells[lvl].next != s.tail && s.compare(col.cells[lvl].next, cmpCol) < 0 {
			col = col.cells[lvl].next
		}
	}
	return col.cells[0].next
}

type Node struct {
	Score float64
	Value string
}

func (s *SortedSet) Range(lidx int, ridx int) []Node {
	return s.getRange(lidx, ridx)
}

func (s *SortedSet) getRange(lidx int, ridx int) []Node {
	if lidx >= ridx {
		return nil
	}
	result := make([]Node, 0, ridx-lidx+1)
	iter := s.head.cells[0].next
	for i := 0; i < len(s.scores); i += 1 {
		if iter == s.tail {
			break
		}
		if i >= lidx && i < ridx {
			result = append(result, Node{
				Score: iter.score,
				Value: iter.value,
			})
		}
		iter = iter.cells[0].next
	}

	return result
}

func (s *SortedSet) Score(value string) (float64, bool) {
	score, exists := s.scores[value]
	return score, exists
}

func (s *SortedSet) Rank(value string) (int, bool) {
	score, exists := s.scores[value]
	if !exists {
		return -1, false
	}
	return s.rank(value, score), true
}

func (s *SortedSet) rank(value string, score float64) int {
	col := s.lowerBound(score, value)
	if s.compare(col, &column{nil, value, score}) != 0 {
		panic("rank called on non-existing member")
	}
	rank := 0
	for iter := s.head.cells[0].next; iter != col; iter = iter.cells[0].next {
		rank += 1
	}
	return rank
}
