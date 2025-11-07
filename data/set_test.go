package data_test

import (
	"testing"

	"github.com/ttn-nguyen42/gedis/data"
)

// helper to collect range fully
func collectAll(s *data.SortedSet) []data.Node {
	return s.Range(0, s.Len())
}

func TestSortedSet_InsertAndLen(t *testing.T) {
	s := data.NewSortedSet()
	if s.Len() != 0 {
		t.Fatalf("expected empty set length 0, got %d", s.Len())
	}

	replaced := s.Insert("a", 1.0)
	if replaced {
		t.Fatalf("first insert should not be replacement")
	}
	if s.Len() != 1 {
		t.Fatalf("expected length 1, got %d", s.Len())
	}

	// duplicate score different value
	if r := s.Insert("b", 1.0); r {
		t.Fatalf("different member same score should be treated as new")
	}
	if s.Len() != 2 {
		t.Fatalf("expected length 2, got %d", s.Len())
	}

	// update existing value score
	if r := s.Insert("a", 2.5); !r {
		t.Fatalf("updating existing member should return true (replacement)")
	}
	if s.Len() != 2 {
		t.Fatalf("length should remain 2 after score update, got %d", s.Len())
	}

	node, ok := s.Search("a")
	if !ok {
		t.Fatalf("expected to find member 'a'")
	}
	if node.Score != 2.5 {
		t.Fatalf("expected updated score 2.5, got %f", node.Score)
	}
}

func TestSortedSet_OrderByScoreThenValue(t *testing.T) {
	s := data.NewSortedSet()
	s.Insert("b", 1)
	s.Insert("a", 1) // same score, lexicographically smaller
	s.Insert("c", 2)

	all := collectAll(s)
	if len(all) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(all))
	}

	// Expect ordering: score asc, then value asc for ties
	if all[0].Value != "a" || all[0].Score != 1 {
		t.Fatalf("expected first element a:1, got %v:%v", all[0].Value, all[0].Score)
	}
	if all[1].Value != "b" || all[1].Score != 1 {
		t.Fatalf("expected second element b:1, got %v:%v", all[1].Value, all[1].Score)
	}
	if all[2].Value != "c" || all[2].Score != 2 {
		t.Fatalf("expected third element c:2, got %v:%v", all[2].Value, all[2].Score)
	}
}

func TestSortedSet_Remove(t *testing.T) {
	s := data.NewSortedSet()
	s.Insert("x", 9)
	s.Insert("y", 10)
	s.Insert("z", 11)

	if !s.Remove("y") {
		t.Fatalf("expected remove y to succeed")
	}
	if s.Len() != 2 {
		t.Fatalf("expected length 2 after removal, got %d", s.Len())
	}
	if _, ok := s.Search("y"); ok {
		t.Fatalf("did not expect to find removed member y")
	}

	if s.Remove("y") {
		t.Fatalf("removing already removed member should return false")
	}

	all := collectAll(s)
	if len(all) != 2 {
		t.Fatalf("expected 2 members after removal, got %d", len(all))
	}
	if all[0].Value != "x" || all[1].Value != "z" {
		t.Fatalf("unexpected ordering after removal: %v, %v", all[0].Value, all[1].Value)
	}
}

func TestSortedSet_SearchNotFound(t *testing.T) {
	s := data.NewSortedSet()
	if _, ok := s.Search("missing"); ok {
		t.Fatalf("expected search on empty set to fail")
	}
	s.Insert("present", 1)
	if _, ok := s.Search("other"); ok {
		t.Fatalf("expected other to be absent")
	}
}

func TestSortedSet_RangeBounds(t *testing.T) {
	s := data.NewSortedSet()
	s.Insert("one", 1)
	s.Insert("two", 2)
	s.Insert("three", 3)

	// lidx >= ridx should return nil
	if r := s.Range(2, 2); r != nil {
		t.Fatalf("expected nil slice when lidx == ridx")
	}
	if r := s.Range(3, 2); r != nil {
		t.Fatalf("expected nil slice when lidx > ridx")
	}

	r := s.Range(0, 2)
	if len(r) != 2 {
		t.Fatalf("expected first two elements, got %d", len(r))
	}
	if r[0].Value != "one" || r[1].Value != "two" {
		t.Fatalf("unexpected range values: %v %v", r[0].Value, r[1].Value)
	}
}

func TestSortedSet_IsEmpty(t *testing.T) {
	s := data.NewSortedSet()
	// After init, head is not nil, so IsEmpty reports whether underlying head pointer is nil.
	if !s.IsEmpty() {
		t.Fatalf("IsEmpty should be true after initialization")
	}
	// Remove all elements and ensure still false (current implementation only checks head nil)
	s.Insert("a", 1)
	s.Remove("a")
	if !s.IsEmpty() {
		t.Fatalf("IsEmpty should be true after removal")
	}
}
