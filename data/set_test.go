package data_test

import (
	"testing"

	"github.com/ttn-nguyen42/gedis/data"
)

// helper to collect range fully
func collectAll(s *data.SortedSet[float64]) []data.Node[float64] {
	return s.Range(0, s.Len())
}

func TestSortedSet_InsertAndLen(t *testing.T) {
	s := data.NewSortedSet[float64]()
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

	score, ok := s.Score("a")
	if !ok {
		t.Fatalf("expected to find member 'a'")
	}
	if score != 2.5 {
		t.Fatalf("expected updated score 2.5, got %f", score)
	}
}

func TestSortedSet_OrderByScoreThenValue(t *testing.T) {
	s := data.NewSortedSet[float64]()
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
	s := data.NewSortedSet[float64]()
	s.Insert("x", 9)
	s.Insert("y", 10)
	s.Insert("z", 11)

	if !s.Remove("y") {
		t.Fatalf("expected remove y to succeed")
	}
	if s.Len() != 2 {
		t.Fatalf("expected length 2 after removal, got %d", s.Len())
	}
	if _, ok := s.Score("y"); ok {
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
	s := data.NewSortedSet[float64]()
	if _, ok := s.Score("missing"); ok {
		t.Fatalf("expected search on empty set to fail")
	}
	s.Insert("present", 1)
	if _, ok := s.Score("other"); ok {
		t.Fatalf("expected other to be absent")
	}
}

func TestSortedSet_RangeBounds(t *testing.T) {
	s := data.NewSortedSet[float64]()
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
	s := data.NewSortedSet[float64]()
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

func TestSortedSet_Rank_NotFound(t *testing.T) {
	s := data.NewSortedSet[float64]()

	// Test on empty set
	rank, ok := s.Rank("missing")
	if ok {
		t.Fatalf("expected Rank to return false for missing member in empty set")
	}
	if rank != -1 {
		t.Fatalf("expected rank -1 for missing member, got %d", rank)
	}

	// Test on non-empty set
	s.Insert("a", 1.0)
	s.Insert("b", 2.0)

	rank, ok = s.Rank("missing")
	if ok {
		t.Fatalf("expected Rank to return false for missing member")
	}
	if rank != -1 {
		t.Fatalf("expected rank -1 for missing member, got %d", rank)
	}
}

func TestSortedSet_Rank_SingleElement(t *testing.T) {
	s := data.NewSortedSet[float64]()
	s.Insert("only", 5.0)

	rank, ok := s.Rank("only")
	if !ok {
		t.Fatalf("expected to find member 'only'")
	}
	if rank != 0 {
		t.Fatalf("expected rank 0 for single element, got %d", rank)
	}
}

func TestSortedSet_Rank_UniqueScores(t *testing.T) {
	s := data.NewSortedSet[float64]()

	// Insert in non-sorted order
	s.Insert("third", 3.0)
	s.Insert("first", 1.0)
	s.Insert("fifth", 5.0)
	s.Insert("second", 2.0)
	s.Insert("fourth", 4.0)

	tests := []struct {
		member       string
		expectedRank int
	}{
		{"first", 0},
		{"second", 1},
		{"third", 2},
		{"fourth", 3},
		{"fifth", 4},
	}

	for _, tt := range tests {
		rank, ok := s.Rank(tt.member)
		if !ok {
			t.Fatalf("expected to find member '%s'", tt.member)
		}
		if rank != tt.expectedRank {
			t.Fatalf("member '%s': expected rank %d, got %d", tt.member, tt.expectedRank, rank)
		}
	}
}

func TestSortedSet_Rank_SameScore_LexicographicOrder(t *testing.T) {
	s := data.NewSortedSet[float64]()

	// All have the same score, should be ordered lexicographically
	s.Insert("charlie", 1.0)
	s.Insert("alice", 1.0)
	s.Insert("bob", 1.0)
	s.Insert("david", 1.0)

	tests := []struct {
		member       string
		expectedRank int
	}{
		{"alice", 0}, // lexicographically first
		{"bob", 1},
		{"charlie", 2},
		{"david", 3}, // lexicographically last
	}

	for _, tt := range tests {
		rank, ok := s.Rank(tt.member)
		if !ok {
			t.Fatalf("expected to find member '%s'", tt.member)
		}
		if rank != tt.expectedRank {
			t.Fatalf("member '%s': expected rank %d, got %d", tt.member, tt.expectedRank, rank)
		}
	}
}

func TestSortedSet_Rank_MixedScores(t *testing.T) {
	s := data.NewSortedSet[float64]()

	// Mixed: some same scores, some unique
	s.Insert("low1", 1.0)
	s.Insert("low2", 1.0) // same as low1
	s.Insert("mid", 5.0)  // unique
	s.Insert("high1", 10.0)
	s.Insert("high2", 10.0) // same as high1
	s.Insert("high3", 10.0) // same as high1, high2

	// Expected order: low1(1.0), low2(1.0), mid(5.0), high1(10.0), high2(10.0), high3(10.0)
	// But with same scores, lexicographic ordering applies
	tests := []struct {
		member       string
		expectedRank int
	}{
		{"low1", 0},
		{"low2", 1},
		{"mid", 2},
		{"high1", 3},
		{"high2", 4},
		{"high3", 5},
	}

	for _, tt := range tests {
		rank, ok := s.Rank(tt.member)
		if !ok {
			t.Fatalf("expected to find member '%s'", tt.member)
		}
		if rank != tt.expectedRank {
			t.Fatalf("member '%s': expected rank %d, got %d", tt.member, tt.expectedRank, rank)
		}
	}
}

func TestSortedSet_Rank_AfterUpdate(t *testing.T) {
	s := data.NewSortedSet[float64]()

	s.Insert("a", 1.0)
	s.Insert("b", 2.0)
	s.Insert("c", 3.0)

	// Initial ranks: a=0, b=1, c=2
	rank, _ := s.Rank("b")
	if rank != 1 {
		t.Fatalf("expected initial rank 1 for 'b', got %d", rank)
	}

	// Update b's score to make it last
	s.Insert("b", 5.0)

	// New ranks: a=0, c=1, b=2
	rank, ok := s.Rank("b")
	if !ok {
		t.Fatalf("expected to find member 'b' after update")
	}
	if rank != 2 {
		t.Fatalf("expected rank 2 for 'b' after score update, got %d", rank)
	}

	// Verify other ranks shifted
	rank, _ = s.Rank("c")
	if rank != 1 {
		t.Fatalf("expected rank 1 for 'c' after b's update, got %d", rank)
	}
}

func TestSortedSet_Rank_AfterRemoval(t *testing.T) {
	s := data.NewSortedSet[float64]()

	s.Insert("a", 1.0)
	s.Insert("b", 2.0)
	s.Insert("c", 3.0)
	s.Insert("d", 4.0)

	// Remove middle element
	s.Remove("b")

	// New ranks: a=0, c=1, d=2
	rank, ok := s.Rank("c")
	if !ok {
		t.Fatalf("expected to find member 'c'")
	}
	if rank != 1 {
		t.Fatalf("expected rank 1 for 'c' after removal of 'b', got %d", rank)
	}

	rank, ok = s.Rank("d")
	if !ok {
		t.Fatalf("expected to find member 'd'")
	}
	if rank != 2 {
		t.Fatalf("expected rank 2 for 'd' after removal of 'b', got %d", rank)
	}
}

func TestSortedSet_Rank_NegativeScores(t *testing.T) {
	s := data.NewSortedSet[float64]()

	s.Insert("negative", -5.0)
	s.Insert("zero", 0.0)
	s.Insert("positive", 5.0)

	tests := []struct {
		member       string
		expectedRank int
	}{
		{"negative", 0}, // lowest score
		{"zero", 1},
		{"positive", 2}, // highest score
	}

	for _, tt := range tests {
		rank, ok := s.Rank(tt.member)
		if !ok {
			t.Fatalf("expected to find member '%s'", tt.member)
		}
		if rank != tt.expectedRank {
			t.Fatalf("member '%s': expected rank %d, got %d", tt.member, tt.expectedRank, rank)
		}
	}
}

func TestSortedSet_Rank_FloatingPointScores(t *testing.T) {
	s := data.NewSortedSet[float64]()

	s.Insert("low", 1.1)
	s.Insert("mid", 1.5)
	s.Insert("high", 1.9)

	rank, ok := s.Rank("mid")
	if !ok {
		t.Fatalf("expected to find member 'mid'")
	}
	if rank != 1 {
		t.Fatalf("expected rank 1 for 'mid', got %d", rank)
	}
}
