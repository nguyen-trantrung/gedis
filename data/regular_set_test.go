package data

import (
	"sort"
	"testing"
)

func TestNewSet(t *testing.T) {
	s := NewSet()
	if s == nil {
		t.Fatal("NewSet returned nil")
	}
	if s.Len() != 0 {
		t.Errorf("New set should be empty, got length %d", s.Len())
	}
}

func TestSet_Add(t *testing.T) {
	s := NewSet()
	if !s.Add("a") {
		t.Error("Add should return true for new element")
	}
	if s.Len() != 1 {
		t.Errorf("Set length should be 1, got %d", s.Len())
	}
	if !s.Contains("a") {
		t.Error("Set should contain added element")
	}

	if s.Add("a") {
		t.Error("Add should return false for existing element")
	}
	if s.Len() != 1 {
		t.Errorf("Set length should still be 1, got %d", s.Len())
	}
}

func TestSet_Remove(t *testing.T) {
	s := NewSet()
	s.Add("a")

	if !s.Remove("a") {
		t.Error("Remove should return true for existing element")
	}
	if s.Contains("a") {
		t.Error("Set should not contain removed element")
	}
	if s.Len() != 0 {
		t.Errorf("Set length should be 0, got %d", s.Len())
	}

	if s.Remove("a") {
		t.Error("Remove should return false for non-existing element")
	}
}

func TestSet_Members(t *testing.T) {
	s := NewSet()
	elements := []string{"a", "b", "c"}
	for _, e := range elements {
		s.Add(e)
	}

	members := s.Members()
	if len(members) != len(elements) {
		t.Errorf("Members returned %d elements, want %d", len(members), len(elements))
	}

	sort.Strings(members)
	if members[0] != "a" || members[1] != "b" || members[2] != "c" {
		t.Errorf("Members returned unexpected elements: %v", members)
	}
}

func TestSet_Operations(t *testing.T) {
	s1 := NewSet()
	s1.Add("a")
	s1.Add("b")

	s2 := NewSet()
	s2.Add("b")
	s2.Add("c")

	t.Run("Intersect", func(t *testing.T) {
		inter := s1.Intersect(s2)
		if inter.Len() != 1 || !inter.Contains("b") {
			t.Errorf("Intersect failed, got %v", inter.Members())
		}
	})

	t.Run("Union", func(t *testing.T) {
		union := s1.Union(s2)
		if union.Len() != 3 || !union.Contains("a") || !union.Contains("b") || !union.Contains("c") {
			t.Errorf("Union failed, got %v", union.Members())
		}
	})

	t.Run("Difference", func(t *testing.T) {
		diff := s1.Difference(s2)
		if diff.Len() != 1 || !diff.Contains("a") {
			t.Errorf("Difference failed, got %v", diff.Members())
		}
	})
}

func TestSet_Clear(t *testing.T) {
	s := NewSet()
	s.Add("a")
	s.Clear()
	if !s.IsEmpty() {
		t.Error("Set should be empty after Clear")
	}
}

func TestSet_Random(t *testing.T) {
	s := NewSet()
	for _, v := range []string{"a", "b", "c", "d", "e"} {
		s.Add(v)
	}

	randMembers := s.RandomMembers(3)
	if len(randMembers) != 3 {
		t.Errorf("RandomMembers returned %d items, want 3", len(randMembers))
	}

	popped := s.PopRandom(2)
	if len(popped) != 2 {
		t.Errorf("PopRandom returned %d items, want 2", len(popped))
	}
	if s.Len() != 3 {
		t.Errorf("Set should have 3 items left, got %d", s.Len())
	}
}

func TestSet_Random_EdgeCases(t *testing.T) {
	s := NewSet()
	s.Add("a")
	s.Add("b")
	s.Add("c")

	// Test requesting more members than available
	allMembers := s.RandomMembers(100)
	if len(allMembers) != 3 {
		t.Errorf("RandomMembers(100) returned %d items, want 3", len(allMembers))
	}

	// Test requesting 0 members
	zeroMembers := s.RandomMembers(0)
	if len(zeroMembers) != 0 {
		t.Errorf("RandomMembers(0) returned %d items, want 0", len(zeroMembers))
	}

	// Test PopRandom with count > len
	poppedMore := s.PopRandom(5)
	if len(poppedMore) != 3 {
		t.Errorf("PopRandom(5) on 3-item set returned %d items", len(poppedMore))
	}
	if !s.IsEmpty() {
		t.Error("Set should be empty after popping all items")
	}

	// Test PopRandom with empty set
	poppedEmpty := s.PopRandom(1)
	if len(poppedEmpty) != 0 {
		t.Errorf("PopRandom(1) on empty set returned %d items", len(poppedEmpty))
	}
}
