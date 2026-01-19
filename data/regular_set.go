package data

type Set struct {
	members map[string]bool
}

func NewSet() *Set {
	return &Set{
		members: make(map[string]bool),
	}
}

func (s *Set) Add(member string) bool {
	_, exists := s.members[member]
	s.members[member] = true
	return !exists
}

func (s *Set) Remove(member string) bool {
	_, exists := s.members[member]
	if exists {
		delete(s.members, member)
	}
	return exists
}

func (s *Set) Contains(member string) bool {
	_, exists := s.members[member]
	return exists
}

func (s *Set) Members() []string {
	members := make([]string, 0, len(s.members))
	for member := range s.members {
		members = append(members, member)
	}
	return members
}

func (s *Set) Len() int {
	return len(s.members)
}

func (s *Set) IsEmpty() bool {
	return len(s.members) == 0
}

func (s *Set) Clear() {
	s.members = make(map[string]bool)
}

func (s *Set) Intersect(other *Set) *Set {
	result := NewSet()
	for member := range s.members {
		if other.Contains(member) {
			result.Add(member)
		}
	}
	return result
}

func (s *Set) Union(other *Set) *Set {
	result := NewSet()
	for member := range s.members {
		result.Add(member)
	}
	for member := range other.members {
		result.Add(member)
	}
	return result
}

func (s *Set) Difference(other *Set) *Set {
	result := NewSet()
	for member := range s.members {
		if !other.Contains(member) {
			result.Add(member)
		}
	}
	return result
}

func (s *Set) RandomMembers(count int) []string {
	if count <= 0 {
		return []string{}
	}
	members := s.Members()
	if count >= len(members) {
		return members
	}
	// Shuffle and take first count
	seed.Shuffle(len(members), func(i, j int) {
		members[i], members[j] = members[j], members[i]
	})
	return members[:count]
}

func (s *Set) PopRandom(count int) []string {
	if count <= 0 || s.IsEmpty() {
		return []string{}
	}
	members := s.Members()
	if count >= len(members) {
		result := members
		s.Clear()
		return result
	}
	// Shuffle and take first count, remove them
	seed.Shuffle(len(members), func(i, j int) {
		members[i], members[j] = members[j], members[i]
	})
	result := members[:count]
	for _, member := range result {
		s.Remove(member)
	}
	return result
}
