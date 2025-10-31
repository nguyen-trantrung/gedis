package data

import (
	"regexp"
	"time"
)

type HashMap struct {
	d map[any]any
	// time.Time already uses monotonic clock for Add, Sub
	// so clock drift safe here
	expires map[any]time.Time
}

func NewHashMap() *HashMap {
	return &HashMap{
		d:       make(map[any]any),
		expires: make(map[any]time.Time),
	}
}

func (h *HashMap) Set(key any, value any, ttl int) bool {
	_, exists := h.d[key]
	var evicted bool
	if exists {
		evicted = h.evict(key)
	}
	h.d[key] = value
	if ttl > 0 {
		h.expires[key] = time.Now().Add(time.Duration(ttl) * time.Millisecond)
	}
	return !evicted && exists
}

func (h *HashMap) Get(key any) (any, bool) {
	val, exists := h.d[key]
	if !exists {
		return nil, false
	}
	if h.evict(key) {
		return nil, false
	}
	return val, true
}

func (h *HashMap) Delete(key any) (any, bool) {
	val, exists := h.d[key]
	if !exists {
		return nil, false
	}
	if h.evict(key) {
		return nil, false
	}
	delete(h.d, key)
	delete(h.expires, key)
	return val, true
}

func (h *HashMap) evict(key any) bool {
	expiresAt, yes := h.expires[key]
	if !yes {
		return false
	}

	if expiresAt.Before(time.Now()) {
		delete(h.d, key)
		delete(h.expires, key)
		return true
	}

	return false
}

func (h *HashMap) Evict() (int, bool) {
	now := time.Now()
	evictList := make([]any, 0, len(h.expires))
	for key, expTime := range h.expires {
		if expTime.Before(now) {
			evictList = append(evictList, key)
		}
	}
	for _, key := range evictList {
		delete(h.expires, key)
		delete(h.d, key)
	}
	return len(evictList), len(evictList) != 0
}

func (h *HashMap) List(pattern *regexp.Regexp) []string {
	keys := make([]string, 0)

	for key := range h.d {
		if h.evict(key) {
			continue
		}
		if strKey, ok := key.(string); ok && pattern.MatchString(strKey) {
			keys = append(keys, strKey)
		}
	}
	return keys
}

func (h *HashMap) Expiration() map[string]int {
	exp := make(map[string]int)
	for key, expTime := range h.expires {
		if expTime.After(time.Now()) {
			if strKey, ok := key.(string); ok {
				ttl := int(time.Until(expTime).Milliseconds())
				exp[strKey] = ttl
			}
		}
	}
	return exp
}

func (h *HashMap) SetExpiration(key any, ttl int) bool {
	if _, exists := h.d[key]; !exists {
		return false
	}
	if ttl > 0 {
		h.expires[key] = time.Now().Add(time.Duration(ttl) * time.Millisecond)
	} else {
		delete(h.expires, key)
	}
	return true
}
