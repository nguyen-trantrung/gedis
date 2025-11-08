package data

import (
	"fmt"

	"github.com/ttn-nguyen42/gedis/data/geohash"
	"github.com/ttn-nguyen42/gedis/util"
)

type GeoIndex struct {
	bits int
	set  *SortedSet[float64]
}

func NewGeoIndex(precision int) *GeoIndex {
	return &GeoIndex{
		bits: precision,
		set:  NewSortedSet[float64](),
	}
}
func NewGeoIndexFromSet(precision int, set *SortedSet[float64]) *GeoIndex {
	return &GeoIndex{
		bits: precision,
		set:  set,
	}
}

func (i *GeoIndex) Add(key string, lat, lon float64) (bool, error) {
	err := i.validate(lat, lon)
	if err != nil {
		return false, err
	}
	return i.add(key, lat, lon), nil
}

func (i *GeoIndex) add(key string, lat, lon float64) bool {
	hash := geohash.Encode(lat, lon, i.bits)
	return i.set.Insert(key, float64(hash))
}

func (i *GeoIndex) validate(lat, lon float64) error {
	if lat <= -85.05112878 || lat >= 85.05112878 {
		return fmt.Errorf("invalid latitude value")
	}
	if lon <= -180.0 || lon >= 180.0 {
		return fmt.Errorf("invalid longitude value")
	}
	return nil
}

func (i *GeoIndex) Get(key string) (lat, lon float64, ok bool) {
	hash, ok := i.set.Score(key)
	if !ok {
		return -1, -1, false
	}
	intHash := uint64(hash)

	lat, lon = geohash.Decode(intHash, i.bits)
	return lat, lon, true
}

func (i *GeoIndex) Dist(key1 string, key2 string) (float64, error) {
	k1Hash, ok := i.set.Score(key1)
	if !ok {
		return -1, fmt.Errorf("key1 missing in set")
	}
	k2Hash, ok := i.set.Score(key2)
	if !ok {
		return -1, fmt.Errorf("key2 missing in set")
	}

	lat1, lon1 := geohash.Decode(uint64(k1Hash), i.bits)
	lat2, lon2 := geohash.Decode(uint64(k2Hash), i.bits)
	return util.Haversine(lat1, lon1, lat2, lon2), nil
}

func (i *GeoIndex) SortedSet() *SortedSet[float64] {
	return i.set
}

func (i *GeoIndex) SearchRadius(lat, lon, radius float64) ([]string, error) {
	err := i.validate(lat, lon)
	if err != nil {
		return nil, err
	}
	return i.searchRadius(lat, lon, radius)
}

func (i *GeoIndex) searchRadius(lat, lon, radius float64) ([]string, error) {
	// Calculate appropriate precision for the search radius
	// We use a coarser precision to create larger search boxes
	searchBits := geohash.EstimateBitsForRadius(radius, i.bits)

	centerHash := geohash.Encode(lat, lon, searchBits)

	// Get all 8 neighbors plus center (9 cells total)
	neighbors := geohash.Neighbors(centerHash, searchBits)
	searchHashes := append([]uint64{centerHash}, neighbors...)

	// Calculate how many bits to shift to get from searchBits to full 52 bits
	shift := i.bits - searchBits

	candidates := make(map[string]struct{})

	for _, hash := range searchHashes {
		// Create a score range for this geohash box
		// The box represents all 52-bit hashes that start with this coarse hash
		minScore := float64(hash << shift)
		maxScore := float64((hash + 1) << shift)

		// Query the sorted set for all points in this score range
		nodes := i.set.RangeByScore(minScore, maxScore)
		for _, node := range nodes {
			candidates[node.Value] = struct{}{}
		}
	}

	var results []string
	for key := range candidates {
		keyLat, keyLon, ok := i.Get(key)
		if !ok {
			continue
		}

		distance := util.Haversine(lat, lon, keyLat, keyLon)
		if distance <= radius {
			results = append(results, key)
		}
	}

	return results, nil
}
