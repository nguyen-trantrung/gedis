package data

import (
	"fmt"

	"github.com/ttn-nguyen42/gedis/data/geohash"
	"github.com/ttn-nguyen42/gedis/util"
)

type GeoIndex struct {
	bits int
	set  *SortedSet[uint64]
}

func NewGeoIndex(precision int) *GeoIndex {
	return &GeoIndex{
		bits: precision,
		set:  NewSortedSet[uint64](),
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
	return i.set.Insert(key, hash)
}

func (i *GeoIndex) validate(lat, lon float64) error {
	if lat <= -90.0 || lat >= 90.0 {
		return fmt.Errorf("invalid latitute value")
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

	lat, lon = geohash.Decode(hash, i.bits)
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
	lat1, lon1 := geohash.Decode(k1Hash, i.bits)
	lat2, lon2 := geohash.Decode(k2Hash, i.bits)
	return util.Haversine(lat1, lon1, lat2, lon2), nil
}

func (i *GeoIndex) SortedSet() *SortedSet[uint64] {
	return i.set
}
