package data_test

import (
	"testing"

	"github.com/ttn-nguyen42/gedis/data"
	"github.com/ttn-nguyen42/gedis/data/geohash"
)

func TestGeoIndex_SearchRadius(t *testing.T) {
	geoIndex := data.NewGeoIndex(52)

	// Add test points from the failing test
	testPoints := []struct {
		name string
		lon  float64
		lat  float64
	}{
		{"pear", 173.78007382689725, 71.29040383439394},
		{"pineapple", -22.34938226097612, 58.45290170796341},
		{"orange", 126.60356794662907, -6.1777878605543535},
		{"raspberry", -19.87475417945569, 76.37836567147966},
	}

	for _, pt := range testPoints {
		ok, err := geoIndex.Add(pt.name, pt.lat, pt.lon)
		if err != nil {
			t.Fatalf("Failed to add %s: %v", pt.name, err)
		}
		t.Logf("Added %s: lat=%.6f, lon=%.6f, inserted=%v", pt.name, pt.lat, pt.lon, !ok)
	}

	// Search from center point with large radius
	centerLon := 64.53987633327363
	centerLat := 49.98597083832066
	radius := 10770003.08883019 // ~10,770 km

	t.Logf("Searching from center: lat=%.6f, lon=%.6f, radius=%.2f meters", centerLat, centerLon, radius)

	// Debug: check what's in the sorted set
	allNodes := geoIndex.SortedSet().Range(0, geoIndex.SortedSet().Len())
	t.Logf("Total points in sorted set: %d", len(allNodes))
	for _, node := range allNodes {
		hash := uint64(node.Score)
		lat, lon := geohash.Decode(hash, 52)
		t.Logf("  - %s: geohash=%d, lat=%.6f, lon=%.6f", node.Value, hash, lat, lon)
	}

	results, err := geoIndex.SearchRadius(centerLat, centerLon, radius)
	if err != nil {
		t.Fatalf("SearchRadius failed: %v", err)
	}

	t.Logf("Found %d results: %v", len(results), results)

	if len(results) == 0 {
		t.Errorf("Expected to find points within radius, got 0")
	}

	// Expected: should find all 4 points (pear, pineapple, orange, raspberry)
	if len(results) != 4 {
		t.Errorf("Expected 4 results, got %d: %v", len(results), results)
	}
}

func TestGeoIndex_AddAndGet(t *testing.T) {
	geoIndex := data.NewGeoIndex(52)

	// Add a point
	lon := 2.3488
	lat := 48.8534
	ok, err := geoIndex.Add("paris", lat, lon)
	if err != nil {
		t.Fatalf("Failed to add paris: %v", err)
	}
	if !ok {
		t.Error("Expected insert to succeed")
	}

	// Get it back
	gotLat, gotLon, found := geoIndex.Get("paris")
	if !found {
		t.Fatal("Expected to find paris")
	}

	t.Logf("Original: lat=%.6f, lon=%.6f", lat, lon)
	t.Logf("Retrieved: lat=%.6f, lon=%.6f", gotLat, gotLon)

	// Check if coordinates are close enough (geohash has precision limits)
	latDiff := abs(lat - gotLat)
	lonDiff := abs(lon - gotLon)

	if latDiff > 0.0001 || lonDiff > 0.0001 {
		t.Errorf("Coordinates differ too much: lat diff=%.6f, lon diff=%.6f", latDiff, lonDiff)
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
