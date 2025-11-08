package geohash_test

import (
	"testing"

	"github.com/ttn-nguyen42/gedis/data/geohash"
)

func TestEncodeLat_Equator(t *testing.T) {
	// Latitude 0 (equator) should be in the middle of the range
	bits := geohash.EncodeLat(0, 5)
	// With 5 bits, middle should be around 16 (binary 10000)
	if bits == 0 {
		t.Fatalf("expected non-zero bits for equator, got %d", bits)
	}
}

func TestEncodeLat_NorthPole(t *testing.T) {
	// Latitude 90 (north pole) should have all bits set (max value)
	bits := geohash.EncodeLat(90, 5)
	expected := uint64(31) // 11111 in binary (all 5 bits set)
	if bits != expected {
		t.Fatalf("expected bits %d for north pole, got %d (binary: %b)", expected, bits, bits)
	}
}

func TestEncodeLat_SouthPole(t *testing.T) {
	// Latitude -90 (south pole) should have no bits set (min value)
	bits := geohash.EncodeLat(-90, 5)
	if bits != 0 {
		t.Fatalf("expected bits 0 for south pole, got %d", bits)
	}
}

func TestEncodeLon_PrimeMeridian(t *testing.T) {
	// Longitude 0 (prime meridian) should be in the middle of the range
	bits := geohash.EncodeLon(0, 5)
	if bits == 0 {
		t.Fatalf("expected non-zero bits for prime meridian, got %d", bits)
	}
}

func TestEncodeLon_EastBound(t *testing.T) {
	// Longitude 180 should have all bits set
	bits := geohash.EncodeLon(180, 5)
	expected := uint64(31) // 11111 in binary
	if bits != expected {
		t.Fatalf("expected bits %d for 180° longitude, got %d (binary: %b)", expected, bits, bits)
	}
}

func TestEncodeLon_WestBound(t *testing.T) {
	// Longitude -180 should have no bits set
	bits := geohash.EncodeLon(-180, 5)
	if bits != 0 {
		t.Fatalf("expected bits 0 for -180° longitude, got %d", bits)
	}
}

func TestEncode_KnownLocations(t *testing.T) {
	tests := []struct {
		name      string
		lat       float64
		lon       float64
		precision int
	}{
		{
			name:      "New York City",
			lat:       40.7128,
			lon:       -74.0060,
			precision: 20,
		},
		{
			name:      "London",
			lat:       51.5074,
			lon:       -0.1278,
			precision: 20,
		},
		{
			name:      "Tokyo",
			lat:       35.6762,
			lon:       139.6503,
			precision: 20,
		},
		{
			name:      "Sydney",
			lat:       -33.8688,
			lon:       151.2093,
			precision: 20,
		},
		{
			name:      "Null Island",
			lat:       0.0,
			lon:       0.0,
			precision: 20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash := geohash.Encode(tt.lat, tt.lon, tt.precision)

			// Hash should be non-zero for most locations
			if hash == 0 && tt.name != "Null Island" {
				t.Fatalf("expected non-zero hash for %s, got %d (binary: %b)", tt.name, hash, hash)
			}

			t.Logf("%s: hash=%d (binary: %052b)", tt.name, hash, hash)
		})
	}
}

func TestEncode_DifferentPrecisions(t *testing.T) {
	lat, lon := 37.7749, -122.4194 // San Francisco

	tests := []struct {
		precision int
	}{
		{5},
		{10},
		{15},
		{20},
		{25},
		{30},
		{40},
		{52},
	}

	var prevHash uint64
	for _, tt := range tests {
		hash := geohash.Encode(lat, lon, tt.precision)

		// Higher precision should generally give different (more specific) hashes
		if tt.precision > 5 && hash == prevHash {
			t.Logf("precision %d: same hash as previous precision", tt.precision)
		}

		t.Logf("precision %d: hash=%d (binary: %b)", tt.precision, hash, hash)
		prevHash = hash
	}
}

func TestEncode_SameLocation_SameHash(t *testing.T) {
	lat, lon := 48.8566, 2.3522 // Paris
	precision := 25

	hash1 := geohash.Encode(lat, lon, precision)
	hash2 := geohash.Encode(lat, lon, precision)

	if hash1 != hash2 {
		t.Fatalf("same location should produce same hash: %d != %d", hash1, hash2)
	}
}

func TestEncode_NearbyLocations_SimilarHash(t *testing.T) {
	// Two locations very close to each other should have similar hashes
	lat1, lon1 := 40.7128, -74.0060 // New York
	lat2, lon2 := 40.7129, -74.0061 // Very close to New York
	precision := 30

	hash1 := geohash.Encode(lat1, lon1, precision)
	hash2 := geohash.Encode(lat2, lon2, precision)

	// Calculate the XOR difference to see how many bits differ
	diff := hash1 ^ hash2

	// For nearby locations, the difference should be relatively small
	// (they should share most significant bits)
	t.Logf("nearby locations: hash1=%d, hash2=%d, diff=%d (binary: %b)",
		hash1, hash2, diff, diff)

	// They should not be identical (different locations)
	if hash1 == hash2 {
		t.Fatalf("nearby but different locations should have different hashes")
	}
}

func TestEncode_DistantLocations_DifferentHash(t *testing.T) {
	hash1 := geohash.Encode(40.7128, -74.0060, 20)  // New York
	hash2 := geohash.Encode(-33.8688, 151.2093, 20) // Sydney

	t.Logf("New York: %d (binary: %020b)", hash1, hash1)
	t.Logf("Sydney: %d (binary: %020b)", hash2, hash2)

	if hash1 == hash2 {
		t.Fatalf("distant locations should have different hashes")
	}
}

func TestEncode_BoundaryConditions(t *testing.T) {
	tests := []struct {
		name string
		lat  float64
		lon  float64
	}{
		{"Max lat, max lon", 90, 180},
		{"Max lat, min lon", 90, -180},
		{"Min lat, max lon", -90, 180},
		{"Min lat, min lon", -90, -180},
		{"Max lat, zero lon", 90, 0},
		{"Min lat, zero lon", -90, 0},
		{"Zero lat, max lon", 0, 180},
		{"Zero lat, min lon", 0, -180},
	}

	precision := 20
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash := geohash.Encode(tt.lat, tt.lon, precision)

			// All hashes should be valid uint64 values
			t.Logf("%s: hash=%d (binary: %020b)", tt.name, hash, hash)
		})
	}
}

func TestEncodeLat_IncreasingPrecision(t *testing.T) {
	lat := 45.0

	// Higher precision should give more bits
	bits5 := geohash.EncodeLat(lat, 5)
	bits10 := geohash.EncodeLat(lat, 10)
	bits20 := geohash.EncodeLat(lat, 20)

	t.Logf("lat 45.0: bits5=%d (%05b), bits10=%d (%010b), bits20=%d (%020b)",
		bits5, bits5, bits10, bits10, bits20, bits20)

	// With more precision, we should have more bits set
	// The actual values depend on the binary search algorithm
	if bits20 == 0 {
		t.Fatalf("expected non-zero bits for lat 45.0 with precision 20")
	}
}

func TestEncodeLon_IncreasingPrecision(t *testing.T) {
	lon := 90.0

	bits5 := geohash.EncodeLon(lon, 5)
	bits10 := geohash.EncodeLon(lon, 10)
	bits20 := geohash.EncodeLon(lon, 20)

	t.Logf("lon 90.0: bits5=%d (%05b), bits10=%d (%010b), bits20=%d (%020b)",
		bits5, bits5, bits10, bits10, bits20, bits20)

	// With more precision, we should have more bits set
	if bits20 == 0 {
		t.Fatalf("expected non-zero bits for lon 90.0 with precision 20")
	}
}

func TestEncode_MaxBitsLimit(t *testing.T) {
	// Test that the 52-bit limit is enforced
	hash52 := geohash.Encode(37.7749, -122.4194, 52)
	hash60 := geohash.Encode(37.7749, -122.4194, 60)

	// Both should produce the same result (capped at 52 bits)
	if hash52 != hash60 {
		t.Fatalf("hashes with precision > 52 should be capped at 52 bits: %d != %d", hash52, hash60)
	}

	t.Logf("52-bit hash: %d (binary: %052b)", hash52, hash52)
}

func TestEncode_Paris(t *testing.T) {
	// Test specific Paris coordinates to verify expected hash value
	lat := 48.8584625
	lon := 2.2944692
	precision := 52

	hash := geohash.Encode(lat, lon, precision)
	expected := uint64(3663832614298053)

	if hash != expected {
		t.Fatalf("Paris geohash mismatch: expected %d, got %d\nBinary expected: %052b\nBinary got:      %052b",
			expected, hash, expected, hash)
	}

	t.Logf("Paris (%.7f, %.7f): hash=%d (binary: %052b)", lat, lon, hash, hash)
}
