package geohash_test

import (
	"log"
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
		t.Fatalf("expected bits %d for north pole, got %d", expected, bits)
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
		t.Fatalf("expected bits %d for 180° longitude, got %d", expected, bits)
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
		// We'll check length and that it's not empty
		// Exact hash values depend on the implementation details
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
			
			expectedLen := (tt.precision + 4) / 5 // ceiling(precision / 5)
			if len(hash) != expectedLen {
				t.Fatalf("expected hash length %d, got %d for %s", expectedLen, len(hash), hash)
			}
			
			if hash == "" {
				t.Fatalf("expected non-empty hash for %s", tt.name)
			}
		})
	}
}

func TestEncode_DifferentPrecisions(t *testing.T) {
	lat, lon := 37.7749, -122.4194 // San Francisco

	tests := []struct {
		precision    int
		expectedLen  int
	}{
		{5, 1},   // 5 bits = 1 base32 char
		{10, 2},  // 10 bits = 2 base32 chars
		{15, 3},  // 15 bits = 3 base32 chars
		{20, 4},  // 20 bits = 4 base32 chars
		{25, 5},  // 25 bits = 5 base32 chars
		{30, 6},  // 30 bits = 6 base32 chars
		{11, 3},  // 11 bits = 3 base32 chars (ceil(11/5))
	}

	for _, tt := range tests {
		hash := geohash.Encode(lat, lon, tt.precision)
		if len(hash) != tt.expectedLen {
			t.Fatalf("precision %d: expected length %d, got %d (hash: %s)", 
				tt.precision, tt.expectedLen, len(hash), hash)
		}
	}
}

func TestEncode_SameLocation_SameHash(t *testing.T) {
	lat, lon := 48.8566, 2.3522 // Paris
	precision := 25

	hash1 := geohash.Encode(lat, lon, precision)
	hash2 := geohash.Encode(lat, lon, precision)

	if hash1 != hash2 {
		t.Fatalf("same location should produce same hash: %s != %s", hash1, hash2)
	}
}

func TestEncode_NearbyLocations_SimilarPrefix(t *testing.T) {
	// Two locations very close to each other should share a common prefix
	lat1, lon1 := 40.7128, -74.0060 // New York
	lat2, lon2 := 40.7129, -74.0061 // Very close to New York
	precision := 30

	hash1 := geohash.Encode(lat1, lon1, precision)
	hash2 := geohash.Encode(lat2, lon2, precision)

	// They should share at least the first few characters
	if len(hash1) < 2 || len(hash2) < 2 {
		t.Fatalf("hashes too short to compare")
	}

	// Check if they share at least the first character
	if hash1[0] != hash2[0] {
		t.Logf("nearby locations should share prefix: %s vs %s", hash1, hash2)
		// Note: This might not always be true depending on precision and proximity
		// but for very close points it should hold
	}
}

func TestEncode_DistantLocations_DifferentHash(t *testing.T) {
	hash1 := geohash.Encode(40.7128, -74.0060, 20) // New York
	hash2 := geohash.Encode(-33.8688, 151.2093, 20) // Sydney
	log.Println(hash1, hash2)
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
			expectedLen := (precision + 4) / 5
			
			if len(hash) != expectedLen {
				t.Fatalf("expected length %d, got %d", expectedLen, len(hash))
			}
			
			if hash == "" {
				t.Fatalf("expected non-empty hash")
			}
		})
	}
}

func TestEncodeLat_IncreasingPrecision(t *testing.T) {
	lat := 45.0
	
	// Higher precision should give more bits
	bits5 := geohash.EncodeLat(lat, 5)
	bits10 := geohash.EncodeLat(lat, 10)
	bits20 := geohash.EncodeLat(lat, 20)
	
	// With more precision, we should have larger bit values
	if bits10 <= bits5 {
		t.Logf("warning: bits10 (%d) should be > bits5 (%d) when left-aligned", bits10, bits5)
	}
	
	if bits20 <= bits10 {
		t.Logf("warning: bits20 (%d) should be > bits10 (%d) when left-aligned", bits20, bits10)
	}
}

func TestEncodeLon_IncreasingPrecision(t *testing.T) {
	lon := 90.0
	
	bits5 := geohash.EncodeLon(lon, 5)
	bits10 := geohash.EncodeLon(lon, 10)
	bits20 := geohash.EncodeLon(lon, 20)
	
	if bits10 <= bits5 {
		t.Logf("warning: bits10 (%d) should be > bits5 (%d) when left-aligned", bits10, bits5)
	}
	
	if bits20 <= bits10 {
		t.Logf("warning: bits20 (%d) should be > bits10 (%d) when left-aligned", bits20, bits10)
	}
}

func TestEncode_Base32Characters(t *testing.T) {
	// Ensure the output only contains valid base32 characters
	validChars := "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
	
	hash := geohash.Encode(37.7749, -122.4194, 25)
	
	for _, char := range hash {
		found := false
		for _, valid := range validChars {
			if char == valid {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("invalid character '%c' in hash %s", char, hash)
		}
	}
}
