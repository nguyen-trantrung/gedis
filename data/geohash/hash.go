package geohash

// Encode generates a geohash as uint64 for the given latitude, longitude and number of bits
// Maximum bits is 52 (to fit in uint64 without precision loss in interleaving)
func Encode(lat, lon float64, bits int) uint64 {
	if bits > 52 {
		bits = 52
	}

	latRange := [2]float64{-85.05112878, 85.05112878}
	lonRange := [2]float64{-180.0, 180.0}

	var hash uint64
	isEven := true

	for range bits {
		hash <<= 1

		if isEven {
			mid := (lonRange[0] + lonRange[1]) / 2
			if lon >= mid {
				hash |= 1
				lonRange[0] = mid
			} else {
				lonRange[1] = mid
			}
		} else {
			mid := (latRange[0] + latRange[1]) / 2
			if lat >= mid {
				hash |= 1
				latRange[0] = mid
			} else {
				latRange[1] = mid
			}
		}

		isEven = !isEven
	}

	return hash
}

// Decode converts a geohash back to latitude and longitude coordinates
// Returns the center point of the hash's bounding box
func Decode(hash uint64, bits int) (lat, lon float64) {
	if bits > 52 {
		bits = 52
	}

	latRange := [2]float64{-85.05112878, 85.05112878}
	lonRange := [2]float64{-180.0, 180.0}

	isEven := true

	for i := bits - 1; i >= 0; i -= 1 {
		bit := (hash >> i) & 1

		if isEven {
			mid := (lonRange[0] + lonRange[1]) / 2
			if bit == 1 {
				lonRange[0] = mid
			} else {
				lonRange[1] = mid
			}
		} else {
			mid := (latRange[0] + latRange[1]) / 2
			if bit == 1 {
				latRange[0] = mid
			} else {
				latRange[1] = mid
			}
		}

		isEven = !isEven
	}

	lat = (latRange[0] + latRange[1]) / 2
	lon = (lonRange[0] + lonRange[1]) / 2
	return
}

// EncodeLat encodes latitude into binary representation with given precision
func EncodeLat(lat float64, precision int) uint64 {
	return encodeBinary(lat, -85.05112878, 85.05112878, precision)
}

// EncodeLon encodes longitude into binary representation with given precision
func EncodeLon(lon float64, precision int) uint64 {
	return encodeBinary(lon, -180.0, 180.0, precision)
}

func encodeBinary(value, min, max float64, precision int) uint64 {
	var bits uint64

	for range precision {
		mid := (min + max) / 2
		bits <<= 1

		if value >= mid {
			bits |= 1
			min = mid
		} else {
			max = mid
		}
	}

	return bits
}

// EstimateBitsForRadius estimates the appropriate geohash precision (bits)
// for a given search radius in meters and maximum precision
func EstimateBitsForRadius(radius float64, maxBits int) int {
	const earthRadius = 6371000.0

	// For a given radius, estimate the cell size needed
	// We want cells that are roughly 2x the radius to ensure coverage
	targetCellSize := radius * 2

	// Calculate degrees of latitude that corresponds to this distance
	// (longitude varies by latitude, but this is a rough estimate)
	degreesPerMeter := 1.0 / (earthRadius * 3.14159 / 180.0)
	targetDegrees := targetCellSize * degreesPerMeter

	const mercatorLatRange = 170.10225756

	// Calculate how many bits we need
	// Each bit divides the space in half
	bits := 1
	cellSize := mercatorLatRange
	for bits < maxBits && cellSize > targetDegrees {
		cellSize /= 2
		bits += 1
	}

	// Use at least 1 bit, at most maxBits
	if bits < 1 {
		bits = 1
	}
	if bits > maxBits {
		bits = maxBits
	}

	// Make sure bits is even for proper interleaving (or adjust as needed)
	// Round down to nearest even number for balanced lat/lon precision
	bits = (bits / 2) * 2
	if bits == 0 {
		bits = 2
	}

	return bits
}
