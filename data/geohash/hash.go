package geohash

import (
	"math"
	"strings"
)

const base32Alphabet = "0123456789bcdefghjkmnpqrstuvwxyz"

// Encode generates a geohash string for the given latitude, longitude and number of bits
func Encode(lat, lon float64, bits int) string {
	// Calculate how many base32 characters we need (5 bits per character)
	numChars := int(math.Ceil(float64(bits) / 5.0))

	var result strings.Builder
	result.Grow(numChars)

	latRange := [2]float64{-90.0, 90.0}
	lonRange := [2]float64{-180.0, 180.0}

	var ch uint8
	var bit uint8
	isEven := true

	for range bits {
		if isEven {
			mid := (lonRange[0] + lonRange[1]) / 2
			if lon >= mid {
				ch |= (1 << (4 - bit))
				lonRange[0] = mid
			} else {
				lonRange[1] = mid
			}
		} else {
			mid := (latRange[0] + latRange[1]) / 2
			if lat >= mid {
				ch |= (1 << (4 - bit)) // 4 = 11111
				latRange[0] = mid
			} else {
				latRange[1] = mid
			}
		}

		isEven = !isEven
		bit += 1

		// Every 5 bits, write a base32 character
		if bit == 5 {
			result.WriteByte(base32Alphabet[ch])
			ch = 0
			bit = 0
		}
	}

	if bit > 0 {
		result.WriteByte(base32Alphabet[ch])
	}

	return result.String()
}

// EncodeLat encodes latitude into binary representation with given precision
func EncodeLat(lat float64, precision int) uint64 {
	return encodeBinary(lat, -90.0, 90.0, precision)
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
