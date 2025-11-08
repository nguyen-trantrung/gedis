package geohash

func Neighbors(hash uint64, bits int) []uint64 {
	var neighbors []uint64
	directions := [][]int{
		{1, 0},   // North
		{1, 1},   // Northeast
		{0, 1},   // East
		{-1, 1},  // Southeast
		{-1, 0},  // South
		{-1, -1}, // Southwest
		{0, -1},  // West
		{1, -1},  // Northwest
	}

	// Decode the center hash to get lat/lon
	centerLat, centerLon := Decode(hash, bits)

	// Calculate approximate cell dimensions
	// Each bit alternates between lon and lat, so we divide by 2
	latBits := bits / 2
	lonBits := bits - latBits

	// Cell size in degrees (using Web Mercator limits)
	const mercatorLatRange = 170.10225756 // 85.05112878 - (-85.05112878)
	latCellSize := mercatorLatRange / float64(uint64(1)<<latBits)
	lonCellSize := 360.0 / float64(uint64(1)<<lonBits)

	for _, dir := range directions {
		// Calculate neighbor coordinates
		neighborLat := centerLat + float64(dir[0])*latCellSize
		neighborLon := centerLon + float64(dir[1])*lonCellSize

		// Handle longitude wraparound
		if neighborLon > 180.0 {
			neighborLon -= 360.0
		} else if neighborLon < -180.0 {
			neighborLon += 360.0
		}

		// Skip if latitude is out of bounds (Web Mercator limits)
		if neighborLat <= -85.05112878 || neighborLat >= 85.05112878 {
			continue
		}

		// Encode the neighbor coordinates
		neighborHash := Encode(neighborLat, neighborLon, bits)
		neighbors = append(neighbors, neighborHash)
	}

	return neighbors
}
