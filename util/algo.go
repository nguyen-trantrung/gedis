package util

import "math"

const EARTH_RADIUS_IN_METERS = 6372797.560856
const DR = math.Pi / 180.0

func Haversine(lat1, lon1 float64, lat2, lon2 float64) float64 {
	lon1r := degRad(lon1)
	lon2r := degRad(lon2)
	v := math.Sin((lon2r - lon1r) / 2)
	/* if v == 0 we can avoid doing expensive math when lons are practically the same */
	if v == 0.0 {
		return geohashGetLatDistance(lat1, lat2)
	}
	lat1r := degRad(lat1)
	lat2r := degRad(lat2)
	u := math.Sin((lat2r - lat1r) / 2)
	a := u*u + math.Cos(lat1r)*math.Cos(lat2r)*v*v
	return 2.0 * EARTH_RADIUS_IN_METERS * math.Asin(math.Sqrt(a))
}

func geohashGetLatDistance(lat1, lat2 float64) float64 {
	return EARTH_RADIUS_IN_METERS * math.Abs(degRad(lat2)-degRad(lat1))
}

func degRad(ang float64) float64 {
	return ang * DR
}
