package gedis

type database struct {
	num int
}

func newDb(n int) *database {
	return &database{
		num: n,
	}
}
