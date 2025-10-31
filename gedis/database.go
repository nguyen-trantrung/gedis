package gedis

import (
	"log"

	"github.com/ttn-nguyen42/gedis/data"
)

type database struct {
	num int
	hm  *data.HashMap
}

func newDb(n int) *database {
	return &database{
		num: n,
		hm:  data.NewHashMap(),
	}
}

func (d *database) EvictHashMap() {
	n, evicted := d.hm.Evict()
	if evicted {
		log.Printf("evicted %d keys, db=%d", n,d.num)
	}
}

func (d *database) HashMap() *data.HashMap {
	return d.hm
}
