package gedis

import (
	"log"

	"github.com/ttn-nguyen42/gedis/data"
	gedis_types "github.com/ttn-nguyen42/gedis/gedis/types"
)

type blockingOps struct {
	blockLpop map[any][]*gedis_types.Command
}

type database struct {
	num   int
	hm    *data.HashMap
	list  map[any]*data.LinkedList
	block *blockingOps
}

func newDb(n int) *database {
	return &database{
		num:  n,
		hm:   data.NewHashMap(),
		list: make(map[any]*data.LinkedList),
		block: &blockingOps{
			blockLpop: make(map[any][]*gedis_types.Command),
		},
	}
}

func (d *database) EvictHashMap() {
	n, evicted := d.hm.Evict()
	if evicted {
		log.Printf("evicted %d keys, db=%d", n, d.num)
	}
}

func (d *database) HashMap() *data.HashMap {
	return d.hm
}

func (d *database) GetOrCreateList(key any) *data.LinkedList {
	list, exists := d.list[key]
	if !exists {
		list = data.NewLinkedList()
		d.list[key] = list
	}
	return list
}

func (d *database) GetList(key any) (*data.LinkedList, bool) {
	list, exists := d.list[key]
	return list, exists
}

func (d *database) DeleteList(key any) bool {
	_, exists := d.list[key]
	if exists {
		delete(d.list, key)
	}
	return exists
}
