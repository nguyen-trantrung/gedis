package gedis

import (
	"log"
	"net"
	"slices"

	"github.com/ttn-nguyen42/gedis/data"
	"github.com/ttn-nguyen42/gedis/resp"
	"github.com/ttn-nguyen42/gedis/util"
)

type sub struct {
	conn net.Conn
	id   string
}

type pubsub struct {
	subs map[string][]*sub
	qu   map[string]*data.Queue
}

func newPubsub() *pubsub {
	return &pubsub{
		subs: make(map[string][]*sub),
		qu:   make(map[string]*data.Queue),
	}
}

func (p *pubsub) publish(channel string, message any) int {
	q := p.initChannel(channel)
	q.Enqueue(message)
	return 0
}

func (p *pubsub) initChannel(channel string) *data.Queue {
	if _, ok := p.qu[channel]; !ok {
		p.qu[channel] = data.NewQueue()
	}
	return p.qu[channel]
}

func (p *pubsub) subscribe(id string, channel string, conn net.Conn) {
	subscription := &sub{
		id:   id,
		conn: conn,
	}
	p.subs[channel] = append(p.subs[channel], subscription)
}

func (p *pubsub) countSubs(id string) int {
	count := 0
	for _, subscribers := range p.subs {
		for _, sub := range subscribers {
			if sub.id == id {
				count += 1
			}
		}
	}
	return count
}

func (p *pubsub) unsubscribe(id string, channel string) {
	subscribers, ok := p.subs[channel]
	if !ok {
		return
	}
	subs := slices.DeleteFunc(subscribers, func(s *sub) bool {
		return s.id == id
	})
	p.subs[channel] = subs
}

func (p *pubsub) unsubscribeAll(id string) []string {
	var channels []string
	for channel, subscribers := range p.subs {
		subs := slices.DeleteFunc(subscribers, func(s *sub) bool {
			return s.id == id
		})
		p.subs[channel] = subs
		if len(subs) < len(subscribers) {
			channels = append(channels, channel)
		}
	}
	return channels
}

func (p *pubsub) resolveSubs() {
	for channel, subscribers := range p.subs {
		qu, ok := p.qu[channel]
		if !ok {
			continue
		}
		if qu.IsEmpty() {
			continue
		}
		message, _ := qu.Dequeue()
		arr := p.produceMessage(channel, message)
		remv := make(map[string]struct{}, 0)

		for _, sub := range subscribers {
			n, err := resp.WriteAnyTo(arr, sub.conn)
			if err != nil {
				if util.IsDisconnected(err) {
					log.Printf("subscriber disconnected, id=%s, addr=%s", sub.id, sub.conn.RemoteAddr())
					remv[sub.id] = struct{}{}
					continue
				}
				log.Printf("failed to write pubsub message to subscriber, id=%s, addr=%s, err=%s", sub.id, sub.conn.RemoteAddr(), err)
				continue
			}
			log.Printf("delivered message to subscriber, id=%s, addr=%s, n=%d", sub.id, sub.conn.RemoteAddr(), n)
		}

		p.subs[channel] = slices.DeleteFunc(subscribers, func(s *sub) bool {
			_, ok := remv[s.id]
			return ok
		})
	}
}

func (p *pubsub) produceMessage(channel string, content any) resp.Array {
	arr := resp.Array{Size: 3, Items: make([]any, 0, 3)}

	arr.Items = append(arr.Items, "message")
	arr.Items = append(arr.Items, channel)
	arr.Items = append(arr.Items, content)

	return arr
}
