package gedis

import (
	"context"
	"sync"
)

type circular struct {
	data   []RespondableCmd
	head   int
	tail   int
	size   int
	cap    int
	signal chan struct{}
	mu     sync.Mutex
}

func newBuffer(cap int) *circular {
	return &circular{
		data:   make([]RespondableCmd, cap),
		cap:    cap,
		signal: make(chan struct{}, 1),
	}
}

func (c *circular) Send(ctx context.Context, cmd RespondableCmd) {
	c.send(ctx, []RespondableCmd{cmd})
}

func (c *circular) MultiSend(ctx context.Context, cmds []RespondableCmd) {
	c.send(ctx, cmds)
}

func (c *circular) send(ctx context.Context, cmds []RespondableCmd) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, cmd := range cmds {
		for c.size >= c.cap {
			c.mu.Unlock()
			select {
			case <-c.signal:
			case <-ctx.Done():
				c.mu.Lock()
				return
			}
			c.mu.Lock()
		}
		c.data[c.tail] = cmd
		c.tail = (c.tail + 1) % c.cap
		c.size += 1
	}
}

func (c *circular) Read() (RespondableCmd, bool) {
	return c.read()
}

func (c *circular) read() (RespondableCmd, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.size == 0 {
		return RespondableCmd{}, false
	}

	d := c.data[c.head]
	c.head = (c.head + 1) % c.cap
	c.size -= 1
	select {
	case c.signal <- struct{}{}:
	default:
	}
	return d, true
}
