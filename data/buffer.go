package data

import (
	"context"
	"sync"
)

type CircularBuffer[D any] struct {
	data   []D
	head   int
	tail   int
	size   int
	cap    int
	signal chan struct{}
	mu     sync.Mutex
}

func NewCircularBuffer[D any](cap int) *CircularBuffer[D] {
	return &CircularBuffer[D]{
		data:   make([]D, cap),
		cap:    cap,
		signal: make(chan struct{}, 1),
	}
}

func (c *CircularBuffer[D]) Size() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.size
}

func (c *CircularBuffer[D]) Send(ctx context.Context, cmd D) {
	c.send(ctx, []D{cmd})
}

func (c *CircularBuffer[D]) MultiSend(ctx context.Context, cmds []D) {
	c.send(ctx, cmds)
}

func (c *CircularBuffer[D]) send(ctx context.Context, cmds []D) {
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

func (c *CircularBuffer[D]) Read() (D, bool) {
	return c.read()
}

func (c *CircularBuffer[D]) read() (D, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var empty D

	if c.size == 0 {
		return empty, false
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

func (c *CircularBuffer[D]) ReadBatch(n int) []D {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.size == 0 {
		return nil
	}

	batchSize := min(n, c.size)

	batch := make([]D, 0, batchSize)
	for i := 0; i < batchSize; i++ {
		d := c.data[c.head]
		batch = append(batch, d)
		c.head = (c.head + 1) % c.cap
		c.size -= 1
	}

	select {
	case c.signal <- struct{}{}:
	default:
	}

	return batch
}
