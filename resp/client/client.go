package resp_client

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/ttn-nguyen42/gedis/resp"
)

// Client wraps a single RESP connection. It is NOT intended to be shared between
// concurrent goroutines without synchronization. Deadlines on a net.Conn are
// global: setting a deadline for one in-flight operation affects all other
// concurrent operations on the same connection. Previously, multiple goroutines
// calling SendSync could race: goroutine A sets a deadline, goroutine B sets a
// later deadline, then A returns early and clears the deadline causing B to
// lose its protection or vice‑versa. That manifested as "one timeout cancels
// others" in higher layers. We serialize request operations with a mutex so a
// deadline only ever applies to the single request in flight.
type Client struct {
	mu   sync.Mutex
	host string
	port int
	conn net.Conn
}

func NewClient(host string, port int) (*Client, error) {
	client := &Client{
		host: host,
		port: port,
	}
	if err := client.connect(); err != nil {
		return nil, err
	}
	return client, nil
}

func (c *Client) connect() error {
	conn, err := net.Dial("tcp", net.JoinHostPort(c.host, fmt.Sprintf("%d", c.port)))
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func NewClientFromConn(conn net.Conn) *Client {
	addr := conn.RemoteAddr().(*net.TCPAddr)
	return &Client{
		host: addr.IP.String(),
		port: addr.Port,
		conn: conn,
	}
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) SendSync(ctx context.Context, cmd resp.Command) (any, int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	arr := cmd.Array()
	total := 0

	log.Println("set dl", c.conn.RemoteAddr())
	dl := time.Now().Add(100 * time.Millisecond)
	c.conn.SetReadDeadline(dl)
	defer c.conn.SetReadDeadline(time.Time{})

	if n, err := arr.WriteTo(c.conn); err != nil {
		if isTimeoutErr(err) {
			log.Println("write time passed", time.Since(dl))
			return nil, 0, context.DeadlineExceeded
		}
		return nil, 0, err
	} else {
		total += int(n)
	}

	out, err := resp.ParseValue(c.conn)
	if err != nil {
		if isTimeoutErr(err) {
			log.Println("read time passed", time.Since(dl))
			return nil, 0, context.DeadlineExceeded
		}
		return nil, 0, fmt.Errorf("invalid output: %w", err)
	}
	if valerr, ok := out.(resp.Err); ok {
		return nil, 0, fmt.Errorf("command error: %s", valerr.Value)
	}
	return out, total, nil
}

func (c *Client) SendBinary(ctx context.Context, data []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	if dl, ok := ctx.Deadline(); ok {
		if err := c.conn.SetWriteDeadline(dl); err != nil {
			return 0, fmt.Errorf("failed to set write deadline: %w", err)
		}
		defer c.conn.SetWriteDeadline(time.Time{})
	}

	proto := fmt.Sprintf("$%d\r\n", len(data))
	total := 0
	n, err := c.conn.Write([]byte(proto))
	if err != nil {
		if isTimeoutErr(err) {
			return 0, context.DeadlineExceeded
		}
		return 0, err
	}
	total += n
	log.Printf("written to replication stream, l=%d", n)
	n, err = c.conn.Write(data)
	if err != nil {
		if isTimeoutErr(err) {
			return 0, context.DeadlineExceeded
		}
		return 0, err
	}
	total += n
	log.Printf("written to replication stream, l=%d", n)
	return total, nil
}

func (c *Client) SendForget(ctx context.Context, cmd resp.Command) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	if dl, ok := ctx.Deadline(); ok {
		if err := c.conn.SetWriteDeadline(dl); err != nil {
			return 0, fmt.Errorf("failed to set write deadline: %w", err)
		}
		defer c.conn.SetWriteDeadline(time.Time{})
	}

	arr := cmd.Array()
	n, err := arr.WriteTo(c.conn)
	if err != nil {
		if isTimeoutErr(err) {
			return 0, context.DeadlineExceeded
		}
		return 0, err
	}
	return int(n), nil
}

func (c *Client) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *Client) Conn() net.Conn {
	return c.conn
}

// isTimeoutErr provides robust detection for deadline/timeout errors across net & os packages.
func isTimeoutErr(err error) bool {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	return false
}
