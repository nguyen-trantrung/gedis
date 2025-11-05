package resp_client

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"

	"github.com/ttn-nguyen42/gedis/resp"
)

type Client struct {
	host string
	port int
	conn net.Conn
	*bufio.Reader
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
	c.Reader = bufio.NewReader(conn)
	return nil
}

func NewClientFromConn(conn net.Conn) *Client {
	addr := conn.RemoteAddr().(*net.TCPAddr)
	return &Client{
		host:   addr.IP.String(),
		port:   addr.Port,
		conn:   conn,
		Reader: bufio.NewReader(conn),
	}
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) SendSync(ctx context.Context, cmd resp.Command) (any, int, error) {
	arr := cmd.Array()
	total := 0
	if n, err := arr.WriteTo(c.conn); err != nil {
		return nil, 0, err
	} else {
		total += int(n)
	}
	out, err := resp.ParseValue(c)
	if err != nil {
		return nil, 0, fmt.Errorf("invalid output: %w", err)
	}
	valerr, ok := out.(resp.Err)
	if ok {
		return nil, 0, fmt.Errorf("command error: %s", valerr.Value)
	}
	return out, 0, nil
}

func (c *Client) SendBinary(ctx context.Context, data []byte) (int, error) {
	proto := fmt.Sprintf("$%d\r\n", len(data))
	total := 0
	n, err := c.conn.Write([]byte(proto))
	if err != nil {
		return 0, err
	}
	total += n
	log.Printf("written to replication stream, l=%d", n)
	n, err = c.conn.Write(data)
	if err != nil {
		return 0, err
	}
	total += n
	log.Printf("written to replication stream, l=%d", n)
	return total, nil
}

func (c *Client) SendForget(ctx context.Context, cmd resp.Command) (int, error) {
	arr := cmd.Array()
	n, err := arr.WriteTo(c.conn)
	if err != nil {
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
