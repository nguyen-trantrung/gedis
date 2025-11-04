package resp_client

import (
	"context"
	"fmt"
	"net"

	"github.com/ttn-nguyen42/gedis/resp"
)

type Client struct {
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

func (c *Client) SendSync(ctx context.Context, cmd *resp.Command) (any, error) {
	arr := cmd.Array()
	if _, err := arr.WriteTo(c.conn); err != nil {
		return nil, err
	}
	out, err := resp.ParseValue(c.conn)
	if err != nil {
		return nil, fmt.Errorf("invalid output: %w", err)
	}
	valerr, ok := out.(resp.Err)
	if ok {
		return nil, fmt.Errorf("command error: %s", valerr.Value)
	}
	return out, nil
}

func (c *Client) SendBinary(ctx context.Context, data []byte) error {
	proto := fmt.Sprintf("$%d\r\n", len(data))
	if _, err := c.conn.Write([]byte(proto)); err != nil {
		return err
	}
	if _, err := c.conn.Write(data); err != nil {
		return err
	}
	return nil
}

func (c *Client) SendForget(ctx context.Context, cmd *resp.Command) error {
	arr := cmd.Array()
	if _, err := arr.WriteTo(c.conn); err != nil {
		return err
	}
	return nil
}
