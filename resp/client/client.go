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
