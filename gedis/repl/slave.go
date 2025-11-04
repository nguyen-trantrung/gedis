package repl

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/ttn-nguyen42/gedis/resp"
	resp_client "github.com/ttn-nguyen42/gedis/resp/client"
)

type hostPort struct {
	host string
	port int
}

func (hp *hostPort) String() string {
	return fmt.Sprintf("%s:%d", hp.host, hp.port)
}

func (hp *hostPort) Client() (*resp_client.Client, error) {
	return resp_client.NewClient(hp.host, hp.port)
}

func newHostPort(url string) (*hostPort, error) {
	hp := &hostPort{}
	args := strings.Split(url, " ")
	if len(args) != 2 {
		return nil, fmt.Errorf("invalid master url")
	}
	hostStr, portStr := args[0], args[1]
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return nil, fmt.Errorf("invalid master port")
	}
	hp.host = hostStr
	hp.port = port
	return hp, nil
}

type Slave struct {
	master *hostPort
	client *resp_client.Client
}

func NewSlave(masterUrl string) (*Slave, error) {
	slave := &Slave{}
	if err := slave.init(masterUrl); err != nil {
		return nil, err
	}
	return slave, nil
}

func (s *Slave) MasterUrl() string {
	if s.master == nil {
		return ""
	}
	return s.master.String()
}

func (s *Slave) init(masterUrl string) error {
	if len(masterUrl) == 0 {
		return nil
	}
	hp, err := newHostPort(masterUrl)
	if err != nil {
		return err
	}
	s.master = hp
	s.client, err = hp.Client()
	if err != nil {
		return err
	}
	return nil
}

func (s *Slave) Handshake(ctx context.Context) error {
	if err := s.ping(ctx); err != nil {
		return fmt.Errorf("ping master err: %w", err)
	}
	log.Printf("handshake ping master success")
	// if err := s.replConf(ctx); err != nil {
	// 	return fmt.Errorf("replconf master err: %w", err)
	// }
	// if err := s.replConf(ctx); err != nil {
	// 	return fmt.Errorf("replconf master err: %w", err)
	// }
	// if err := s.psync(ctx); err != nil {
	// 	return fmt.Errorf("psync master err: %w", err)
	// }
	return nil
}

func (s *Slave) ping(ctx context.Context) error {
	cmd := resp.Command{Cmd: "PING", Args: nil}
	_, err := s.client.SendSync(ctx, &cmd)
	return err
}

func (s *Slave) replConf(ctx context.Context) error {
	cmd := resp.Command{Cmd: "REPLCONF", Args: []any{"listening-port", 6379, "capa", "eof"}}
	_, err := s.client.SendSync(ctx, &cmd)
	return err
}

func (s *Slave) psync(ctx context.Context) error {
	cmd := resp.Command{Cmd: "PSYNC", Args: nil}
	_, err := s.client.SendSync(ctx, &cmd)
	return err
}
