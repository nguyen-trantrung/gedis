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
	myPort int
}

func NewSlave(masterUrl string, myPort int) (*Slave, error) {
	slave := &Slave{myPort: myPort}
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

	lp := []any{"listening-port", fmt.Sprintf("%d", s.myPort)}
	if err := s.replConf(ctx, lp); err != nil {
		return fmt.Errorf("replconf master err: %w", err)
	}
	capa := []any{"capa", "psync2"}
	if err := s.replConf(ctx, capa); err != nil {
		return fmt.Errorf("replconf master err: %w", err)
	}
	syncargs := []any{"?", "-1"}
	if err := s.psync(ctx, syncargs); err != nil {
		return fmt.Errorf("psync master err: %w", err)
	}
	return nil
}

func (s *Slave) ping(ctx context.Context) error {
	cmd := resp.Command{Cmd: "PING", Args: nil}
	_, err := s.client.SendSync(ctx, &cmd)
	return err
}

func (s *Slave) replConf(ctx context.Context, args []any) error {
	cmd := resp.Command{Cmd: "REPLCONF", Args: args}
	_, err := s.client.SendSync(ctx, &cmd)
	return err
}

func (s *Slave) psync(ctx context.Context, args []any) error {
	cmd := resp.Command{Cmd: "PSYNC", Args: args}
	_, err := s.client.SendSync(ctx, &cmd)
	return err
}
