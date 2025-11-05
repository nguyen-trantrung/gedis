package repl

import (
	"context"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ttn-nguyen42/gedis/data"
	gedis_types "github.com/ttn-nguyen42/gedis/gedis/types"
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

type slaveState struct {
	mu      sync.Mutex
	pending []*gedis_types.Command
}

type Slave struct {
	master     *hostPort
	client     *resp_client.Client
	myPort     int
	changesBuf *data.CircularBuffer[*gedis_types.Command]
	connState  *gedis_types.ConnState
	replOffset int
	state      *slaveState
}

func NewSlave(masterUrl string, myPort int) (*Slave, error) {
	slave := &Slave{
		myPort:     myPort,
		changesBuf: data.NewCircularBuffer[*gedis_types.Command](1024),
		connState: &gedis_types.ConnState{
			InTransaction: false,
			Tx:            nil,
			DbNumber:      0,
			Conn:          nil,
		},
		replOffset: 0,
		state: &slaveState{
			pending: make([]*gedis_types.Command, 0),
		},
	}
	if err := slave.init(masterUrl); err != nil {
		return nil, err
	}
	slave.connState.Conn = slave.client.Conn()
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
		return fmt.Errorf("replconf listening-port err: %w", err)
	}
	capa := []any{"capa", "psync2"}
	if err := s.replConf(ctx, capa); err != nil {
		return fmt.Errorf("replconf capa err: %w", err)
	}
	syncargs := []any{"?", "-1"}
	if err := s.psync(ctx, syncargs); err != nil {
		return fmt.Errorf("psync master err: %w", err)
	}
	log.Printf("handshake psync master success")
	if err := s.readInitRdb(); err != nil {
		return fmt.Errorf("read initial RDB err: %w", err)
	}
	log.Printf("handshake read initial RDB success")
	s.beginHandleSyncs(ctx)
	return nil
}

func (s *Slave) ping(ctx context.Context) error {
	cmd := resp.Command{Cmd: "PING", Args: nil}
	_, _, err := s.client.SendSync(ctx, cmd)
	return err
}

func (s *Slave) replConf(ctx context.Context, args []any) error {
	cmd := resp.Command{Cmd: "REPLCONF", Args: args}
	_, _, err := s.client.SendSync(ctx, cmd)
	return err
}

func (s *Slave) psync(ctx context.Context, args []any) error {
	cmd := resp.Command{Cmd: "PSYNC", Args: args}
	_, _, err := s.client.SendSync(ctx, cmd)
	return err
}

func (s *Slave) readInitRdb() error {
	value, err := resp.ParseRDBFile(s.client)
	if err != nil {
		return fmt.Errorf("failed to read init RDB from master: %w", err)
	}
	log.Printf("received initial RDB from master, len=%d", len(value))
	return nil
}

func (s *Slave) beginHandleSyncs(baseCtx context.Context) error {
	log.Printf("beginning to handle sync commands from master")
	ctx, cancel := context.WithCancel(baseCtx)

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		<-ctx.Done()
		wg.Wait()
		s.client.Close()
	}()

	go func() {
		defer wg.Done()

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			cmd, err := resp.ParseCmd(s.client)
			if err != nil {
				if err == io.EOF {
					log.Printf("master closed connection, stopping")
				} else {
					log.Printf("failed to read sync from master: %s", err)
				}
				cancel()
				return
			}

			log.Printf("received sync command from master: %s, size=%d, repl_offset=%d", cmd.Cmd, cmd.Size, s.ReplOffset())

			replCmd := gedis_types.NewReplCommand(cmd, s.connState, s.master.String())

			s.state.mu.Lock()
			s.state.pending = append(s.state.pending, replCmd)
			s.state.mu.Unlock()

			s.changesBuf.Send(ctx, replCmd)

			s.IncrOffset(cmd.Size)
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			s.state.mu.Lock()
			if len(s.state.pending) > 0 {
				cmd := s.state.pending[0]

				if cmd.IsDone() || cmd.HasTimedOut() {
					if cmd.Len() >= 0 {
						resp, err := cmd.WriteTo(s.connState.Conn)
						if err != nil {
							log.Printf("resp back to master err to TCP, err=%s, addr=%s", err, s.connState.Conn.RemoteAddr())
						} else {
							log.Printf("written back to replication TCP stream, addr=%s n=%d", s.connState.Conn.RemoteAddr(), resp)
						}
					}
					if cmd.Defer != nil {
						cmd.Defer()
					}
					s.state.pending = s.state.pending[1:]
				}
			}
			s.state.mu.Unlock()

			time.Sleep(10 * time.Millisecond)
		}
	}()

	return nil
}

func (s *Slave) GetChanges(n int) []*gedis_types.Command {
	return s.changesBuf.ReadBatch(n)
}

func (s *Slave) IncrOffset(n int) {
	s.replOffset += n
}

func (s *Slave) ReplOffset() int {
	return s.replOffset
}
