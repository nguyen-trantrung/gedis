package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/ttn-nguyen42/gedis/gedis"
	"github.com/ttn-nguyen42/gedis/resp"
)

type Server struct {
	lis  net.Listener
	host string
	port int
	wg   sync.WaitGroup
	done chan struct{}
	core *gedis.Instance
}

func NewServer(host string, port int) *Server {
	s := &Server{
		host: host,
		port: port,
		done: make(chan struct{}, 1),
		core: gedis.NewInstance(256),
	}
	return s
}

func (s *Server) Run(ctx context.Context) error {
	if err := s.startConn(); err != nil {
		return fmt.Errorf("failed to start connection: %w", err)
	}

	if err := s.core.Run(ctx); err != nil {
		return fmt.Errorf("failed to start core process: %w", err)
	}

	log.Printf("%v:%v attached, server is running", s.host, s.port)

	return s.loop(ctx)
}

func (s *Server) startConn() error {
	var err error
	s.lis, err = net.Listen("tcp", net.JoinHostPort(s.host, strconv.Itoa(s.port)))
	return err
}

func (s *Server) loop(ctx context.Context) error {
	go s.closeConn(ctx)
	for {
		conn, err := s.lis.Accept()
		if err != nil {
			return s.handleConnErr(err)
		}
		s.wg.Add(1)
		s.handleConn(ctx, conn)
	}
}

func (s *Server) handleConnErr(err error) error {
	select {
	case <-s.done:
		return nil
	default:
		return err
	}
}

func (s *Server) closeConn(ctx context.Context) error {
	<-ctx.Done()
	close(s.done)
	err := s.lis.Close()
	if err != nil {
		log.Printf("close connection err: %+v", err)
	}
	s.wg.Wait()

	err = s.core.Stop()
	if err != nil {
		log.Printf("stopping core err: %+v", err)
	}

	return nil
}

type connState struct {
	mu      sync.Mutex
	pending []*gedis.Command
}

func (s *Server) handleConn(baseCtx context.Context, conn net.Conn) {
	defer conn.Close()
	log.Printf("connection established, addr=%s", conn.RemoteAddr())
	ctx, cancel := context.WithCancel(baseCtx)
	defer cancel()

	state := &connState{pending: make([]*gedis.Command, 0)}

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			conn.SetDeadline(time.Now().Add(time.Hour))
			cmd, err := resp.ParseStream(conn)
			brk, cont := s.handleProtoError(err, conn)
			if !brk {
				cancel()
				return
			}
			if !cont {
				continue
			}

			rCmd := gedis.NewCommand(cmd, conn.RemoteAddr().String())
			state.mu.Lock()
			state.pending = append(state.pending, rCmd)
			state.mu.Unlock()

			if err := s.core.Submit(ctx, []*gedis.Command{rCmd}); err != nil {
				var netOpErr *net.OpError
				if errors.Is(err, context.DeadlineExceeded) {
					log.Printf("context canceled, stop submitting commands")
				} else if !errors.As(err, &netOpErr) {
					log.Printf("submit err: %+v", err)
				}
				cancel()
				return
			}
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

			state.mu.Lock()
			if len(state.pending) > 0 {
				select {
				case <-state.pending[0].Done():
					respn, err := state.pending[0].WriteTo(conn)
					if err != nil {
						log.Printf("write err to TCP, err=%s, addr=%s", err, conn.RemoteAddr())
					} else {
						log.Printf("written to TCP stream, addr=%s n=%d", conn.RemoteAddr(), respn)
					}
					state.pending = state.pending[1:]
				default:
				}
			}
			state.mu.Unlock()

			time.Sleep(10 * time.Millisecond)
		}
	}()

	wg.Wait()
}

func (s *Server) handleProtoError(err error, conn net.Conn) (brk bool, cont bool) {
	if err == nil {
		return true, true
	}
	if errors.Is(err, net.ErrClosed) {
		return true, false
	}
	if errors.Is(err, resp.ErrProtocolError) || errors.Is(err, resp.ErrInvalidToken) {
		log.Printf("client sent invalid command, err=%s", err)
		out := resp.Err{Value: err.Error(), Size: len(err.Error())}
		_, err = out.WriteTo(conn)
		if err != nil {
			log.Printf("error writing to tcp stream, err=%s", err)
			return true, false
		}
		return true, false
	}
	log.Printf("connection error, addr=%s, err=%s", conn.RemoteAddr(), err)
	return false, false
}
