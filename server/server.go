package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
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
		core: gedis.NewInstance(),
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
		go func() {
			defer s.wg.Done()
			s.handleConn(ctx, conn)
		}()
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

func (s *Server) handleConn(baseCtx context.Context, conn net.Conn) {
	defer conn.Close()
	log.Printf("connection established, addr=%s", conn.RemoteAddr())
	ctx, cancel := context.WithCancel(baseCtx)
	defer cancel()

	for {
		conn.SetDeadline(time.Now().Add(time.Hour * 1))

		cmd, err := resp.ParseStream(conn)
		if err != nil && err != io.EOF {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				log.Printf("connection timeout, addr=%s", conn.RemoteAddr())
				return
			}
			log.Println("read error", err)
			return
		}

		if err == io.EOF {
			log.Printf("connection closed, addr=%s", conn.RemoteAddr())
			break
		}

		rCmds := make([]gedis.RespondableCmd, 0, 1)
		rCmds = append(rCmds, gedis.RespondableCmd{Cmd: cmd, Resp: conn})

		if err := s.core.Submit(ctx, rCmds); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				log.Printf("context canceled, stop submitting commands")
				return
			}
			log.Printf("submit err: %+v", err)
			return
		}
	}
}
