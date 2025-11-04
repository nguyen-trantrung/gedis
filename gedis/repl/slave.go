package repl

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

type Slave struct {
	masterUrlStr string
	host         string
	port         int
}

func NewSlave(masterUrl string) (*Slave, error) {
	slave := &Slave{
		masterUrlStr: masterUrl,
	}
	if err := slave.validate(); err != nil {
		return nil, err
	}
	return slave, nil
}

func (s *Slave) validate() error {
	if len(s.masterUrlStr) == 0 {
		return nil
	}

	args := strings.Split(s.masterUrlStr, " ")
	if len(args) != 2 {
		return fmt.Errorf("invalid master url")
	}
	hostStr, portStr := args[0], args[1]
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return fmt.Errorf("invalid master port")
	}
	s.host = hostStr
	s.port = port
	return nil
}

func (s *Slave) Handshake(ctx context.Context) error {
	return nil
}
