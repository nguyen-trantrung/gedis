package repl

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

type Repl struct {
	masterUrlStr string
	host         string
	port         int
}

func NewRepl(masterUrl string) (*Repl, error) {
	repl := &Repl{
		masterUrlStr: masterUrl,
	}
	if err := repl.validate(); err != nil {
		return nil, err
	}
	return repl, nil
}

func (r *Repl) validate() error {
	if len(r.masterUrlStr) == 0 {
		return nil
	}

	args := strings.Split(r.masterUrlStr, " ")
	if len(args) != 2 {
		return fmt.Errorf("invalid master url")
	}
	hostStr, portStr := args[0], args[1]
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return fmt.Errorf("invalid master port")
	}
	r.host = hostStr
	r.port = port
	return nil
}

func (r *Repl) Handshake(ctx context.Context) error {
	return nil
}
