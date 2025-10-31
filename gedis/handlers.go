package gedis

import (
	"fmt"
	"strings"

	"github.com/ttn-nguyen42/gedis/resp"
)

type Handler func(db *database, cmd *Command) error

var hmap = map[string]Handler{
	"ping": handlePing,
}

func selectHandler(cmd *Command) (Handler, error) {
	r := cmd.Cmd
	hdlr, found := hmap[strings.ToLower(r.Cmd)]
	if !found {
		return nil, fmt.Errorf("%w: invalid command '%s'", resp.ErrProtocolError, r.Cmd)
	}
	return hdlr, nil
}

func handlePing(db *database, cmd *Command) error {
	defer cmd.SetDone()

	cmd.WriteAny("PONG")
	return nil
}
