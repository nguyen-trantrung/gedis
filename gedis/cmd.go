package gedis

import (
	"io"

	"github.com/ttn-nguyen42/gedis/resp"
)

type RespondableCmd struct {
	Resp io.Writer
	Cmd  resp.Command
}
