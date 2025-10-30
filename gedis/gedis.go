package gedis

import (
	"context"
	"log"
	"time"
)

type Instance struct {
	cmdBuf *circular
}

func NewInstance() *Instance {
	return &Instance{
		cmdBuf: newBuffer(255),
	}
}

func (i *Instance) Run(ctx context.Context) error {
	log.Printf("gedis core is running")
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if cmd, ok := i.cmdBuf.Read(); ok {
					log.Printf("received command: %+v", cmd)
					cmd.Resp.Write([]byte("Hi"))
				} else {
					time.Sleep(10 * time.Millisecond)
				}
			}
		}
	}()
	return ctx.Err()
}

func (i *Instance) Submit(ctx context.Context, cmds []RespondableCmd) error {
	i.cmdBuf.MultiSend(ctx, cmds)
	return ctx.Err()
}

func (i *Instance) Stop() error {
	return nil
}
