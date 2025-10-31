package gedis

import (
	"context"
	"fmt"
	"log"
	"time"
)

type Instance struct {
	cmdBuf *circular[*Command]
	stop   chan struct{}
	dbs    []*database
	round  int
}

func NewInstance(cap int) *Instance {
	return &Instance{
		cmdBuf: newBuffer[*Command](cap),
		stop:   make(chan struct{}, 1),
		dbs:    make([]*database, 16),
		round:  0,
	}
}

func (i *Instance) Run(ctx context.Context) error {
	log.Printf("gedis core is running")
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-i.stop:
				log.Printf("gedis core stopping")
				return
			default:
				i.loop(ctx)
			}
		}
	}()
	return ctx.Err()
}

func (i *Instance) loop(_ context.Context) {
	dbi := i.dbs[i.round%len(i.dbs)]
	if dbi != nil {
		dbi.EvictHashMap()
	}

	cmds := i.cmdBuf.ReadBatch(10)
	for _, cmd := range cmds {
		log.Printf("command received, type '%s', addr=%s", cmd.Cmd.Cmd, cmd.Addr)
		i.processCmd(cmd)
	}
	if len(cmds) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	i.round += 1
}

func (i *Instance) initDb(idx int) error {
	if idx >= len(i.dbs) || idx < 0 {
		return fmt.Errorf("invalid database number, must between 0 and 16: %d", idx)
	}
	if i.dbs[idx] == nil {
		i.dbs[idx] = newDb(idx)
	}
	return nil
}

func (i *Instance) processCmd(cmd *Command) {
	idx := 0
	if cmd.DbNumber != nil {
		idx = *cmd.DbNumber
	}
	i.initDb(idx)
	cmd.DbNumber = &idx
	dbi := i.dbs[idx]

	hdl, err := selectHandler(cmd)
	if err != nil {
		cmd.WriteAny(err)
		cmd.SetDone()
		return
	}

	if err := hdl(dbi, cmd); err != nil {
		cmd.WriteAny(err)
		cmd.SetDone()
		return
	}
}

func (i *Instance) Submit(ctx context.Context, cmds []*Command) error {
	i.cmdBuf.MultiSend(ctx, cmds)
	return ctx.Err()
}

func (i *Instance) Stop() error {
	select {
	case i.stop <- struct{}{}:
	default:
	}
	return nil
}
