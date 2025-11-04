package gedis

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ttn-nguyen42/gedis/gedis/info"
	"github.com/ttn-nguyen42/gedis/gedis/repl"
)

type Options struct {
	Role      string
	MasterURL string
}

func (o *Options) Info() *info.Info {
	inf := info.NewInfo(version)
	inf.Replication.SetRole(o.Role)
	return inf
}

func (o *Options) Repl() (*repl.Repl, error) {
	if o.Role != "slave" {
		return nil, nil
	}
	return repl.NewRepl(o.MasterURL)
}

type Option func(o *Options)

func AsMaster() Option {
	return func(o *Options) {
		o.Role = "master"
	}
}

func AsSlave(masterURL string) Option {
	return func(o *Options) {
		o.Role = "slave"
		o.MasterURL = masterURL
	}
}

type Instance struct {
	info    *info.Info
	cmdBuf  *circular[*Command]
	stop    chan struct{}
	dbs     []*database
	round   int
	options *Options
	repl    *repl.Repl
}

func NewInstance(cap int, opts ...Option) (*Instance, error) {
	inst := &Instance{
		cmdBuf: newBuffer[*Command](cap),
		stop:   make(chan struct{}, 1),
		dbs:    make([]*database, 16),
		round:  0,
		options: &Options{
			Role: "master",
		},
	}
	for _, opt := range opts {
		opt(inst.options)
	}
	inst.info = inst.options.Info()
	repl, err := inst.options.Repl()
	if err != nil {
		return nil, fmt.Errorf("failed to setup replication: %w", err)
	}
	inst.repl = repl

	log.Printf("gedis instance created with role=%s", inst.options.Role)
	return inst, nil
}

func (i *Instance) Info() *info.Info {
	return i.info
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
	if cmd.ConnState != nil {
		idx = cmd.ConnState.DbNumber
	} else if cmd.DbNumber != nil {
		idx = *cmd.DbNumber
	}
	i.initDb(idx)
	if cmd.ConnState != nil {
		cmd.ConnState.DbNumber = idx
	}
	cmd.DbNumber = &idx
	dbi := i.dbs[idx]

	hdl, err := selectHandler(cmd)
	if err != nil {
		cmd.WriteAny(err)
		cmd.SetDone()
		return
	}

	if err := hdl(dbi, cmd, cmd.ConnState, i.info); err != nil {
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
