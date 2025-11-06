package gedis

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ttn-nguyen42/gedis/data"
	"github.com/ttn-nguyen42/gedis/gedis/info"
	"github.com/ttn-nguyen42/gedis/gedis/repl"
	gedis_types "github.com/ttn-nguyen42/gedis/gedis/types"
)

type Instance struct {
	info              *info.Info
	cmdBuf            *data.CircularBuffer[*gedis_types.Command]
	stop              chan struct{}
	dbs               []*database
	handlers          map[int]*handlers
	round             int
	options           *Options
	slave             *repl.Slave
	master            *repl.Master
	lastOffsetAskTime time.Time
}

func NewInstance(cap int, opts ...Option) (*Instance, error) {
	inst := &Instance{
		cmdBuf:   data.NewCircularBuffer[*gedis_types.Command](cap),
		stop:     make(chan struct{}, 1),
		dbs:      make([]*database, 16),
		handlers: make(map[int]*handlers, 16),
		round:    0,
		options: &Options{
			Role: "master",
		},
	}
	for _, opt := range opts {
		opt(inst.options)
	}

	if err := inst.init(); err != nil {
		return nil, err
	}
	return inst, nil
}

func (i *Instance) init() error {
	i.info = i.options.Info()

	switch {
	case i.isMaster():
		i.master = i.options.Master(i.info)
	case i.isSlave():
		slave, err := i.options.Slave()
		if err != nil {
			return err
		}
		i.slave = slave
	default:
		return fmt.Errorf("invalid gedis role: %s", i.options.Role)
	}

	log.Printf("gedis instance created with role=%s", i.options.Role)
	return nil
}

func (i *Instance) Info() *info.Info {
	return i.info
}

func (i *Instance) isSlave() bool {
	return i.options.Role == "slave"
}

func (i *Instance) isMaster() bool {
	return i.options.Role == "master"
}

func (i *Instance) Run(ctx context.Context) error {
	log.Printf("gedis core is running")
	if err := i.startReplicate(ctx); err != nil {
		return fmt.Errorf("begin replication failed: %w", err)
	}
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

func (i *Instance) loop(ctx context.Context) {
	dbi := i.dbs[i.round%len(i.dbs)]
	if dbi != nil {
		dbi.EvictHashMap()
	}

	if i.isSlave() {
		replCmds := i.slave.GetChanges(10)
		for _, cmd := range replCmds {
			log.Printf("repl command received, type '%s', addr=%s", cmd.Cmd.Cmd, cmd.Addr)
			i.processCmd(ctx, cmd)
		}
	}

	if i.isMaster() {
		pendingWaits := 0
		for _, h := range i.handlers {
			pendingWaits += h.countWaits()
		}

		if pendingWaits > 0 {
			// now := time.Now()
			// if i.master.GetSlaveCount() > 0 && now.Sub(i.lastOffsetAskTime) >= 50*time.Millisecond {
			// 	i.lastOffsetAskTime = now
			// 	_, err := i.master.AskOffsets(ctx)
			// 	if err != nil {
			// 		log.Printf("failed to ask offsets from slaves: %v", err)
			// 	}
			// }

			for _, h := range i.handlers {
				h.resolveWaits(i.master.GetSlaveCount())
			}
		}
	}

	if i.isMaster() {
		err := i.master.InitialRdbSync()
		if err != nil {
			log.Printf("failed to perform initial RDB sync to slaves: %v", err)
		}
	}

	cmds := i.cmdBuf.ReadBatch(10)
	for _, cmd := range cmds {
		log.Printf("command received, type '%s', addr=%s", cmd.Cmd.Cmd, cmd.Addr)
		i.processCmd(ctx, cmd)
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
		i.handlers[idx] = newHandlers(i.dbs[idx], i.info, i.master, i.slave)
	}
	return nil
}

func (i *Instance) processCmd(ctx context.Context, cmd *gedis_types.Command) {
	dbn := cmd.Db()
	i.initDb(dbn)

	handlers := i.handlers[dbn]

	hdl, shouldReplicate, err := handlers.route(cmd)
	if err != nil {
		cmd.WriteAny(err)
		cmd.SetDone()
		return
	}

	if err := hdl(cmd); err != nil {
		cmd.WriteAny(err)
		cmd.SetDone()
	}

	if i.isSlave() && cmd.IsRepl() && !cmd.OmitOffset() {
		i.slave.IncrOffset(cmd.Cmd.Size)
	}

	if !shouldReplicate {
		return
	}

	if i.isMaster() {
		if err := i.master.Repl(ctx, dbn, cmd.Cmd); err != nil {
			log.Printf("failed to replicate command to slaves: %v", err)
		}
	}
}

func (i *Instance) startReplicate(ctx context.Context) error {
	if i.isSlave() {
		return i.startSlave(ctx)
	}

	return i.startMaster(ctx)
}

func (i *Instance) startSlave(ctx context.Context) error {
	log.Printf("begin handshake with master, master=%s", i.slave.MasterUrl())
	if err := i.slave.Handshake(ctx); err != nil {
		return fmt.Errorf("slave handshake failed: %w", err)
	}
	log.Printf("handshake with master successful, master=%s", i.slave.MasterUrl())
	return nil
}

func (i *Instance) startMaster(_ context.Context) error {
	return nil
}

func (i *Instance) Submit(ctx context.Context, cmds []*gedis_types.Command) error {
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
