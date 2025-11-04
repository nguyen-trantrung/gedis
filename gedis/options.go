package gedis

import (
	"fmt"

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

func (o *Options) Slave() (*repl.Slave, error) {
	slave, err := repl.NewSlave(o.MasterURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create slave: %w", err)
	}
	return slave, nil
}

func (o *Options) Master(info *info.Info) *repl.Master {
	return repl.NewMaster(info)
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
