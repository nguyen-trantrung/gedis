package gedis_types

import (
	"net"
	"sync"
)

type HandshakeStep string

const (
	HandshakeListeningPort HandshakeStep = "listening-port"
	HandshakeCapa          HandshakeStep = "capa"
	HandshakePsync         HandshakeStep = "psync"
	HandshakeReadInitRdb   HandshakeStep = "read-init-rdb"
)

type ConnState struct {
	InTransaction    bool
	Tx               []*Command
	DbNumber         int
	Conn             net.Conn
	isRepl           bool
	hndshkProcedures map[HandshakeStep]bool
	syncDone         chan struct{} // Signals when RDB sync is complete (success or failure)
	syncOnce         sync.Once     // Ensures syncDone is closed only once
}

func NewConnState(conn net.Conn) *ConnState {
	return &ConnState{
		InTransaction:    false,
		DbNumber:         0,
		Tx:               make([]*Command, 0),
		Conn:             conn,
		isRepl:           false,
		hndshkProcedures: make(map[HandshakeStep]bool),
		syncDone:         make(chan struct{}),
	}
}

func (c *ConnState) AddHandshakeStep(step HandshakeStep) {
	c.hndshkProcedures[step] = true
}

func (c *ConnState) HasHandshakeStep(step HandshakeStep) bool {
	return c.hndshkProcedures[step]
}

func (c *ConnState) HandshakeComplete() bool {
	return len(c.hndshkProcedures) == 4
}

func (c *ConnState) RsyncSuccess() {
	c.AddHandshakeStep(HandshakeReadInitRdb)
	c.syncOnce.Do(func() {
		close(c.syncDone)
	})
}

func (c *ConnState) RsyncFailed() {
	c.syncOnce.Do(func() {
		close(c.syncDone)
	})
}

// WaitForRepl blocks until RDB sync is complete (success or failure)
// Returns true if sync succeeded and connection should be upgraded to replication
func (c *ConnState) WaitForRepl() bool {
	<-c.syncDone
	return c.HasHandshakeStep(HandshakeReadInitRdb)
}

func (c *ConnState) UpgradeToReplication() {
	c.isRepl = true
}

func (c *ConnState) IsReplication() bool {
	return c.isRepl
}
