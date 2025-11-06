package gedis_types

import (
	"net"
)

type ConnState struct {
	InTransaction bool
	Tx            []*Command
	DbNumber      int
	Conn          net.Conn
	isRepl        bool
	isSub         bool
	subId         string
}

func NewConnState(conn net.Conn) *ConnState {
	return &ConnState{
		InTransaction: false,
		DbNumber:      0,
		Tx:            make([]*Command, 0),
		Conn:          conn,
		isRepl:        false,
	}
}

func (c *ConnState) UpgradeToReplication() {
	c.isRepl = true
}

func (c *ConnState) IsReplication() bool {
	return c.isRepl
}

func (c *ConnState) UpgradeToSubscription(id string) {
	c.isSub = true
	c.subId = id
}

func (c *ConnState) IsSubscription() bool {
	return c.isSub
}

func (c *ConnState) SubId() string {
	return c.subId
}

func (c *ConnState) QuitSubscription() {
	c.isSub = false
	c.subId = ""
}
