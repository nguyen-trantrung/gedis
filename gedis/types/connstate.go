package gedis_types

import "net"

type ConnState struct {
	InTransaction bool
	Tx            []*Command
	DbNumber      int
	Conn          net.Conn
}

func NewConnState(conn net.Conn) *ConnState {
	return &ConnState{
		InTransaction: false,
		DbNumber:      0,
		Tx:            make([]*Command, 0),
		Conn:          conn,
	}
}
