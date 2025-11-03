package gedis

type ConnState struct {
	InTransaction bool
	Tx            []*Command
	DbNumber      int
}

func NewConnState() *ConnState {
	return &ConnState{
		InTransaction: false,
		DbNumber:      0,
		Tx: make([]*Command, 0),
	}
}
