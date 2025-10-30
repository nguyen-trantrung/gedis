package server

import "net"

type server struct {
	conn *net.Conn
}

