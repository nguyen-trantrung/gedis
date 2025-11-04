package main

import (
	"context"
	"flag"

	"github.com/ttn-nguyen42/gedis/server"
)

func main() {
	ctx := server.CancelOnSignal(context.Background())
	s := getServer()
	if err := s.Run(ctx); err != nil {
		panic(err)
	}
}

func getServer() *server.Server {
	var port int
	flag.IntVar(&port, "port", 6379, "Specify port to use")

	var host string
	flag.StringVar(&host, "host", "0.0.0.0", "Specify hostname to use")

	flag.Parse()

	return server.NewServer(host, port)
}
