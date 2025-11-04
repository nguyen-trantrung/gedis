package main

import (
	"context"
	"flag"
	"log"
	"os"

	"github.com/ttn-nguyen42/gedis/gedis"
	"github.com/ttn-nguyen42/gedis/server"
)

func main() {
	ctx := server.CancelOnSignal(context.Background())
	s, err := getServer()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	if err := s.Run(ctx); err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

func getServer() (*server.Server, error) {
	var port int
	flag.IntVar(&port, "port", 6379, "Specify port to use")

	var host string
	flag.StringVar(&host, "host", "0.0.0.0", "Specify hostname to use")

	var replicaOf string
	flag.StringVar(&replicaOf, "replicaof", "", "Replica of master at given host:port")

	flag.Parse()

	var opts []gedis.Option
	if len(replicaOf) > 0 {
		opts = append(opts, gedis.AsSlave(replicaOf, port))
	}

	return server.NewServer(host, port, opts...)
}
