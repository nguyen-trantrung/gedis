package main

import (
	"context"

	"github.com/ttn-nguyen42/gedis/server"
)

func main() {
	ctx := server.CancelOnSignal(context.Background())
	s := server.NewServer("localhost", 6379)
	if err := s.Run(ctx); err != nil {
		panic(err)
	}
}
