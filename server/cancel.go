package server

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func CancelOnSignal(ctx context.Context) context.Context {
	cctx, cancel := context.WithCancel(ctx)
	go func() {
		defer cancel()
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		select {
		case <-ctx.Done():
			return
		case <-ch:
			log.Printf("cancel signal captured, canceling context")
			return
		}
	}()
	return cctx
}
