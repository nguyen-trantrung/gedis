package util

import (
	"errors"
	"io"
	"net"
	"strings"
)

func IsDisconnected(err error) bool {
	if errors.Is(err, io.EOF) {
		return true
	}
	if errors.Is(err, io.ErrClosedPipe) {
		return true
	}
	var op *net.OpError
	if errors.As(err, &op) {
		if strings.Contains(op.Error(), "closed") {
			return true
		}
	}
	return false
}
