package gedis

import (
	"bytes"
	"io"
	"log"

	"github.com/ttn-nguyen42/gedis/resp"
)

type Bytes []byte

type Command struct {
	Cmd      resp.Command
	Addr     string
	DbNumber *int
	out      *bytes.Buffer
	done     chan bool
}

func NewCommand(cmd resp.Command, addr string, dbNumber int) *Command {
	return &Command{
		Cmd:      cmd,
		Addr:     addr,
		DbNumber: &dbNumber,
		done:     make(chan bool, 1),
	}
}

func (c *Command) SetDone() {
	c.done <- true
}

func (c *Command) Done() <-chan bool {
	return c.done
}

func (c *Command) SelectDb(dbNum int) {
	c.DbNumber = &dbNum
}

func (c *Command) SetOutput(data Bytes) {
	c.out = bytes.NewBuffer(data)
}

func (c *Command) initBuf(d []byte) bool {
	if c.out == nil {
		c.out = bytes.NewBuffer(d)
		return true
	}
	return false
}

func (c *Command) Write(d []byte) (n int, err error) {
	if c.initBuf(d) {
		return len(d), nil
	}
	log.Println("Good n=", len(d))
	return c.out.Write(d)
}

func (c *Command) WriteAny(d any) (n int64, err error) {
	switch val := d.(type) {
	case io.WriterTo:
		return val.WriteTo(c)
	default:
		return resp.WriteAnyTo(d, c)
	}
}

func (c *Command) Len() int {
	if c.out == nil {
		return 0
	}
	return c.out.Len()
}

func (c *Command) Bytes() Bytes {
	if c.out == nil {
		return nil
	}
	return c.out.Bytes()
}

func (c *Command) WriteTo(str io.Writer) (n int64, err error) {
	if c.out == nil {
		return 0, nil
	}

	return c.out.WriteTo(str)
}
