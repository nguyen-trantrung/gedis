package gedis_types

import (
	"bytes"
	"io"
	"time"

	"github.com/ttn-nguyen42/gedis/resp"
)

type Bytes []byte

type Command struct {
	ConnState       *ConnState
	Cmd             resp.Command
	Addr            string
	Defer           func()
	out             *bytes.Buffer
	done            bool
	timedOut        time.Time
	timeOutProducer func() any
	isRepl          bool
	omitOffset      bool
}

func NewCommand(cmd resp.Command, state *ConnState, addr string) *Command {
	return &Command{
		Cmd:        cmd,
		Addr:       addr,
		ConnState:  state,
		done:       false,
		Defer:      nil,
		isRepl:     false,
		omitOffset: true,
	}
}

func NewReplCommand(cmd resp.Command, state *ConnState, addr string) *Command {
	return &Command{
		Cmd:             cmd,
		Addr:            addr,
		ConnState:       state,
		done:            false,
		Defer:           nil,
		timeOutProducer: nil,
		isRepl:          true,
		omitOffset:      false,
	}
}

func (c *Command) SetDefer(f func()) {
	c.Defer = f
}

func (c *Command) SetDone() {
	c.done = true
}

func (c *Command) OmitOffset() bool {
	return c.omitOffset
}

func (c *Command) SetOmitOffset(omit bool) {
	c.omitOffset = omit
}

func (c *Command) IsDone() bool {
	return c.done
}

func (c *Command) SetTimeout(t time.Time) {
	c.timedOut = t
}

func (c *Command) HasTimedOut() bool {
	return !c.timedOut.IsZero() && c.timedOut.Before(time.Now())
}

func (c *Command) SelectDb(dbNum int) {
	c.ConnState.DbNumber = dbNum
}

func (c *Command) SetOutput(data Bytes) {
	c.out = bytes.NewBuffer(data)
}

func (c *Command) Output() *bytes.Buffer {
	return c.out
}

func (c *Command) SetTimeoutProducer(f func() any) {
	c.timeOutProducer = f
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
		if c.HasTimedOut() {
			if c.timeOutProducer != nil {
				return resp.WriteAnyTo(c.timeOutProducer(), str)
			}
			return resp.WriteAnyTo(resp.Array{Size: -1}, str)
		}

		return 0, nil
	}

	return c.out.WriteTo(str)
}

func (c *Command) Copy() *Command {
	var outCopy *bytes.Buffer
	if c.out != nil {
		outCopy = bytes.NewBuffer(c.out.Bytes())
	}
	cmdCopy := c.Cmd
	return &Command{
		Cmd:             cmdCopy,
		Addr:            c.Addr,
		ConnState:       c.ConnState,
		out:             outCopy,
		done:            c.done,
		timedOut:        c.timedOut,
		timeOutProducer: c.timeOutProducer,
	}
}

func (c *Command) Db() int {
	if c.ConnState != nil {
		return c.ConnState.DbNumber
	}
	return 0
}

func (c *Command) IsRepl() bool {
	return c.isRepl
}
