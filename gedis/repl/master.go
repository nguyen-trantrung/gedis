package repl

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ttn-nguyen42/gedis/data"
	"github.com/ttn-nguyen42/gedis/gedis/info"
	"github.com/ttn-nguyen42/gedis/resp"
	resp_client "github.com/ttn-nguyen42/gedis/resp/client"
)

const EMPTY_RDB_BASE64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

type HandshakeStep string

const (
	HandshakeListeningPort HandshakeStep = "listening-port"
	HandshakeCapa          HandshakeStep = "capa"
	HandshakePsync         HandshakeStep = "psync"
)

type slaveData struct {
	theirPort        int
	proto            string
	conn             net.Conn
	client           *resp_client.Client
	currDb           int
	isSyncing        bool
	hndshkProcedures map[HandshakeStep]bool
	isReady          bool
	lastOffset       int
}

func (s *slaveData) completeHandshake() bool {
	return s.isReady
}

var seededRand = newSeededRand()

func newSeededRand() *rand.Rand {
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}

type Master struct {
	mu             sync.RWMutex
	replId         string
	replOffset     int64
	info           *info.Info
	slaves         map[string]*slaveData
	buf            *data.CircularBuffer[resp.Command]
	curInsyncCount int
	isDirty        bool
}

func NewMaster(info *info.Info) *Master {
	m := &Master{
		replId:     randomId(40),
		replOffset: 0,
		info:       info,
		slaves:     make(map[string]*slaveData),
		buf:        data.NewCircularBuffer[resp.Command](1024),
		isDirty:    true,
	}
	m.syncInfo()
	return m
}

func randomId(l int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, l)
	for i := range b {
		b[i] = letters[seededRand.Intn(len(letters))]
	}
	return string(b)
}

func (m *Master) syncInfo() {
	m.info.GetRepl().SetMasterReplID(m.replId)
	m.info.GetRepl().SetMasterReplOffset(int(m.replOffset))
	m.info.GetRepl().SetConnectedSlaves(len(m.slaves))
}

func (m *Master) ReplId() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.replId
}

func (m *Master) ReplOffset() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.replOffset
}

func (m *Master) AddSlave(conn net.Conn, theirPort int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	defer m.syncInfo()

	sd := &slaveData{
		theirPort:        theirPort,
		proto:            "",
		conn:             conn,
		currDb:           -1,
		isSyncing:        false,
		hndshkProcedures: make(map[HandshakeStep]bool),
	}
	sd.client = resp_client.NewClientFromConn(conn)
	m.slaves[conn.RemoteAddr().String()] = sd

	return nil
}

func (m *Master) GetSlave(addr string) (*slaveData, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	sd, ok := m.slaves[addr]
	return sd, ok
}

func (m *Master) IsSyncing(addr string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	sd, ok := m.slaves[addr]
	if !ok {
		return false
	}
	return sd.isSyncing
}

func (m *Master) AddHandshakeStep(addr string, step HandshakeStep) {
	m.mu.Lock()
	defer m.mu.Unlock()

	sd, ok := m.slaves[addr]
	if !ok {
		return
	}
	sd.hndshkProcedures[step] = true
}

func (m *Master) HasHandshakeStep(addr string, step HandshakeStep) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	sd, ok := m.slaves[addr]
	if !ok {
		return false
	}
	return sd.hndshkProcedures[step]
}

func (m *Master) StartSync(addr string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	sd, ok := m.slaves[addr]
	if !ok {
		return false
	}
	sd.isSyncing = true
	return true
}

func (m *Master) IsReady(addr string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	sd, ok := m.slaves[addr]
	if !ok {
		return false
	}
	return sd.isReady
}

func (m *Master) SetSlaveProto(conn net.Conn, proto string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	sd, ok := m.slaves[conn.RemoteAddr().String()]
	if !ok {
		return false
	}
	sd.proto = proto
	return true
}

func (m *Master) RemoveSlave(conn net.Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()

	defer m.syncInfo()

	delete(m.slaves, conn.RemoteAddr().String())
}

func (m *Master) InitialRdbSync() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	defer m.syncInfo()

	bdata, err := base64.StdEncoding.DecodeString(EMPTY_RDB_BASE64)
	if err != nil {
		return fmt.Errorf("failed to decode RDB data: %w", err)
	}

	done := make([]*slaveData, 0, len(m.slaves))

	for _, sd := range m.slaves {
		if !sd.isSyncing {
			continue
		}

		client := sd.client

		log.Printf("starting RDB sync to slave, addr=%s", sd.client.RemoteAddr())

		_, err = client.SendBinary(context.TODO(), bdata)
		if err != nil {
			return fmt.Errorf("failed to sync RDB to slave: %w", err)
		}

		done = append(done, sd)

		log.Printf("RDB sync to slave completed, addr=%s", sd.client.RemoteAddr())
	}

	for _, sd := range done {
		sd.isSyncing = false
		sd.isReady = true
	}

	if len(done) > 0 {
		log.Printf("RDB initial sync completed")
	}
	return nil
}

func (m *Master) isDisconnectedErr(err error, sd *slaveData) bool {
	if errors.Is(err, io.EOF) {
		log.Printf("slave connection closed, addr=%s", sd.client.RemoteAddr())
		return true
	}
	if errors.Is(err, io.ErrClosedPipe) {
		log.Printf("slave connection closed, addr=%s", sd.client.RemoteAddr())
		return true
	}
	var op *net.OpError
	if errors.As(err, &op) {
		if strings.Contains(op.Error(), "closed") {
			log.Printf("slave connection closed, addr=%s", sd.client.RemoteAddr())
			return true
		}
	}
	return false
}

func (m *Master) Repl(ctx context.Context, db int, cmd resp.Command) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	defer m.syncInfo()

	remv := make([]string, 0, len(m.slaves))

	for sk, sd := range m.slaves {
		if !sd.completeHandshake() {
			continue
		}

		// if err := m.selectDb(ctx, sd, db); err != nil {
		// 	if isDisconnected(err, sk, sd) {
		// 		continue
		// 	}
		// 	return fmt.Errorf("failed to select db on slave, addr=%s: %w", sd.client.RemoteAddr(), err)
		// }
		_, err := sd.client.SendForget(ctx, cmd)
		if err != nil {
			if m.isDisconnectedErr(err, sd) {
				remv = append(remv, sk)
				continue
			}
			return fmt.Errorf("failed to send repl command to slave, addr=%s: %w", sd.client.RemoteAddr(), err)
		}
	}

	m.addOffset(cmd.Size)

	if len(remv) > 0 {
		for _, sk := range remv {
			delete(m.slaves, sk)
		}
	}

	m.buf.Send(ctx, cmd)
	return nil
}

func (m *Master) selectDb(ctx context.Context, sd *slaveData, db int) error {
	if sd.currDb == db {
		return nil
	}
	items := []any{"SELECT", fmt.Sprintf("%d", db)}
	selectCmd, err := resp.NewCommand(resp.Array{Size: len(items), Items: items})
	if err != nil {
		return fmt.Errorf("failed to create SELECT command: %w", err)
	}
	n, err := sd.client.SendForget(context.TODO(), selectCmd)
	if err != nil {
		return fmt.Errorf("failed to select db %d on slave: %w", db, err)
	}
	sd.currDb = db
	m.buf.Send(ctx, selectCmd)
	m.addOffset(n)
	return nil
}

func (m *Master) addOffset(n int) {
	m.replOffset += int64(n)
}

func (m *Master) AskOffsets(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rm := make([]string, 0, len(m.slaves))
	insyncCount := 0

	if !m.isDirty {
		return nil
	}

	var err error

	var written int
	for sk, sd := range m.slaves {
		if !sd.completeHandshake() {
			continue
		}
		var offset int

		offset, written, err = m.askOffset(ctx, sd)
		if err != nil {
			if m.isDisconnectedErr(err, sd) {
				rm = append(rm, sk)
				continue
			}

			if errors.Is(err, context.DeadlineExceeded) {
				log.Printf("deadline exceeded asking offset from slave %s", sd.client.RemoteAddr())
			} else {
				log.Printf("failed to ask offset from slave %s: %v", sd.client.RemoteAddr(), err)
			}
		}

		if offset != -1 {
			sd.lastOffset = offset
		}

		if m.replOffset == int64(sd.lastOffset) {
			insyncCount += 1
		}
	}

	m.replOffset += int64(written)
	if insyncCount == len(m.slaves) {
		m.isDirty = false
	}

	for _, sk := range rm {
		delete(m.slaves, sk)
	}

	m.curInsyncCount = insyncCount
	return nil
}

func (m *Master) askOffset(ctx context.Context, sd *slaveData) (int, int, error) {
	getAck := resp.Command{
		Cmd:  "REPLCONF",
		Args: []any{"GETACK", resp.BulkStr{Value: "*", Size: 1}},
	}

	r, n, err := sd.client.SendSync(ctx, getAck)
	if err != nil {
		return -1, 0, fmt.Errorf("failed to send REPLCONF GETACK to slave: %w", err)
	}
	arr, ok := r.(resp.Array)
	if !ok || len(arr.Items) != 3 {
		return -1, 0, fmt.Errorf("invalid REPLCONF GETACK response from slave: %+v", r)
	}
	offsetStr, ok := arr.Items[len(arr.Items)-1].(resp.BulkStr)
	if !ok {
		return -1, 0, fmt.Errorf("invalid REPLCONF GETACK offset from slave: %+v", arr.Items[1])
	}
	offset, err := strconv.Atoi(offsetStr.Value)
	if err != nil {
		return -1, 0, fmt.Errorf("invalid REPLCONF GETACK offset value from slave: %v", offsetStr)
	}
	return offset, n, nil
}

func (s *Master) GetSlaveCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 0
	for _, sd := range s.slaves {
		if sd.isReady {
			count += 1
		}
	}
	return count
}

func (s *Master) InsyncSlaveCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.curInsyncCount
}

func (s *Master) IncrOffset(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.replOffset += int64(n)
	s.isDirty = true
}
