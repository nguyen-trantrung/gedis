package repl

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
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

type slaveData struct {
	theirPort int
	proto     string
	conn      net.Conn
	client    *resp_client.Client
	currDb    int
	isSyncing bool
}

var seededRand = newSeededRand()

func newSeededRand() *rand.Rand {
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}

type Master struct {
	mu         sync.RWMutex
	replId     string
	replOffset int64
	info       *info.Info
	slaves     map[string]*slaveData
	buf        *data.CircularBuffer[resp.Command]
}

func NewMaster(info *info.Info) *Master {
	m := &Master{
		replId:     randomId(40),
		replOffset: 0,
		info:       info,
		slaves:     make(map[string]*slaveData),
		buf:        data.NewCircularBuffer[resp.Command](1024),
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
		theirPort: theirPort,
		proto:     "",
		conn:      conn,
		currDb:    -1,
		isSyncing: false,
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

func (m *Master) InitialRdbSync(addr string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	defer m.syncInfo()

	sd, ok := m.slaves[addr]
	if !ok {
		return fmt.Errorf("slave not found: %s", addr)
	}

	bdata, err := base64.StdEncoding.DecodeString(EMPTY_RDB_BASE64)
	if err != nil {
		return fmt.Errorf("failed to decode RDB data: %w", err)
	}

	_, err = sd.client.SendBinary(context.TODO(), bdata)
	if err != nil {
		return fmt.Errorf("failed to sync RDB to slave: %w", err)
	}

	return nil
}

func (m *Master) Repl(ctx context.Context, db int, cmd resp.Command) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	defer m.syncInfo()

	remv := make([]string, 0, len(m.slaves))

	isDisconnected := func(err error, sk string, sd *slaveData) bool {
		var op *net.OpError
		if errors.As(err, &op) {
			if strings.Contains(op.Error(), "closed") {
				log.Printf("slave connection closed, addr=%s", sd.client.RemoteAddr())
				remv = append(remv, sk)
				return true
			}
		}
		return false
	}

	for sk, sd := range m.slaves {
		if sd.isSyncing {
			log.Printf("slave is syncing, skipping repl, addr=%s", sd.client.RemoteAddr())
			continue
		}
		// if err := m.selectDb(ctx, sd, db); err != nil {
		// 	if isDisconnected(err, sk, sd) {
		// 		continue
		// 	}
		// 	return fmt.Errorf("failed to select db on slave, addr=%s: %w", sd.client.RemoteAddr(), err)
		// }
		n, err := sd.client.SendForget(ctx, cmd)
		if err != nil {
			if isDisconnected(err, sk, sd) {
				continue
			}
			return fmt.Errorf("failed to send repl command to slave, addr=%s: %w", sd.client.RemoteAddr(), err)
		}
		m.addOffset(n)
	}

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

func (m *Master) AskOffsets(ctx context.Context) ([]int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	offsets := make([]int64, 0, len(m.slaves))
	for _, sd := range m.slaves {
		offset, err := m.askOffset(ctx, sd)
		if err != nil {
			return nil, fmt.Errorf("failed to ask offset from slave %s: %w", sd.client.RemoteAddr(), err)
		}
		offsets = append(offsets, int64(offset))
	}
	return offsets, nil
}

func (m *Master) askOffset(ctx context.Context, sd *slaveData) (int, error) {
	getAck := resp.Command{
		Cmd:  "REPLCONF",
		Args: []any{"GETACK", "*"},
	}
	r, _, err := sd.client.SendSync(ctx, getAck)
	if err != nil {
		return -1, fmt.Errorf("failed to send REPLCONF GETACK to slave: %w", err)
	}
	arr, ok := r.(resp.Array)
	if !ok || len(arr.Items) != 2 {
		return -1, fmt.Errorf("invalid REPLCONF GETACK response from slave: %+v", r)
	}
	offsetStr, ok := arr.Items[1].(resp.BulkStr)
	if !ok {
		return -1, fmt.Errorf("invalid REPLCONF GETACK offset from slave: %+v", arr.Items[1])
	}
	offset, err := strconv.Atoi(offsetStr.Value)
	if err != nil {
		return -1, fmt.Errorf("invalid REPLCONF GETACK offset value from slave: %v", offsetStr)
	}
	return offset, nil
}
