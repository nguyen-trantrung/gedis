package repl

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/ttn-nguyen42/gedis/gedis/info"
	resp_client "github.com/ttn-nguyen42/gedis/resp/client"
)

// Add support for RDB later
const EMPTY_RDB_BASE64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

type slaveData struct {
	theirPort int
	proto     string
	conn      net.Conn
	client    *resp_client.Client
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
}

func NewMaster(info *info.Info) *Master {
	m := &Master{
		replId:     randomId(40),
		replOffset: 0,
		info:       info,
		slaves:     make(map[string]*slaveData),
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

	bdata, _ := base64.StdEncoding.DecodeString(EMPTY_RDB_BASE64)
	err := sd.client.SendBinary(context.TODO(), bdata)
	if err != nil {
		return fmt.Errorf("failed to sync RDB to slave: %w", err)
	}

	return nil
}
