package repl

import (
	"math/rand"
	"sync"
	"time"

	"github.com/ttn-nguyen42/gedis/gedis/info"
)

type slaveData struct {
	theirPort int
	proto     string
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
