package repl

import "sync"

type Master struct {
	mu     sync.RWMutex
	slaves map[string]*Slave
}

func NewMaster() *Master {
	return &Master{
		slaves: make(map[string]*Slave),
	}
}

func (m *Master) AddSlave(slave *Slave) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.slaves[slave.masterUrlStr] = slave
	return nil
}

func (m *Master) RemoveSlave(slave *Slave) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.slaves, slave.masterUrlStr)
	return nil
}

func (m *Master) GetSlaves() []*Slave {
	m.mu.RLock()
	defer m.mu.RUnlock()
	slaves := make([]*Slave, 0, len(m.slaves))
	for _, slave := range m.slaves {
		slaves = append(slaves, slave)
	}
	return slaves
}
