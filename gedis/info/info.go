package info

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
)

type Info struct {
	Replication *Replication `resp:"Replication"`
	Clients     *Clients     `resp:"Clients"`
	Server      *Server      `resp:"Server"`
}

func NewInfo(version string) *Info {
	return &Info{
		Replication: &Replication{},
		Clients:     &Clients{},
		Server:      &Server{RedisVersion: version},
	}
}

type Replication struct {
	mu                         sync.RWMutex
	Role                       string `resp:"role"`
	ConnectedSlaves            int    `resp:"connected_slaves"`
	MasterReplID               string `resp:"master_replid"`
	MasterReplOffset           int    `resp:"master_repl_offset"`
	SecondReplOffset           int    `resp:"second_repl_offset"`
	ReplBacklogActive          int    `resp:"repl_backlog_active"`
	ReplBacklogSize            int    `resp:"repl_backlog_size"`
	ReplBacklogFirstByteOffset int    `resp:"repl_backlog_first_byte_offset"`
	ReplBacklogHistoryLen      int    `resp:"repl_backlog_histlen"`
}

func (r *Replication) SetRole(role string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Role = role
}

func (r *Replication) GetRole() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.Role
}

func (r *Replication) SetConnectedSlaves(count int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ConnectedSlaves = count
}

func (r *Replication) GetConnectedSlaves() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.ConnectedSlaves
}

func (r *Replication) IncrConnectedSlaves() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ConnectedSlaves += 1
	return r.ConnectedSlaves
}

func (r *Replication) DecrConnectedSlaves() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ConnectedSlaves > 0 {
		r.ConnectedSlaves -= 1
	}
	return r.ConnectedSlaves
}

func (r *Replication) SetMasterReplID(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.MasterReplID = id
}

func (r *Replication) GetMasterReplID() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.MasterReplID
}

func (r *Replication) SetMasterReplOffset(offset int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.MasterReplOffset = offset
}

func (r *Replication) GetMasterReplOffset() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.MasterReplOffset
}

func (r *Replication) SetSecondReplOffset(offset int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.SecondReplOffset = offset
}

func (r *Replication) GetSecondReplOffset() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.SecondReplOffset
}

func (r *Replication) SetReplBacklogActive(active int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ReplBacklogActive = active
}

func (r *Replication) GetReplBacklogActive() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.ReplBacklogActive
}

func (r *Replication) SetReplBacklogSize(size int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ReplBacklogSize = size
}

func (r *Replication) GetReplBacklogSize() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.ReplBacklogSize
}

func (r *Replication) SetReplBacklogFirstByteOffset(offset int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ReplBacklogFirstByteOffset = offset
}

func (r *Replication) GetReplBacklogFirstByteOffset() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.ReplBacklogFirstByteOffset
}

func (r *Replication) SetReplBacklogHistoryLen(len int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ReplBacklogHistoryLen = len
}

func (r *Replication) GetReplBacklogHistoryLen() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.ReplBacklogHistoryLen
}

func (i *Info) GetRepl() *Replication {
	return i.Replication
}

func (i *Info) GetClients() *Clients {
	return i.Clients
}

func (i *Info) GetServer() *Server {
	return i.Server
}

func (i *Info) String() string {
	fields := i.Fields()
	buf := strings.Builder{}
	for _, field := range fields {
		buf.WriteString("# ")
		buf.WriteString(field.Name)
		buf.WriteString("\n")
		buf.WriteString(strings.TrimSpace(fmt.Sprintf("%v", field.Value)))
		buf.WriteString("\n")
	}
	return buf.String()
}

type Field struct {
	Name  string
	Value any
}

func (i *Info) Fields() []Field {
	fields := []Field{}
	val := reflect.ValueOf(i).Elem() // Use Elem() to get the value that the pointer points to
	typ := reflect.TypeOf(i).Elem()

	for i := 0; i < val.NumField(); i += 1 {
		f := val.Field(i)
		fieldType := typ.Field(i)
		tag := fieldType.Tag.Get("resp")
		if tag != "" {
			fields = append(fields, Field{Name: tag, Value: f.Interface()})
		}
	}

	return fields
}

func (r *Replication) String() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fields := r.Fields()
	buf := strings.Builder{}

	for _, field := range fields {
		buf.WriteString(field.Name)
		buf.WriteString(":")
		buf.WriteString(fmt.Sprintf("%v", field.Value))
		buf.WriteString("\n")
	}
	return buf.String()
}

func (r *Replication) Fields() []Field {
	fields := []Field{}
	val := reflect.ValueOf(r).Elem()
	typ := reflect.TypeOf(r).Elem()

	for i := 0; i < val.NumField(); i += 1 {
		f := val.Field(i)
		fieldType := typ.Field(i)
		tag := fieldType.Tag.Get("resp")
		if tag != "" {
			fields = append(fields, Field{Name: tag, Value: f.Interface()})
		}
	}

	return fields
}

type Server struct {
	mu           sync.RWMutex
	RedisVersion string `resp:"redis_version"`
}

func (s *Server) SetRedisVersion(version string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.RedisVersion = version
}

func (s *Server) GetRedisVersion() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.RedisVersion
}

type Clients struct {
	mu               sync.RWMutex
	ConnectedClients int `resp:"connected_clients"`
}

func (c *Clients) SetConnectedClients(count int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ConnectedClients = count
}

func (c *Clients) GetConnectedClients() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ConnectedClients
}

func (c *Clients) IncrConnectedClients() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ConnectedClients++
	return c.ConnectedClients
}

func (c *Clients) DecrConnectedClients() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ConnectedClients > 0 {
		c.ConnectedClients--
	}
	return c.ConnectedClients
}

func (c *Clients) String() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	fields := c.Fields()
	buf := strings.Builder{}

	for _, field := range fields {
		buf.WriteString(field.Name)
		buf.WriteString(":")
		buf.WriteString(fmt.Sprintf("%v", field.Value))
		buf.WriteString("\n")
	}
	return buf.String()
}

func (c *Clients) Fields() []Field {
	fields := []Field{}
	val := reflect.ValueOf(c).Elem()
	typ := reflect.TypeOf(c).Elem()

	for i := 0; i < val.NumField(); i += 1 {
		f := val.Field(i)
		fieldType := typ.Field(i)
		tag := fieldType.Tag.Get("resp")
		if tag != "" {
			fields = append(fields, Field{Name: tag, Value: f.Interface()})
		}
	}

	return fields
}

func (s *Server) String() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	fields := s.Fields()
	buf := strings.Builder{}

	for _, field := range fields {
		buf.WriteString(field.Name)
		buf.WriteString(":")
		buf.WriteString(fmt.Sprintf("%v", field.Value))
		buf.WriteString("\n")
	}
	return buf.String()
}

func (s *Server) Fields() []Field {
	fields := []Field{}
	val := reflect.ValueOf(s).Elem()
	typ := reflect.TypeOf(s).Elem()

	for i := 0; i < val.NumField(); i += 1 {
		f := val.Field(i)
		fieldType := typ.Field(i)
		tag := fieldType.Tag.Get("resp")
		if tag != "" {
			fields = append(fields, Field{Name: tag, Value: f.Interface()})
		}
	}

	return fields
}
