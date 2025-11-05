package gedis

import (
	"fmt"
	"log"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ttn-nguyen42/gedis/gedis/info"
	"github.com/ttn-nguyen42/gedis/gedis/repl"
	gedis_types "github.com/ttn-nguyen42/gedis/gedis/types"
	"github.com/ttn-nguyen42/gedis/resp"
)

var ErrInvalidArguments error = fmt.Errorf("invalid arguments")

type handler func(cmd *gedis_types.Command) error

type handlerEntry struct {
	handler         handler
	shouldReplicate bool
}

type handlers struct {
	isSlave bool
	master  *repl.Master
	slave   *repl.Slave
	db      *database
	info    *info.Info
	hmap    map[string]handlerEntry
}

func newHandlers(db *database, info *info.Info, master *repl.Master, slave *repl.Slave) *handlers {
	hdl := &handlers{
		db:      db,
		info:    info,
		hmap:    nil,
		isSlave: slave != nil,
		master:  master,
		slave:   slave,
	}
	hdl.init()
	return hdl
}

func (h *handlers) init() {
	h.hmap = map[string]handlerEntry{
		"ping":     {h.handlePing, false},
		"echo":     {h.handleEcho, false},
		"select":   {h.handleSelect, false},
		"set":      {h.handleSet, true},
		"get":      {h.handleGet, false},
		"rpush":    {h.handleRPush, true},
		"lpush":    {h.handleLPush, true},
		"lpop":     {h.handleLPop, true},
		"rpop":     {h.handleRPop, true},
		"lrange":   {h.handleLRange, false},
		"llen":     {h.handleLLen, false},
		"lindex":   {h.handleLIndex, false},
		"blpop":    {h.handleBlockLpop, false},
		"incr":     {h.handleIncr, true},
		"multi":    {h.handleMulti, true},
		"exec":     {h.handleExec, true},
		"discard":  {h.handleDiscard, true},
		"info":     {h.handleInfo, false},
		"replconf": {h.handleReplConf, false},
		"psync":    {h.handlePsync, false},
	}
}

func parseBulkStr(arg any) (string, error) {
	bulkStr, ok := arg.(resp.BulkStr)
	if !ok {
		return "", fmt.Errorf("%w: expected bulk string, got %T", ErrInvalidArguments, arg)
	}
	return bulkStr.Value, nil
}

func parseStr(arg any) (string, error) {
	switch v := arg.(type) {
	case string:
		return v, nil
	case resp.BulkStr:
		return v.Value, nil
	default:
		return "", fmt.Errorf("%w: expected string or bulk string, got %T", ErrInvalidArguments, arg)
	}
}

func parseInt(arg any) (int, error) {
	var str string
	bulkStr, ok := arg.(resp.BulkStr)
	if !ok {
		str, ok = arg.(string)
		if !ok {
			return 0, fmt.Errorf("%w: expected integer, got %T", ErrInvalidArguments, arg)
		}
	} else {
		str = bulkStr.Value
	}
	val, err := strconv.Atoi(str)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid integer value", ErrInvalidArguments)
	}
	return val, nil
}

func parseFloat(arg any) (float64, error) {
	bulkStr, ok := arg.(resp.BulkStr)
	if !ok {
		return 0, fmt.Errorf("%w: expected integer, got %T", ErrInvalidArguments, arg)
	}
	val, err := strconv.ParseFloat(bulkStr.Value, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid float value", ErrInvalidArguments)
	}
	return val, nil
}

func (h *handlers) route(cmd *gedis_types.Command) (handler, bool, error) {
	r := cmd.Cmd
	entry, found := h.hmap[strings.ToLower(r.Cmd)]
	if !found {
		return nil, false, fmt.Errorf("%w: invalid command '%s'", resp.ErrProtocolError, r.Cmd)
	}
	return entry.handler, entry.shouldReplicate, nil
}

func (h *handlers) handlePing(cmd *gedis_types.Command) error {
	defer cmd.SetDone()
	if h.checkInTx(cmd) {
		return nil
	}
	cmd.WriteAny("PONG")
	return nil
}

func (h *handlers) handleEcho(cmd *gedis_types.Command) error {
	defer cmd.SetDone()
	if h.checkInTx(cmd) {
		return nil
	}
	for _, arg := range cmd.Cmd.Args {
		cmd.WriteAny(arg)
	}
	return nil
}

func (h *handlers) handleSelect(cmd *gedis_types.Command) error {
	if len(cmd.Cmd.Args) < 1 {
		return fmt.Errorf("%w: missing database number", ErrInvalidArguments)
	}
	dbn, err := parseInt(cmd.Cmd.Args[0])
	if err != nil {
		return err
	}
	defer cmd.SetDone()
	if h.checkInTx(cmd) {
		return nil
	}
	cmd.SelectDb(dbn)
	cmd.WriteAny("OK")
	return nil
}

func checkExpiry(args []any) (int, bool, error) {
	if len(args) == 0 {
		return -1, false, nil
	}
	if len(args) < 2 {
		return -1, false, fmt.Errorf("%w: missing TTL duration", ErrInvalidArguments)
	}

	typeStr, err := parseBulkStr(args[0])
	if err != nil {
		return -1, false, err
	}

	ttl, err := parseInt(args[1])
	if err != nil {
		return -1, false, err
	}

	var mod = 1
	switch strings.ToLower(typeStr) {
	case "ex":
		mod = 1000
	case "px":
		mod = 1
	default:
		return -1, false, fmt.Errorf("%w: unknown TTL modifier: %v", ErrInvalidArguments, args[0])
	}

	return ttl * mod, true, nil
}

func (h *handlers) handleSet(cmd *gedis_types.Command) error {
	args := cmd.Cmd.Args
	if len(args) < 2 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}

	if err := h.checkSlaveWrite(cmd); err != nil {
		return err
	}

	key, err := parseBulkStr(args[0])
	if err != nil {
		return err
	}
	value := args[1]
	ttl, _, err := checkExpiry(args[2:])
	if err != nil {
		return err
	}
	defer cmd.SetDone()
	if h.checkInTx(cmd) {
		return nil
	}

	h.db.HashMap().Set(key, value, ttl)
	if h.shouldWriteOutput(cmd) {
		cmd.WriteAny("OK")
	}
	return nil
}

func (h *handlers) handleGet(cmd *gedis_types.Command) error {
	args := cmd.Cmd.Args
	if len(args) < 1 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	if h.checkInTx(cmd) {
		return nil
	}
	key, err := parseBulkStr(args[0])
	if err != nil {
		return err
	}
	value, ok := h.db.HashMap().Get(key)
	if !ok {
		cmd.WriteAny(resp.BulkStr{Size: -1})
		return nil
	}
	cmd.WriteAny(value)
	return nil
}

func (h *handlers) handleRPush(cmd *gedis_types.Command) error {
	args := cmd.Cmd.Args
	if len(args) < 2 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}

	if err := h.checkSlaveWrite(cmd); err != nil {
		return err
	}

	defer cmd.SetDone()
	if h.checkInTx(cmd) {
		return nil
	}

	key, err := parseBulkStr(args[0])
	if err != nil {
		return err
	}
	list := h.db.GetOrCreateList(key)
	for _, value := range args[1:] {
		list.RightPush(value)
	}

	if h.shouldWriteOutput(cmd) {
		cmd.WriteAny(list.Len())
	}

	blkRequests, ok := h.db.block.blockLpop[key]
	if !ok {
		return nil
	}

	h.db.block.blockLpop[key] = slices.DeleteFunc(blkRequests, func(req *gedis_types.Command) bool {
		ok := h.resolveBlockLpop(key, req)
		if ok {
			log.Printf("resolved blpop request, listKey=%s", key)
		}
		return ok
	})
	return nil
}

func (h *handlers) handleLPush(cmd *gedis_types.Command) error {
	args := cmd.Cmd.Args
	if len(args) < 2 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}

	if err := h.checkSlaveWrite(cmd); err != nil {
		return err
	}

	defer cmd.SetDone()
	if h.checkInTx(cmd) {
		return nil
	}

	key, err := parseBulkStr(args[0])
	if err != nil {
		return err
	}
	list := h.db.GetOrCreateList(key)
	for _, value := range args[1:] {
		list.LeftPush(value)
	}

	if h.shouldWriteOutput(cmd) {
		cmd.WriteAny(list.Len())
	}

	blkRequests, ok := h.db.block.blockLpop[key]
	if !ok {
		return nil
	}

	h.db.block.blockLpop[key] = slices.DeleteFunc(blkRequests, func(req *gedis_types.Command) bool {
		ok := h.resolveBlockLpop(key, req)
		if ok {
			log.Printf("resolved blpop request, listKey=%s", key)
		}
		return ok
	})
	return nil
}

func (h *handlers) handleLPop(cmd *gedis_types.Command) error {
	args := cmd.Cmd.Args
	if len(args) < 1 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}

	if err := h.checkSlaveWrite(cmd); err != nil {
		return err
	}

	defer cmd.SetDone()
	if h.checkInTx(cmd) {
		return nil
	}

	key, err := parseBulkStr(args[0])
	if err != nil {
		return err
	}

	count := 1
	if len(args) >= 2 {
		var err error
		count, err = parseInt(args[1])
		if err != nil {
			return err
		}
	}

	list, exists := h.db.GetList(key)
	if !exists {
		if h.shouldWriteOutput(cmd) {
			cmd.WriteAny(resp.BulkStr{Size: -1})
		}
		return nil
	}

	if len(args) >= 2 {
		items := make([]any, 0, count)
		for i := 0; i < count; i += 1 {
			value, ok := list.LeftPop()
			if !ok {
				break
			}
			items = append(items, value)
		}
		if list.Len() == 0 {
			h.db.DeleteList(key)
		}
		if h.shouldWriteOutput(cmd) {
			cmd.WriteAny(resp.Array{Size: len(items), Items: items})
		}
		return nil
	}

	value, ok := list.LeftPop()
	if !ok {
		if h.shouldWriteOutput(cmd) {
			cmd.WriteAny(resp.BulkStr{Size: -1})
		}
		return nil
	}
	if list.Len() == 0 {
		h.db.DeleteList(key)
	}
	if h.shouldWriteOutput(cmd) {
		cmd.WriteAny(value)
	}
	return nil
}

func (h *handlers) handleRPop(cmd *gedis_types.Command) error {
	args := cmd.Cmd.Args
	if len(args) < 1 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}

	if err := h.checkSlaveWrite(cmd); err != nil {
		return err
	}

	defer cmd.SetDone()
	if h.checkInTx(cmd) {
		return nil
	}

	key, err := parseBulkStr(args[0])
	if err != nil {
		return err
	}

	count := 1
	if len(args) >= 2 {
		var err error
		count, err = parseInt(args[1])
		if err != nil {
			return err
		}
	}

	list, exists := h.db.GetList(key)
	if !exists {
		if h.shouldWriteOutput(cmd) {
			cmd.WriteAny(resp.BulkStr{Size: -1})
		}
		return nil
	}

	if len(args) >= 2 {
		items := make([]any, 0, count)
		for i := 0; i < count; i++ {
			value, ok := list.RightPop()
			if !ok {
				break
			}
			items = append(items, value)
		}
		if list.Len() == 0 {
			h.db.DeleteList(key)
		}
		if h.shouldWriteOutput(cmd) {
			cmd.WriteAny(resp.Array{Size: len(items), Items: items})
		}
		return nil
	}

	value, ok := list.RightPop()
	if !ok {
		if h.shouldWriteOutput(cmd) {
			cmd.WriteAny(resp.BulkStr{Size: -1})
		}
		return nil
	}
	if list.Len() == 0 {
		h.db.DeleteList(key)
	}
	if h.shouldWriteOutput(cmd) {
		cmd.WriteAny(value)
	}
	return nil
}

func (h *handlers) handleLRange(cmd *gedis_types.Command) error {
	args := cmd.Cmd.Args
	if len(args) < 3 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	if h.checkInTx(cmd) {
		return nil
	}
	key, err := parseBulkStr(args[0])
	if err != nil {
		return err
	}

	start, err := parseInt(args[1])
	if err != nil {
		return err
	}

	stop, err := parseInt(args[2])
	if err != nil {
		return err
	}

	list, exists := h.db.GetList(key)
	if !exists {
		cmd.WriteAny(resp.Array{Size: 0, Items: []any{}})
		return nil
	}

	items := list.LeftRange(start, stop)
	cmd.WriteAny(resp.Array{Size: len(items), Items: items})
	return nil
}

func (h *handlers) handleLLen(cmd *gedis_types.Command) error {
	args := cmd.Cmd.Args
	if len(args) < 1 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	if h.checkInTx(cmd) {
		return nil
	}
	key, err := parseBulkStr(args[0])
	if err != nil {
		return err
	}
	list, exists := h.db.GetList(key)
	if !exists {
		cmd.WriteAny(0)
		return nil
	}
	cmd.WriteAny(list.Len())
	return nil
}

func (h *handlers) handleLIndex(cmd *gedis_types.Command) error {
	args := cmd.Cmd.Args
	if len(args) < 2 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	if h.checkInTx(cmd) {
		return nil
	}
	key, err := parseBulkStr(args[0])
	if err != nil {
		return err
	}

	index, err := parseInt(args[1])
	if err != nil {
		return err
	}

	list, exists := h.db.GetList(key)
	if !exists {
		cmd.WriteAny(resp.BulkStr{Size: -1})
		return nil
	}

	value, ok := list.LeftIndex(index)
	if !ok {
		cmd.WriteAny(resp.BulkStr{Size: -1})
		return nil
	}
	cmd.WriteAny(value)
	return nil
}

func (h *handlers) handleBlockLpop(cmd *gedis_types.Command) error {
	args := cmd.Cmd.Args
	if len(args) < 2 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}

	if h.checkInTx(cmd) {
		return nil
	}

	key, err := parseBulkStr(args[0])
	if err != nil {
		return err
	}

	timeout, err := parseFloat(args[1])
	if err != nil {
		return err
	}

	if timeout != 0 {
		cmd.SetTimeout(time.Now().Add(time.Duration(timeout * float64(time.Second))))
		cmd.SetDefaultTimeoutOutput(resp.Array{Size: -1})
	}

	ok := h.resolveBlockLpop(key, cmd)
	if !ok {
		h.db.block.blockLpop[key] = append(h.db.block.blockLpop[key], cmd)
	}
	return nil
}

func (h *handlers) resolveBlockLpop(key string, cmd *gedis_types.Command) (ok bool) {
	if cmd.HasTimedOut() {
		return true
	}

	list, exists := h.db.GetList(key)
	if !exists {
		return false
	}

	if list.Len() == 0 {
		return false
	}

	defer cmd.SetDone()
	pdata, _ := list.LeftPop()
	cmd.WriteAny(resp.Array{Size: 2, Items: []any{key, pdata}})
	return true
}

func (h *handlers) handleIncr(cmd *gedis_types.Command) error {
	args := cmd.Cmd.Args
	if len(args) != 1 {
		log.Println(args)
		return fmt.Errorf("%w: requires exactly 1 argument", ErrInvalidArguments)
	}

	if err := h.checkSlaveWrite(cmd); err != nil {
		return err
	}

	key, err := parseBulkStr(args[0])
	if err != nil {
		return err
	}
	defer cmd.SetDone()

	if h.checkInTx(cmd) {
		return nil
	}

	val, ok := h.db.hm.Get(key)
	if !ok {
		h.db.HashMap().Set(key, resp.BulkStr{Size: 1, Value: "1"}, 0)
		if h.shouldWriteOutput(cmd) {
			cmd.WriteAny(1)
		}
		return nil
	}
	switch val.(type) {
	case resp.BulkStr, string:
		num, err := parseInt(val)
		if err != nil {
			return fmt.Errorf("value is not an integer or out of range")
		}
		num += 1
		numStr := fmt.Sprintf("%d", num)
		h.db.HashMap().Set(key, resp.BulkStr{Size: len(numStr), Value: numStr}, 0)
		if h.shouldWriteOutput(cmd) {
			cmd.WriteAny(num)
		}
	default:
		return fmt.Errorf("value is not an integer or out of range")
	}

	return nil
}

func (h *handlers) handleMulti(cmd *gedis_types.Command) error {

	if err := h.checkSlaveWrite(cmd); err != nil {
		return err
	}

	state := cmd.ConnState

	defer cmd.SetDone()
	if state.InTransaction {
		return fmt.Errorf("MULTI calls cannot be nested")
	}

	state.InTransaction = true
	if h.shouldWriteOutput(cmd) {
		cmd.WriteAny("OK")
	}
	return nil
}

func (h *handlers) handleExec(cmd *gedis_types.Command) error {

	if err := h.checkSlaveWrite(cmd); err != nil {
		return err
	}

	state := cmd.ConnState

	defer cmd.SetDone()
	if !state.InTransaction {
		return fmt.Errorf("EXEC without MULTI")
	}
	defer func() {
		state.Tx = make([]*gedis_types.Command, 0)
	}()

	bufs := make([]any, 0, len(state.Tx))
	state.InTransaction = false

	for _, op := range state.Tx {
		hdl, _, err := h.route(op)
		if err != nil {
			return err
		}
		err = hdl(op)
		if err != nil {
			cmd.SetDone()
			bufs = append(bufs, err)
		} else {
			bufs = append(bufs, op.Output())
		}
	}

	if h.shouldWriteOutput(cmd) {
		arr := resp.Array{Size: len(bufs), Items: bufs}
		cmd.WriteAny(arr)
	}
	return nil
}

func (h *handlers) checkInTx(cmd *gedis_types.Command) bool {
	state := cmd.ConnState
	if state.InTransaction {
		state.Tx = append(state.Tx, cmd.Copy())
		cmd.WriteAny("QUEUED")
		return true
	}
	return false
}

func (h *handlers) shouldWriteOutput(cmd *gedis_types.Command) bool {

	if h.isSlave {
		return !cmd.IsRepl()
	}

	return true
}

func (h *handlers) checkSlaveWrite(cmd *gedis_types.Command) error {
	if h.isSlave && !cmd.IsRepl() {
		return fmt.Errorf("READONLY You can't write against a read only replica")
	}
	return nil
}

func (h *handlers) handleDiscard(cmd *gedis_types.Command) error {

	if err := h.checkSlaveWrite(cmd); err != nil {
		return err
	}

	defer cmd.SetDone()

	state := cmd.ConnState

	if !state.InTransaction {
		return fmt.Errorf("DISCARD without MULTI")
	}

	if h.shouldWriteOutput(cmd) {
		cmd.WriteAny("OK")
	}

	state.InTransaction = false
	state.Tx = make([]*gedis_types.Command, 0)
	return nil
}

func (h *handlers) handleInfo(cmd *gedis_types.Command) error {
	defer cmd.SetDone()
	if h.checkInTx(cmd) {
		return fmt.Errorf("INFO not available during transaction")
	}
	args := cmd.Cmd.Args
	if len(args) > 0 {
		section, err := parseBulkStr(args[0])
		if err != nil {
			return err
		}
		fields := h.info.Fields()
		for _, field := range fields {
			if strings.EqualFold(section, field.Name) {
				str := fmt.Sprintf("# %s\n%v\n", field.Name, field.Value)
				bs := resp.BulkStr{Size: len(str), Value: str}
				cmd.WriteAny(bs)
				return nil
			}
		}
		return fmt.Errorf("unknown section '%s'", section)
	}

	str := h.info.String()
	bs := resp.BulkStr{Size: len(str), Value: str}
	cmd.WriteAny(bs)
	return nil
}

func (h *handlers) handleReplConf(cmd *gedis_types.Command) error {
	defer cmd.SetDone()
	if h.checkInTx(cmd) {
		return nil
	}
	args := cmd.Cmd.Args
	if len(args) < 2 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	subcmd, err := parseStr(args[0])
	if err != nil {
		return err
	}

	state := cmd.ConnState

	switch strings.ToLower(subcmd) {
	case "listening-port":
		portnum, err := parseInt(args[1])
		if err != nil {
			return fmt.Errorf("%w: invalid port number: %w", ErrInvalidArguments, err)
		}
		err = h.master.AddSlave(state.Conn, portnum)
		if err != nil {
			return err
		}
	case "capa":
		proto, err := parseStr(args[1])
		if err != nil {
			return fmt.Errorf("%w: invalid capability: %w", ErrInvalidArguments, err)
		}
		exists := h.master.SetSlaveProto(state.Conn, proto)
		if !exists {
			return fmt.Errorf("%w: slave not registered yet", ErrInvalidArguments)
		}
	default:
		return fmt.Errorf("%w: unknown REPLCONF subcommand '%s'", ErrInvalidArguments, subcmd)
	}
	cmd.WriteAny("OK")
	return nil
}

func (h *handlers) handlePsync(cmd *gedis_types.Command) error {
	defer cmd.SetDone()

	if h.checkInTx(cmd) {
		return fmt.Errorf("PSYNC cannot be in a transaction")
	}
	if h.isSlave {
		return fmt.Errorf("PSYNC invalid for slave")
	}
	str := fmt.Sprintf("FULLRESYNC %s %d", h.master.ReplId(), h.master.ReplOffset())
	sarr := resp.BulkStr{Size: len(str), Value: str}
	cmd.WriteAny(sarr)

	state := cmd.ConnState

	cmd.SetDefer(func() {
		_, found := h.master.GetSlave(state.Conn.RemoteAddr().String())
		if found {
			h.resolveInitRdbSync(state)
		}
	})

	return nil
}

func (h *handlers) resolveInitRdbSync(state *gedis_types.ConnState) {
	addr := state.Conn.RemoteAddr().String()
	err := h.master.InitialRdbSync(addr)
	if err != nil {
		log.Printf("rdb sync failed, addr=%s, err=%s", addr, err)
	}
	log.Printf("master sync RDB to replica, addr=%s", addr)
}
