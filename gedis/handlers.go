package gedis

import (
	"fmt"
	"log"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ttn-nguyen42/gedis/gedis/info"
	"github.com/ttn-nguyen42/gedis/resp"
)

var ErrInvalidArguments error = fmt.Errorf("invalid arguments")

type handler func(conn *ConnState, cmd *Command) error

type handlers struct {
	isSlave bool
	db      *database
	info    *info.Info
	hmap    map[string]handler
}

func newHandlers(db *database, info *info.Info, isSlave bool) *handlers {
	hdl := &handlers{
		db:      db,
		info:    info,
		hmap:    nil,
		isSlave: isSlave,
	}
	hdl.init()
	return hdl
}

func (h *handlers) init() {
	h.hmap = map[string]handler{
		"ping":     h.handlePing,
		"echo":     h.handleEcho,
		"select":   h.handleSelect,
		"set":      h.handleSet,
		"get":      h.handleGet,
		"rpush":    h.handleRPush,
		"lpush":    h.handleLPush,
		"lpop":     h.handleLPop,
		"rpop":     h.handleRPop,
		"lrange":   h.handleLRange,
		"llen":     h.handleLLen,
		"lindex":   h.handleLIndex,
		"blpop":    h.handleBlockLpop,
		"incr":     h.handleIncr,
		"multi":    h.handleMulti,
		"exec":     h.handleExec,
		"discard":  h.handleDiscard,
		"info":     h.handleInfo,
		"replconf": h.handleReplConf,
		"psync":    h.handlePsync,
	}
}

func parseBulkStr(arg any) (string, error) {
	bulkStr, ok := arg.(resp.BulkStr)
	if !ok {
		return "", fmt.Errorf("%w: expected bulk string, got %T", ErrInvalidArguments, arg)
	}
	return bulkStr.Value, nil
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

func (h *handlers) route(cmd *Command) (handler, error) {
	r := cmd.Cmd
	hdlr, found := h.hmap[strings.ToLower(r.Cmd)]
	if !found {
		return nil, fmt.Errorf("%w: invalid command '%s'", resp.ErrProtocolError, r.Cmd)
	}
	return hdlr, nil
}

func (h *handlers) handlePing(conn *ConnState, cmd *Command) error {
	defer cmd.SetDone()
	if h.checkInTx(conn, cmd) {
		return nil
	}
	cmd.WriteAny("PONG")
	return nil
}

func (h *handlers) handleEcho(conn *ConnState, cmd *Command) error {
	defer cmd.SetDone()
	if h.checkInTx(conn, cmd) {
		return nil
	}
	for _, arg := range cmd.Cmd.Args {
		cmd.WriteAny(arg)
	}
	return nil
}

func (h *handlers) handleSelect(conn *ConnState, cmd *Command) error {
	if len(cmd.Cmd.Args) < 1 {
		return fmt.Errorf("%w: missing database number", ErrInvalidArguments)
	}
	dbn, err := parseInt(cmd.Cmd.Args[0])
	if err != nil {
		return err
	}
	defer cmd.SetDone()
	if h.checkInTx(conn, cmd) {
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

func (h *handlers) handleSet(conn *ConnState, cmd *Command) error {
	args := cmd.Cmd.Args
	if len(args) < 2 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
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
	if h.checkInTx(conn, cmd) {
		return nil
	}
	h.db.HashMap().Set(key, value, ttl)
	cmd.WriteAny("OK")
	return nil
}

func (h *handlers) handleGet(conn *ConnState, cmd *Command) error {
	args := cmd.Cmd.Args
	if len(args) < 1 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	if h.checkInTx(conn, cmd) {
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

func (h *handlers) handleRPush(conn *ConnState, cmd *Command) error {
	args := cmd.Cmd.Args
	if len(args) < 2 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	if h.checkInTx(conn, cmd) {
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
	cmd.WriteAny(list.Len())

	blkRequests, ok := h.db.block.blockLpop[key]
	if !ok {
		return nil
	}

	h.db.block.blockLpop[key] = slices.DeleteFunc(blkRequests, func(req *Command) bool {
		ok := h.resolveBlockLpop(key, req)
		if ok {
			log.Printf("resolved blpop request, listKey=%s", key)
		}
		return ok
	})
	return nil
}

func (h *handlers) handleLPush(conn *ConnState, cmd *Command) error {
	args := cmd.Cmd.Args
	if len(args) < 2 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	if h.checkInTx(conn, cmd) {
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
	cmd.WriteAny(list.Len())

	blkRequests, ok := h.db.block.blockLpop[key]
	if !ok {
		return nil
	}

	h.db.block.blockLpop[key] = slices.DeleteFunc(blkRequests, func(req *Command) bool {
		ok := h.resolveBlockLpop(key, req)
		if ok {
			log.Printf("resolved blpop request, listKey=%s", key)
		}
		return ok
	})
	return nil
}

func (h *handlers) handleLPop(conn *ConnState, cmd *Command) error {
	args := cmd.Cmd.Args
	if len(args) < 1 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	if h.checkInTx(conn, cmd) {
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
		cmd.WriteAny(resp.BulkStr{Size: -1})
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
		cmd.WriteAny(resp.Array{Size: len(items), Items: items})
		return nil
	}

	value, ok := list.LeftPop()
	if !ok {
		cmd.WriteAny(resp.BulkStr{Size: -1})
		return nil
	}
	if list.Len() == 0 {
		h.db.DeleteList(key)
	}
	cmd.WriteAny(value)
	return nil
}

func (h *handlers) handleRPop(conn *ConnState, cmd *Command) error {
	args := cmd.Cmd.Args
	if len(args) < 1 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	if h.checkInTx(conn, cmd) {
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
		cmd.WriteAny(resp.BulkStr{Size: -1})
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
		cmd.WriteAny(resp.Array{Size: len(items), Items: items})
		return nil
	}

	value, ok := list.RightPop()
	if !ok {
		cmd.WriteAny(resp.BulkStr{Size: -1})
		return nil
	}
	if list.Len() == 0 {
		h.db.DeleteList(key)
	}
	cmd.WriteAny(value)
	return nil
}

func (h *handlers) handleLRange(conn *ConnState, cmd *Command) error {
	args := cmd.Cmd.Args
	if len(args) < 3 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	if h.checkInTx(conn, cmd) {
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

func (h *handlers) handleLLen(conn *ConnState, cmd *Command) error {
	args := cmd.Cmd.Args
	if len(args) < 1 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	if h.checkInTx(conn, cmd) {
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

func (h *handlers) handleLIndex(conn *ConnState, cmd *Command) error {
	args := cmd.Cmd.Args
	if len(args) < 2 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	if h.checkInTx(conn, cmd) {
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

func (h *handlers) handleBlockLpop(conn *ConnState, cmd *Command) error {
	args := cmd.Cmd.Args
	if len(args) < 2 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}

	if h.checkInTx(conn, cmd) {
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

func (h *handlers) resolveBlockLpop(key string, cmd *Command) (ok bool) {
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

func (h *handlers) handleIncr(conn *ConnState, cmd *Command) error {
	args := cmd.Cmd.Args
	if len(args) != 1 {
		log.Println(args)
		return fmt.Errorf("%w: requires exactly 1 argument", ErrInvalidArguments)
	}
	key, err := parseBulkStr(args[0])
	if err != nil {
		return err
	}
	defer cmd.SetDone()

	if h.checkInTx(conn, cmd) {
		return nil
	}

	val, ok := h.db.hm.Get(key)
	if !ok {
		h.db.HashMap().Set(key, resp.BulkStr{Size: 1, Value: "1"}, 0)
		cmd.WriteAny(1)
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
		cmd.WriteAny(num)
	default:
		return fmt.Errorf("value is not an integer or out of range")
	}

	return nil
}

func (h *handlers) handleMulti(conn *ConnState, cmd *Command) error {
	defer cmd.SetDone()
	if conn.InTransaction {
		return fmt.Errorf("MULTI calls cannot be nested")
	}
	conn.InTransaction = true
	cmd.WriteAny("OK")
	return nil
}

func (h *handlers) handleExec(conn *ConnState, cmd *Command) error {
	defer cmd.SetDone()
	if !conn.InTransaction {
		return fmt.Errorf("EXEC without MULTI")
	}
	defer func() {
		conn.Tx = make([]*Command, 0)
	}()

	bufs := make([]any, 0, len(conn.Tx))
	conn.InTransaction = false

	for _, op := range conn.Tx {
		hdl, err := h.route(op)
		if err != nil {
			return err
		}
		err = hdl(conn, op)
		if err != nil {
			cmd.SetDone()
			bufs = append(bufs, err)
		} else {
			bufs = append(bufs, op.out)
		}
	}

	arr := resp.Array{Size: len(bufs), Items: bufs}

	cmd.WriteAny(arr)
	return nil
}

func (h *handlers) checkInTx(conn *ConnState, cmd *Command) bool {
	if conn.InTransaction {
		conn.Tx = append(conn.Tx, cmd.Copy())
		cmd.WriteAny("QUEUED")
		return true
	}
	return false
}

func (h *handlers) handleDiscard(conn *ConnState, cmd *Command) error {
	defer cmd.SetDone()

	if !conn.InTransaction {
		return fmt.Errorf("DISCARD without MULTI")
	}
	cmd.WriteAny("OK")

	conn.InTransaction = false
	conn.Tx = make([]*Command, 0)
	return nil
}

func (h *handlers) handleInfo(conn *ConnState, cmd *Command) error {
	defer cmd.SetDone()
	if h.checkInTx(conn, cmd) {
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

func (h *handlers) handleReplConf(conn *ConnState, cmd *Command) error {
	defer cmd.SetDone()
	if h.checkInTx(conn, cmd) {
		return nil
	}
	cmd.WriteAny("OK")
	return nil
}

func (h *handlers) handlePsync(conn *ConnState, cmd *Command) error {
	defer cmd.SetDone()
	if h.checkInTx(conn, cmd) {
		return fmt.Errorf("PSYNC cannot be in a transaction")
	}
	if h.isSlave {
		return fmt.Errorf("PSYNC invalid for slave")
	}
	cmd.WriteAny("OK")
	return nil
}
