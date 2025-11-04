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

type Handler func(db *database, cmd *Command, conn *ConnState, info *info.Info) error

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

var hmap map[string]Handler

func init() {
	hmap = map[string]Handler{
		"ping":    handlePing,
		"echo":    handleEcho,
		"select":  handleSelect,
		"set":     handleSet,
		"get":     handleGet,
		"rpush":   handleRPush,
		"lpush":   handleLPush,
		"lpop":    handleLPop,
		"rpop":    handleRPop,
		"lrange":  handleLRange,
		"llen":    handleLLen,
		"lindex":  handleLIndex,
		"blpop":   handleBlockLpop,
		"incr":    handleIncr,
		"multi":   handleMulti,
		"exec":    handleExec,
		"discard": handleDiscard,
		"info":    handleInfo,
	}
}

func selectHandler(cmd *Command) (Handler, error) {
	r := cmd.Cmd
	hdlr, found := hmap[strings.ToLower(r.Cmd)]
	if !found {
		return nil, fmt.Errorf("%w: invalid command '%s'", resp.ErrProtocolError, r.Cmd)
	}
	return hdlr, nil
}

func handlePing(db *database, cmd *Command, conn *ConnState, info *info.Info) error {
	defer cmd.SetDone()
	if checkInTx(cmd, conn) {
		return nil
	}
	cmd.WriteAny("PONG")
	return nil
}

func handleEcho(db *database, cmd *Command, conn *ConnState, info *info.Info) error {
	defer cmd.SetDone()
	if checkInTx(cmd, conn) {
		return nil
	}
	for _, arg := range cmd.Cmd.Args {
		cmd.WriteAny(arg)
	}
	return nil
}

func handleSelect(db *database, cmd *Command, conn *ConnState, info *info.Info) error {
	if len(cmd.Cmd.Args) < 1 {
		return fmt.Errorf("%w: missing database number", ErrInvalidArguments)
	}
	dbn, err := parseInt(cmd.Cmd.Args[0])
	if err != nil {
		return err
	}
	defer cmd.SetDone()
	if checkInTx(cmd, conn) {
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

func handleSet(db *database, cmd *Command, conn *ConnState, info *info.Info) error {
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
	if checkInTx(cmd, conn) {
		return nil
	}
	db.HashMap().Set(key, value, ttl)
	cmd.WriteAny("OK")
	return nil
}

func handleGet(db *database, cmd *Command, conn *ConnState, info *info.Info) error {
	args := cmd.Cmd.Args
	if len(args) < 1 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	if checkInTx(cmd, conn) {
		return nil
	}
	key, err := parseBulkStr(args[0])
	if err != nil {
		return err
	}
	value, ok := db.HashMap().Get(key)
	if !ok {
		cmd.WriteAny(resp.BulkStr{Size: -1})
		return nil
	}
	cmd.WriteAny(value)
	return nil
}

func handleRPush(db *database, cmd *Command, conn *ConnState, info *info.Info) error {
	args := cmd.Cmd.Args
	if len(args) < 2 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	if checkInTx(cmd, conn) {
		return nil
	}
	key, err := parseBulkStr(args[0])
	if err != nil {
		return err
	}
	list := db.GetOrCreateList(key)
	for _, value := range args[1:] {
		list.RightPush(value)
	}
	cmd.WriteAny(list.Len())

	blkRequests, ok := db.block.blockLpop[key]
	if !ok {
		return nil
	}

	db.block.blockLpop[key] = slices.DeleteFunc(blkRequests, func(req *Command) bool {
		ok := resolveBlockLpop(db, key, req)
		if ok {
			log.Printf("resolved blpop request, listKey=%s", key)
		}
		return ok
	})
	return nil
}

func handleLPush(db *database, cmd *Command, conn *ConnState, info *info.Info) error {
	args := cmd.Cmd.Args
	if len(args) < 2 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	if checkInTx(cmd, conn) {
		return nil
	}
	key, err := parseBulkStr(args[0])
	if err != nil {
		return err
	}
	list := db.GetOrCreateList(key)
	for _, value := range args[1:] {
		list.LeftPush(value)
	}
	cmd.WriteAny(list.Len())

	blkRequests, ok := db.block.blockLpop[key]
	if !ok {
		return nil
	}

	db.block.blockLpop[key] = slices.DeleteFunc(blkRequests, func(req *Command) bool {
		ok := resolveBlockLpop(db, key, req)
		if ok {
			log.Printf("resolved blpop request, listKey=%s", key)
		}
		return ok
	})
	return nil
}

func handleLPop(db *database, cmd *Command, conn *ConnState, info *info.Info) error {
	args := cmd.Cmd.Args
	if len(args) < 1 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	if checkInTx(cmd, conn) {
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

	list, exists := db.GetList(key)
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
			db.DeleteList(key)
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
		db.DeleteList(key)
	}
	cmd.WriteAny(value)
	return nil
}

func handleRPop(db *database, cmd *Command, conn *ConnState, info *info.Info) error {
	args := cmd.Cmd.Args
	if len(args) < 1 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	if checkInTx(cmd, conn) {
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

	list, exists := db.GetList(key)
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
			db.DeleteList(key)
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
		db.DeleteList(key)
	}
	cmd.WriteAny(value)
	return nil
}

func handleLRange(db *database, cmd *Command, conn *ConnState, info *info.Info) error {
	args := cmd.Cmd.Args
	if len(args) < 3 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	if checkInTx(cmd, conn) {
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

	list, exists := db.GetList(key)
	if !exists {
		cmd.WriteAny(resp.Array{Size: 0, Items: []any{}})
		return nil
	}

	items := list.LeftRange(start, stop)
	cmd.WriteAny(resp.Array{Size: len(items), Items: items})
	return nil
}

func handleLLen(db *database, cmd *Command, conn *ConnState, info *info.Info) error {
	args := cmd.Cmd.Args
	if len(args) < 1 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	if checkInTx(cmd, conn) {
		return nil
	}
	key, err := parseBulkStr(args[0])
	if err != nil {
		return err
	}
	list, exists := db.GetList(key)
	if !exists {
		cmd.WriteAny(0)
		return nil
	}
	cmd.WriteAny(list.Len())
	return nil
}

func handleLIndex(db *database, cmd *Command, conn *ConnState, info *info.Info) error {
	args := cmd.Cmd.Args
	if len(args) < 2 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	if checkInTx(cmd, conn) {
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

	list, exists := db.GetList(key)
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

func handleBlockLpop(db *database, cmd *Command, conn *ConnState, info *info.Info) error {
	args := cmd.Cmd.Args
	if len(args) < 2 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}

	if checkInTx(cmd, conn) {
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

	ok := resolveBlockLpop(db, key, cmd)
	if !ok {
		db.block.blockLpop[key] = append(db.block.blockLpop[key], cmd)
	}
	return nil
}

func resolveBlockLpop(db *database, key string, cmd *Command) (ok bool) {
	list, exists := db.GetList(key)
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

func handleIncr(db *database, cmd *Command, conn *ConnState, info *info.Info) error {
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

	if checkInTx(cmd, conn) {
		return nil
	}

	val, ok := db.hm.Get(key)
	if !ok {
		db.HashMap().Set(key, resp.BulkStr{Size: 1, Value: "1"}, 0)
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
		db.HashMap().Set(key, resp.BulkStr{Size: len(numStr), Value: numStr}, 0)
		cmd.WriteAny(num)
	default:
		return fmt.Errorf("value is not an integer or out of range")
	}

	return nil
}

func handleMulti(db *database, cmd *Command, conn *ConnState, info *info.Info) error {
	defer cmd.SetDone()
	if conn.InTransaction {
		return fmt.Errorf("MULTI calls cannot be nested")
	}
	conn.InTransaction = true
	cmd.WriteAny("OK")
	return nil
}

func handleExec(db *database, cmd *Command, conn *ConnState, info *info.Info) error {
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
		hdl, err := selectHandler(op)
		if err != nil {
			return err
		}
		err = hdl(db, op, conn, info)
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

func checkInTx(cmd *Command, conn *ConnState) bool {
	if conn.InTransaction {
		conn.Tx = append(conn.Tx, cmd.Copy())
		cmd.WriteAny("QUEUED")
		return true
	}
	return false
}

func handleDiscard(db *database, cmd *Command, conn *ConnState, info *info.Info) error {
	defer cmd.SetDone()

	if !conn.InTransaction {
		return fmt.Errorf("DISCARD without MULTI")
	}
	cmd.WriteAny("OK")

	conn.InTransaction = false
	conn.Tx = make([]*Command, 0)
	return nil
}

func handleInfo(db *database, cmd *Command, conn *ConnState, info *info.Info) error {
	defer cmd.SetDone()
	if checkInTx(cmd, conn) {
		return fmt.Errorf("INFO not available during transaction")
	}
	args := cmd.Cmd.Args
	if len(args) > 0 {
		section, err := parseBulkStr(args[0])
		if err != nil {
			return err
		}
		fields := info.Fields()
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

	str := info.String()
	bs := resp.BulkStr{Size: len(str), Value: str}
	cmd.WriteAny(bs)
	return nil
}
