package gedis

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ttn-nguyen42/gedis/resp"
)

var ErrInvalidArguments error = fmt.Errorf("invalid arguments")

type Handler func(db *database, cmd *Command) error

var hmap = map[string]Handler{
	"ping":   handlePing,
	"echo":   handleEcho,
	"select": handleSelect,
	"set":    handleSet,
	"get":    handleGet,
	"rpush":  handleRPush,
	"lpush":  handleLPush,
	"lpop":   handleLPop,
	"rpop":   handleRPop,
	"lrange": handleLRange,
	"llen":   handleLLen,
	"lindex": handleLIndex,
}

func selectHandler(cmd *Command) (Handler, error) {
	r := cmd.Cmd
	hdlr, found := hmap[strings.ToLower(r.Cmd)]
	if !found {
		return nil, fmt.Errorf("%w: invalid command '%s'", resp.ErrProtocolError, r.Cmd)
	}
	return hdlr, nil
}

func handlePing(db *database, cmd *Command) error {
	defer cmd.SetDone()
	cmd.WriteAny("PONG")
	return nil
}

func handleEcho(db *database, cmd *Command) error {
	defer cmd.SetDone()
	for _, arg := range cmd.Cmd.Args {
		cmd.WriteAny(arg)
	}
	return nil
}

func handleSelect(db *database, cmd *Command) error {
	if len(cmd.Cmd.Args) < 1 {
		return fmt.Errorf("%w: missing database number", ErrInvalidArguments)
	}
	dbstr, isBulkStr := cmd.Cmd.Args[0].(resp.BulkStr)
	if !isBulkStr {
		return fmt.Errorf("%w: database number must be integer", ErrInvalidArguments)
	}
	dbn, err := strconv.Atoi(dbstr.Value)
	if err != nil {
		return fmt.Errorf("%w: database number must be integer", ErrInvalidArguments)
	}
	defer cmd.SetDone()
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

	typeStr, ok := args[0].(resp.BulkStr)
	if !ok {
		return -1, false, fmt.Errorf("%w: TTL modifier must be string: %v", ErrInvalidArguments, args[0])
	}

	ttlStr, ok := args[1].(resp.BulkStr)
	if !ok {
		return -1, false, fmt.Errorf("%w: TTL must be integer: %v", ErrInvalidArguments, args[1])
	}

	ttl, err := strconv.Atoi(ttlStr.Value)
	if err != nil {
		return -1, false, fmt.Errorf("%w: TTL must be integer: %v", ErrInvalidArguments, ttlStr)
	}
	var mod = 1
	switch strings.ToLower(typeStr.Value) {
	case "ex":
		mod = 1000
	case "px":
		mod = 1
	default:
		return -1, false, fmt.Errorf("%w: unknown TTL modifier: %v", ErrInvalidArguments, args[0])
	}

	return ttl * mod, true, nil
}

func handleSet(db *database, cmd *Command) error {
	args := cmd.Cmd.Args
	if len(args) < 2 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	key := args[0]
	value := args[1]
	ttl, _, err := checkExpiry(args[2:])
	if err != nil {
		return err
	}
	defer cmd.SetDone()
	db.HashMap().Set(key, value, ttl)
	cmd.WriteAny("OK")
	return nil
}

func handleGet(db *database, cmd *Command) error {
	args := cmd.Cmd.Args
	if len(args) < 1 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	key := args[0]
	value, ok := db.HashMap().Get(key)
	if !ok {
		cmd.WriteAny(resp.BulkStr{Size: -1})
		return nil
	}
	cmd.WriteAny(value)
	return nil
}

func handleRPush(db *database, cmd *Command) error {
	args := cmd.Cmd.Args
	if len(args) < 2 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	key := args[0]
	list := db.GetOrCreateList(key)
	for _, value := range args[1:] {
		list.RightPush(value)
	}
	cmd.WriteAny(list.Len())
	return nil
}

func handleLPush(db *database, cmd *Command) error {
	args := cmd.Cmd.Args
	if len(args) < 2 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	key := args[0]
	list := db.GetOrCreateList(key)
	for _, value := range args[1:] {
		list.LeftPush(value)
	}
	cmd.WriteAny(list.Len())
	return nil
}

func handleLPop(db *database, cmd *Command) error {
	args := cmd.Cmd.Args
	if len(args) < 1 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	key := args[0]

	count := 1
	if len(args) >= 2 {
		countStr, ok := args[1].(resp.BulkStr)
		if !ok {
			return fmt.Errorf("%w: count must be integer", ErrInvalidArguments)
		}
		var err error
		count, err = strconv.Atoi(countStr.Value)
		if err != nil {
			return fmt.Errorf("%w: count must be integer", ErrInvalidArguments)
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

func handleRPop(db *database, cmd *Command) error {
	args := cmd.Cmd.Args
	if len(args) < 1 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	key := args[0]

	count := 1
	if len(args) >= 2 {
		countStr, ok := args[1].(resp.BulkStr)
		if !ok {
			return fmt.Errorf("%w: count must be integer", ErrInvalidArguments)
		}
		var err error
		count, err = strconv.Atoi(countStr.Value)
		if err != nil {
			return fmt.Errorf("%w: count must be integer", ErrInvalidArguments)
		}
	}

	list, exists := db.GetList(key)
	if !exists {
		cmd.WriteAny(resp.BulkStr{Size: -1})
		return nil
	}

	// If count is specified, return array
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

	// Single element, return bulk string
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

func handleLRange(db *database, cmd *Command) error {
	args := cmd.Cmd.Args
	if len(args) < 3 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	key := args[0]

	startStr, ok := args[1].(resp.BulkStr)
	if !ok {
		return fmt.Errorf("%w: start must be integer", ErrInvalidArguments)
	}
	start, err := strconv.Atoi(startStr.Value)
	if err != nil {
		return fmt.Errorf("%w: start must be integer", ErrInvalidArguments)
	}

	stopStr, ok := args[2].(resp.BulkStr)
	if !ok {
		return fmt.Errorf("%w: stop must be integer", ErrInvalidArguments)
	}
	stop, err := strconv.Atoi(stopStr.Value)
	if err != nil {
		return fmt.Errorf("%w: stop must be integer", ErrInvalidArguments)
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

func handleLLen(db *database, cmd *Command) error {
	args := cmd.Cmd.Args
	if len(args) < 1 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	key := args[0]
	list, exists := db.GetList(key)
	if !exists {
		cmd.WriteAny(0)
		return nil
	}
	cmd.WriteAny(list.Len())
	return nil
}

func handleLIndex(db *database, cmd *Command) error {
	args := cmd.Cmd.Args
	if len(args) < 2 {
		return fmt.Errorf("%w: not enough arguments", ErrInvalidArguments)
	}
	defer cmd.SetDone()
	key := args[0]

	indexStr, ok := args[1].(resp.BulkStr)
	if !ok {
		return fmt.Errorf("%w: index must be integer", ErrInvalidArguments)
	}
	index, err := strconv.Atoi(indexStr.Value)
	if err != nil {
		return fmt.Errorf("%w: index must be integer", ErrInvalidArguments)
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
