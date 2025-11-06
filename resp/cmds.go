package resp

import (
	"fmt"
)

type Command struct {
	Cmd  string
	Args []any
	Size int // Number of bytes read from the stream for this command
}

func NewCommand(arr Array) (Command, error) {
	return newCommand(arr.Items)
}

func newCommand(parts []any) (Command, error) {
	if len(parts) == 0 {
		return Command{}, nil
	}
	cmdStr, err := bulkOrStr(parts[0])
	if err != nil {
		return Command{}, err
	}
	return Command{
		Cmd:  cmdStr,
		Args: parts[1:],
	}, nil
}

func bulkOrStr(str any) (string, error) {
	switch val := str.(type) {
	case string:
		return val, nil
	case BulkStr:
		return val.Value, nil
	default:
		return "", fmt.Errorf("unknown string: %+v", str)
	}
}

func toBulkStr(arg any) (BulkStr, error) {
	switch val := arg.(type) {
	case string:
		return BulkStr{Size: len(val), Value: val}, nil
	case BulkStr:
		return val, nil
	default:
		return BulkStr{}, fmt.Errorf("unknown string: %+v", arg)
	}
}

func (c *Command) Array() Array {
	arr := Array{
		Size:  1 + len(c.Args),
		Items: make([]any, 0, 1+len(c.Args)),
	}
	arr.Items = append(arr.Items, BulkStr{Size: len(c.Cmd), Value: c.Cmd})
	for _, arg := range c.Args {
		str, err := toBulkStr(arg)
		if err != nil {
			continue
		}
		arr.Items = append(arr.Items, str)
	}
	return arr
}
