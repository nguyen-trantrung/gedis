package resp

import (
	"fmt"
)

type Command struct {
	Cmd  string
	Args []any
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
