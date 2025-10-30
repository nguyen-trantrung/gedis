package resp

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
	cmdStr, ok := parts[0].(string)
	if !ok {
		return Command{}, nil
	}
	return Command{
		Cmd:  cmdStr,
		Args: parts[1:],
	}, nil
}
