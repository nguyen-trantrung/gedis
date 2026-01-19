package resp_test

import (
	"strings"
	"testing"

	"github.com/ttn-nguyen42/gedis/resp"
)

func TestParseStream(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected resp.Command
		hasError bool
	}{
		{
			name:  "array command",
			input: "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n",
			expected: resp.Command{
				Cmd:  "LLEN",
				Args: []any{resp.BulkStr{Size: 6, Value: "mylist"}},
			},
			hasError: false,
		},
		{
			name:  "inline command",
			input: "PING\r\n",
			expected: resp.Command{
				Cmd:  "PING",
				Args: []any{},
			},
			hasError: false,
		},
		{
			name:  "inline command with args",
			input: "EXISTS somekey\r\n",
			expected: resp.Command{
				Cmd:  "EXISTS",
				Args: []any{"somekey"},
			},
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.input)
			cmd, err := resp.ParseCmd(reader)
			if tt.hasError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseStream() error = %v", err)
			}
			if cmd.Cmd != tt.expected.Cmd {
				t.Errorf("cmd: expected %q, got %q", tt.expected.Cmd, cmd.Cmd)
			}
			if len(cmd.Args) != len(tt.expected.Args) {
				t.Errorf("args length: expected %d, got %d", len(tt.expected.Args), len(cmd.Args))
			}
			for i, arg := range cmd.Args {
				if arg != tt.expected.Args[i] {
					t.Errorf("arg %d: expected %v, got %v", i, tt.expected.Args[i], arg)
				}
			}
		})
	}
}
