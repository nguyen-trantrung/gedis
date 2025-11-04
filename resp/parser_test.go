package resp_test

import (
	"strings"
	"testing"

	"github.com/ttn-nguyen42/gedis/resp"
)

func TestParseTokens(t *testing.T) {
	tests := []struct {
		name     string
		tokens   []resp.Token
		expected []resp.Command
		hasError bool
	}{
		{
			name: "single array command",
			tokens: []resp.Token{
				{Type: resp.TokenTypeArray, Value: 2, Literal: "*2", Size: 2},
				{Type: resp.TokenTypeBulkString, Value: 4, Literal: "$4", Size: 2},
				{Type: resp.TokenTypeValue, Value: "LLEN", Literal: "LLEN", Size: 4},
				{Type: resp.TokenTypeBulkString, Value: 6, Literal: "$6", Size: 2},
				{Type: resp.TokenTypeValue, Value: "mylist", Literal: "mylist", Size: 6},
			},
			expected: []resp.Command{
				{Cmd: "LLEN", Args: []any{"mylist"}},
			},
			hasError: false,
		},
		{
			name: "inline command",
			tokens: []resp.Token{
				{Type: resp.TokenTypeValue, Value: "PING", Literal: "PING", Size: 4},
			},
			expected: []resp.Command{
				{Cmd: "PING", Args: []any{}},
			},
			hasError: false,
		},
		{
			name: "multiple commands",
			tokens: []resp.Token{
				{Type: resp.TokenTypeValue, Value: "PING", Literal: "PING", Size: 4},
				{Type: resp.TokenTypeArray, Value: 2, Literal: "*2", Size: 2},
				{Type: resp.TokenTypeBulkString, Value: 6, Literal: "$6", Size: 2},
				{Type: resp.TokenTypeValue, Value: "EXISTS", Literal: "EXISTS", Size: 6},
				{Type: resp.TokenTypeBulkString, Value: 5, Literal: "$5", Size: 2},
				{Type: resp.TokenTypeValue, Value: "mykey", Literal: "mykey", Size: 5},
			},
			expected: []resp.Command{
				{Cmd: "PING", Args: []any{}},
				{Cmd: "EXISTS", Args: []any{"mykey"}},
			},
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmds, err := resp.ParseTokens(tt.tokens)
			if tt.hasError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseTokens() error = %v", err)
			}
			if len(cmds) != len(tt.expected) {
				t.Fatalf("expected %d commands, got %d", len(tt.expected), len(cmds))
			}
			for i, cmd := range cmds {
				exp := tt.expected[i]
				if cmd.Cmd != exp.Cmd {
					t.Errorf("command %d cmd: expected %q, got %q", i, exp.Cmd, cmd.Cmd)
				}
				if len(cmd.Args) != len(exp.Args) {
					t.Errorf("command %d args length: expected %d, got %d", i, len(exp.Args), len(cmd.Args))
				}
				for j, arg := range cmd.Args {
					if arg != exp.Args[j] {
						t.Errorf("command %d arg %d: expected %v, got %v", i, j, exp.Args[j], arg)
					}
				}
			}
		})
	}
}

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
				Args: []any{"mylist"},
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
