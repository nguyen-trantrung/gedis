package resp_test

import (
	"strings"
	"testing"

	"github.com/ttn-nguyen42/gedis/resp"
)

func TestTokens(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []resp.Token
	}{
		{
			name:  "RESP array and bulk strings",
			input: "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n",
			expected: []resp.Token{
				{Type: resp.TokenTypeArray, Value: 2, Literal: "*2", Size: 2},
				{Type: resp.TokenTypeBulkString, Value: 4, Literal: "$4", Size: 2},
				{Type: resp.TokenTypeValue, Value: "LLEN", Literal: "LLEN", Size: 4},
				{Type: resp.TokenTypeBulkString, Value: 6, Literal: "$6", Size: 2},
				{Type: resp.TokenTypeValue, Value: "mylist", Literal: "mylist", Size: 6},
			},
		},
		{
			name:  "PING command",
			input: "PING\r\n",
			expected: []resp.Token{
				{Type: resp.TokenTypeValue, Value: "PING", Literal: "PING", Size: 4},
			},
		},
		{
			name:  "EXISTS command",
			input: "EXISTS somekey\r\n",
			expected: []resp.Token{
				{Type: resp.TokenTypeValue, Value: "EXISTS somekey", Literal: "EXISTS somekey", Size: 14},
			},
		},
		{
			name:  "Bulk string multi-line",
			input: "$12\r\nHello\r\nWorld\r\n",
			expected: []resp.Token{
				{Type: resp.TokenTypeBulkString, Value: 12, Literal: "$12", Size: 3},
				{Type: resp.TokenTypeValue, Value: "Hello", Literal: "Hello", Size: 5},
				{Type: resp.TokenTypeValue, Value: "World", Literal: "World", Size: 5},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.input)
			tokens, err := resp.Scan(reader)
			if err != nil {
				t.Fatalf("Scan() error = %v", err)
			}
			if len(tokens) != len(tt.expected) {
				t.Fatalf("expected %d tokens, got %d", len(tt.expected), len(tokens))
			}
			for i, token := range tokens {
				exp := tt.expected[i]
				if token.Type != exp.Type {
					t.Errorf("token %d type: expected %v, got %v", i, exp.Type, token.Type)
				}
				if token.Literal != exp.Literal {
					t.Errorf("token %d literal: expected %q, got %q", i, exp.Literal, token.Literal)
				}
				if token.Size != exp.Size {
					t.Errorf("token %d size: expected %v, got %v", i, exp.Size, token.Size)
				}
				switch exp.Type {
				case resp.TokenTypeArray, resp.TokenTypeBulkString:
					if v, ok := token.Value.(int); !ok || v != exp.Value.(int) {
						t.Errorf("token %d value: expected %v, got %v", i, exp.Value, token.Value)
					}
				case resp.TokenTypeValue:
					if v, ok := token.Value.(string); !ok || v != exp.Value.(string) {
						t.Errorf("token %d value: expected %q, got %q", i, exp.Value, token.Value)
					}
				default:
					t.Errorf("unexpected token type %v", exp.Type)
				}
			}
		})
	}
}
