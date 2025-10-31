package resp_test

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/ttn-nguyen42/gedis/resp"
)

func TestErr_WriteTo(t *testing.T) {
	tests := []struct {
		name     string
		err      resp.Err
		expected string
	}{
		{
			name:     "simple_error",
			err:      resp.Err{Size: 10, Value: "test error"},
			expected: "-ERR test error\r\n",
		},
		{
			name:     "empty_error",
			err:      resp.Err{Size: 0, Value: ""},
			expected: "-ERR \r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			n, err := tt.err.WriteTo(&buf)
			if err != nil {
				t.Fatalf("WriteTo() error = %v", err)
			}
			if int(n) != len(tt.expected) {
				t.Errorf("WriteTo() wrote %d bytes, expected %d", n, len(tt.expected))
			}
			if got := buf.String(); got != tt.expected {
				t.Errorf("WriteTo() = %q, expected %q", got, tt.expected)
			}
		})
	}
}

func TestWriteAnyTo(t *testing.T) {
	tests := []struct {
		name     string
		data     any
		expected string
	}{
		{
			name:     "string",
			data:     "hello",
			expected: "$5\r\nhello\r\n",
		},
		{
			name:     "int",
			data:     42,
			expected: ":42\r\n",
		},
		{
			name:     "bool true",
			data:     true,
			expected: "#t\r\n",
		},
		{
			name:     "bool false",
			data:     false,
			expected: "#f\r\n",
		},
		{
			name:     "float64",
			data:     3.14,
			expected: ",3.14\r\n",
		},
		{
			name:     "big int",
			data:     big.NewInt(12345),
			expected: "(12345\r\n",
		},
		{
			name:     "nil",
			data:     nil,
			expected: "_\r\n",
		},
		{
			name:     "array",
			data:     resp.Array{Size: 2, Items: []any{"foo", "bar"}},
			expected: "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
		},
		{
			name:     "map",
			data:     resp.Map{Size: 1, Items: map[any]any{"key": "value"}},
			expected: "%1\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
		},
		{
			name:     "set",
			data:     resp.Set{Size: 2, Items: map[any]struct{}{"a": {}, "b": {}}},
			expected: "~2\r\n$1\r\na\r\n$1\r\nb\r\n",
		},
		{
			name:     "err",
			data:     resp.Err{Size: 0, Value: "error msg"},
			expected: "!9\r\nerror msg\r\n",
		},
		{
			name:     "attrb",
			data:     resp.Attrb(resp.Map{Size: 1, Items: map[any]any{"attr": "val"}}),
			expected: "|1\r\n$4\r\nattr\r\n$3\r\nval\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			n, err := resp.WriteAnyTo(tt.data, &buf)
			if err != nil {
				t.Fatalf("writeAnyTo() error = %v", err)
			}
			if int(n) != len(tt.expected) {
				t.Errorf("writeAnyTo() wrote %d bytes, expected %d", n, len(tt.expected))
			}
			if got := buf.String(); got != tt.expected {
				t.Errorf("writeAnyTo() = %q, expected %q", got, tt.expected)
			}
		})
	}
}

func TestWriteAnyTo_UnsupportedType(t *testing.T) {
	var buf bytes.Buffer
	_, err := resp.WriteAnyTo(make(chan int), &buf)
	if err == nil {
		t.Error("writeAnyTo() expected error for unsupported type")
	}
}
