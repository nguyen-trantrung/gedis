package resp

import (
	"bytes"
	"fmt"
	"io"
	"math/big"
	"strconv"
)

type Value interface {
	io.WriterTo
}

type Array struct {
	Size  int
	Items []any
}

type Map struct {
	Size  int
	Items map[any]any
}

type Attrb Map

type Set struct {
	Size  int
	Items map[any]struct{}
}

type Err struct {
	Size  int
	Value string
}

type BulkStr struct {
	Size  int
	Value string
	Null  bool
}

func NewErr(err error) Err {
	msg := err.Error()
	return Err{Size: len(msg), Value: msg}
}

func (a Array) WriteTo(w io.Writer) (int64, error) {
	header := fmt.Sprintf("*%d\r\n", a.Size)
	total := int64(0)
	n, err := w.Write([]byte(header))
	if err != nil {
		return 0, err
	}
	total += int64(n)
	if a.Size > 0 {
		for _, item := range a.Items {
			nn, err := WriteAnyTo(item, w)
			total += nn
			if err != nil {
				return total, err
			}
		}
	}
	return total, nil
}

func (m Map) WriteTo(w io.Writer) (n int64, err error) {
	header := fmt.Sprintf("%%%d\r\n", m.Size)
	if _, err := w.Write([]byte(header)); err != nil {
		return n, err
	}
	n += int64(len(header))
	for k, v := range m.Items {
		nn, err := WriteAnyTo(k, w)
		n += nn
		if err != nil {
			return n, err
		}
		nn, err = WriteAnyTo(v, w)
		n += nn
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func (a Attrb) WriteTo(w io.Writer) (n int64, err error) {
	header := fmt.Sprintf("|%d\r\n", a.Size)
	if _, err := w.Write([]byte(header)); err != nil {
		return n, err
	}
	n += int64(len(header))
	for k, v := range a.Items {
		nn, err := WriteAnyTo(k, w)
		n += nn
		if err != nil {
			return n, err
		}
		nn, err = WriteAnyTo(v, w)
		n += nn
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func (s Set) WriteTo(w io.Writer) (n int64, err error) {
	header := fmt.Sprintf("~%d\r\n", s.Size)
	if _, err := w.Write([]byte(header)); err != nil {
		return n, err
	}
	n += int64(len(header))
	for item := range s.Items {
		nn, err := WriteAnyTo(item, w)
		n += nn
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func (e *Err) WriteTo(w io.Writer) (n int64, err error) {
	var buf bytes.Buffer
	ec, err := buf.WriteString("-ERR ")
	if err != nil {
		return n, err
	}
	n += int64(ec)
	vc, err := buf.WriteString(e.Value)
	if err != nil {
		return n, err
	}
	n += int64(vc)
	elc, err := buf.WriteString("\r\n")
	if err != nil {
		return n, err
	}
	n += int64(elc)
	return buf.WriteTo(w)
}

func (e *BulkStr) WriteTo(w io.Writer) (n int64, err error) {
	header := fmt.Sprintf("$%d\r\n", e.Size)
	if e.Size >= 0 {
		header = fmt.Sprintf("%s%s\r\n", header, e.Value)
	}
	if _, err := w.Write([]byte(header)); err != nil {
		return n, err
	}
	n += int64(len(header))
	return n, nil
}

func WriteAnyTo(data any, out io.Writer) (n int64, err error) {
	if writer, ok := data.(io.WriterTo); ok {
		return writer.WriteTo(out)
	}
	switch val := data.(type) {
	case error:
		rerr := NewErr(val)
		return rerr.WriteTo(out)
	case Err:
		return val.WriteTo(out)
	case Array:
		return val.WriteTo(out)
	case Set:
		return val.WriteTo(out)
	case Attrb:
		return val.WriteTo(out)
	case Map:
		return val.WriteTo(out)
	case BulkStr:
		return val.WriteTo(out)
	case int, int8, int16, int32, int64:
		header := fmt.Sprintf(":%d\r\n", val)
		if _, err := out.Write([]byte(header)); err != nil {
			return n, err
		}
		n += int64(len(header))
	case bool:
		var b byte = 'f'
		if val {
			b = 't'
		}
		header := fmt.Sprintf("#%c\r\n", b)
		if _, err := out.Write([]byte(header)); err != nil {
			return n, err
		}
		n += int64(len(header))
	case float64:
		header := fmt.Sprintf(",%s\r\n", strconv.FormatFloat(val, 'f', -1, 64))
		if _, err := out.Write([]byte(header)); err != nil {
			return n, err
		}
		n += int64(len(header))
	case *big.Int:
		header := fmt.Sprintf("(%s\r\n", val.String())
		if _, err := out.Write([]byte(header)); err != nil {
			return n, err
		}
		n += int64(len(header))
	case string:
		header := fmt.Sprintf("+%s\r\n", val)
		if _, err := out.Write([]byte(header)); err != nil {
			return n, err
		}
		n += int64(len(header))
	case nil:
		if _, err := out.Write([]byte("_\r\n")); err != nil {
			return n, err
		}
		n += 3
	default:
		return n, fmt.Errorf("unsupported type: %T", data)
	}
	return n, nil
}
