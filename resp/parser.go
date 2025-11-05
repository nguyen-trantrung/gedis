package resp

import (
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"strings"
)

var ErrInvalidToken = fmt.Errorf("invalid token")
var ErrProtocolError = fmt.Errorf("protocol error")

// ParseCmd parses a single command from the byte stream.
// It stops right after a valid command is found, not until end of stream.
func ParseCmd(r io.Reader) (Command, error) {
	sc := &scanner{str: r, bytesRead: 0}
	p := parser{newStreamIterFromScanner(sc)}
	sc.resetBytesRead()
	cmd, err := p.parseCmd()
	if err != nil {
		return cmd, err
	}
	bytesRead := sc.getBytesRead()
	cmd.Size = bytesRead
	return cmd, err
}

// ParseValue parses a single RESP value from the byte stream.
func ParseValue(r io.Reader) (any, error) {
	p := parser{newStreamIter(r)}
	return p.parse()
}

// ParseRDBFile parses an RDB file from the byte stream.
// RDB files are sent in the format: $<length>\r\n<binary_contents>
// Unlike bulk strings, they don't have a trailing \r\n
func ParseRDBFile(r io.Reader) ([]byte, error) {
	sc := &scanner{str: r}
	iter := newStreamIterFromScanner(sc)
	return parseRDBFileStream(iter, sc.str)
}

type parser struct {
	*streamIter
}

func (p *parser) parseCmd() (Command, error) {
	curr, err := p.peek()
	if err != nil {
		var opErr *net.OpError
		if err == io.EOF || errors.As(err, &opErr) {
			return Command{}, err
		}
		return Command{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}

	switch curr.Type {
	case TokenTypeArray:
		d, err := p.parse()
		if err != nil {
			return Command{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
		}
		cmd, err := NewCommand(d.(Array))
		if err != nil {
			return Command{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
		}
		return cmd, nil
	case TokenTypeValue:
		cmd, err := p.parseInline(curr.Literal)
		if err != nil {
			return Command{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
		}
		p.next()
		return cmd, nil
	default:
		return Command{}, fmt.Errorf("%w: invalid starting token: %v", ErrInvalidToken, curr.Type)
	}
}

func (p *parser) parse() (any, error) {
	curr, err := p.next()
	if err != nil {
		return nil, err
	}
	switch curr.Type {
	case TokenTypeArray:
		arr, err := p.parseArray(curr.Value.(int))
		if err != nil {
			return nil, err
		}
		return arr, nil
	case TokenTypeSimpleString:
		return curr.Value.(string), nil
	case TokenTypeSimpleError:
		return Err{Value: curr.Value.(string), Size: curr.Size}, nil
	case TokenTypeInteger:
		return curr.Value.(int), nil
	case TokenTypeBulkString:
		str, err := p.parseBulkString(curr.Value.(int))
		if err != nil {
			return nil, err
		}
		return BulkStr{Size: len(str), Value: str}, nil
	case TokenTypeNull:
		return nil, nil
	case TokenTypeBoolean:
		return curr.Value.(bool), nil
	case TokenTypeDouble:
		return curr.Value.(float64), nil
	case TokenTypeBigNumber:
		return curr.Value.(*big.Int), nil
	case TokenTypeBulkError:
		str, err := p.parseBulkString(curr.Value.(int))
		if err != nil {
			return nil, err
		}
		return Err{Value: str, Size: curr.Value.(int)}, nil
	case TokenTypeVerbatimString:
		str, err := p.parseVerbatimString(curr.Value.(int))
		if err != nil {
			return nil, err
		}
		return str, nil
	case TokenTypeMap:
		m, err := p.parseMap(curr.Value.(int))
		if err != nil {
			return nil, err
		}
		return m, nil
	case TokenTypeAttributes:
		m, err := p.parseMap(curr.Value.(int))
		if err != nil {
			return nil, err
		}
		return Attrb(m), nil
	case TokenTypeSet:
		s, err := p.parseSet(curr.Value.(int))
		if err != nil {
			return nil, err
		}
		return s, nil
	case TokenTypeValue:
		return curr.Literal, nil
	default:
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, curr.Literal)
	}
}

func (p *parser) parseInline(inl string) (Command, error) {
	args := make([]any, 0)
	for arg := range strings.SplitSeq(inl, " ") {
		args = append(args, arg)
	}
	return newCommand(args)
}

func (p *parser) parseBulkString(size int) (string, error) {
	if size <= 0 {
		return "", nil
	}
	sb := strings.Builder{}
	i := 0
	for sb.Len() < size {
		if i > 0 {
			sb.WriteString("\r\n")
		}
		nt, err := p.nextLiteral()
		if err != nil {
			if err == io.EOF {
				return "", err
			}
			return "", fmt.Errorf("%w: %v", ErrInvalidToken, err)
		}
		sb.WriteString(nt.Literal)
		i += 1
	}
	if sb.Len() > size {
		return "", fmt.Errorf("%w: invalid bulk string size, got %v but actually %v", ErrInvalidToken, sb.Len(), size)
	}

	return sb.String(), nil
}

func (p *parser) parseVerbatimString(size int) (string, error) {
	if size <= 0 {
		return "", nil
	}
	sb := strings.Builder{}
	encl, err := p.next()
	if err != nil {
		if err == io.EOF {
			return "", err
		}
		return "", fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	if len(encl.Literal) < 4 {
		return "", fmt.Errorf("%w: invalid encoding header, data len smaller than 4", ErrInvalidToken)
	}
	enc, ld := encl.Literal[:3], encl.Literal[4:]
	if enc == "txt" {
		sb.WriteString(ld)

		i := 0
		for sb.Len() < size {
			sb.WriteString("\r\n")
			nt, err := p.nextLiteral()
			if err != nil {
				if err == io.EOF {
					return "", err
				}
				return "", fmt.Errorf("%w: %v", ErrInvalidToken, err)
			}
			sb.WriteString(nt.Literal)
			i += 1
		}
		if sb.Len() > size {
			return "", fmt.Errorf("%w: invalid bulk string size, got %v but actually %v", ErrInvalidToken, sb.Len(), size)
		}

		return sb.String(), nil
	} else {
		return "", fmt.Errorf("%w: unsupported encoding: %v", ErrInvalidToken, enc)
	}
}

func (p *parser) parseArray(n int) (Array, error) {
	arr := Array{
		Size:  n,
		Items: nil,
	}
	if n <= 0 {
		return arr, nil
	}
	arr.Items = make([]any, 0, n)

	for range n {
		it, err := p.parse()
		if err != nil {
			return Array{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
		}
		arr.Items = append(arr.Items, it)
	}

	return arr, nil
}

func (p *parser) parseMap(n int) (Map, error) {
	m := Map{Size: n, Items: make(map[any]any, n)}
	for range n {
		k, v, err := p.parseMapEntry()
		if err != nil {
			return Map{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
		}
		m.Items[k] = v
	}

	return m, nil
}

func (p *parser) parseMapEntry() (any, any, error) {
	key, err := p.parse()
	if err != nil {
		return nil, nil, fmt.Errorf("%w: invalid map key: %v", ErrInvalidToken, err)
	}
	val, err := p.parse()
	if err != nil {
		return nil, nil, fmt.Errorf("%w: invalid map value: %v", ErrInvalidToken, err)
	}
	return key, val, nil
}

func (p *parser) parseSet(n int) (Set, error) {
	s := Set{Size: n, Items: make(map[any]struct{})}
	for range n {
		it, err := p.parse()
		if err != nil {
			return Set{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
		}
		s.Items[it] = struct{}{}
	}

	return s, nil
}

func parseRDBFileStream(iter *streamIter, rawIo io.Reader) ([]byte, error) {
	// First, read the $<length>\r\n part using the normal token parsing
	curr, err := iter.next()
	if err != nil {
		return nil, err
	}

	if curr.Type != TokenTypeBulkString {
		return nil, fmt.Errorf("%w: expected bulk string for RDB file, got %v", ErrInvalidToken, curr.Type)
	}

	size := curr.Value.(int)
	if size < 0 {
		return nil, fmt.Errorf("%w: invalid RDB file size: %d", ErrInvalidToken, size)
	}

	if size == 0 {
		return []byte{}, nil
	}

	data := make([]byte, size)
	n, err := io.ReadFull(rawIo, data)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to read RDB file data: %v", ErrInvalidToken, err)
	}

	if n != size {
		return nil, fmt.Errorf("%w: expected %d bytes but read %d", ErrInvalidToken, size, n)
	}

	return data, nil
}

type streamIter struct {
	sc        *scanner
	lastToken *Token
}

func newStreamIter(r io.Reader) *streamIter {
	sc := &scanner{str: r}
	si := newStreamIterFromScanner(sc)
	return si
}

func newStreamIterFromScanner(sc *scanner) *streamIter {
	si := &streamIter{
		sc:        sc,
		lastToken: nil,
	}
	return si
}

func (p *streamIter) next() (Token, error) {
	if p.lastToken != nil {
		tok := *p.lastToken
		p.lastToken = nil
		return tok, nil
	}
	l, err := p.sc.nextLine()
	if err != nil {
		return Token{}, err
	}
	tok, err := l.scanToken()
	if err != nil {
		return Token{}, err
	}
	return tok, nil
}

func (p *streamIter) nextLiteral() (Token, error) {
	if p.lastToken != nil {
		tok := *p.lastToken
		p.lastToken = nil
		return tok, nil
	}
	l, err := p.sc.nextLine()
	if err != nil {
		return Token{}, err
	}
	tok := Token{
		Type:    TokenTypeValue,
		Literal: string(l.l),
		Size:    len(l.l),
	}
	return tok, nil
}

func (p *streamIter) peek() (Token, error) {
	if p.lastToken != nil {
		return *p.lastToken, nil
	}
	l, err := p.sc.nextLine()
	if err != nil {
		return Token{}, err
	}
	tok, err := l.scanToken()
	if err != nil {
		return Token{}, err
	}
	p.lastToken = new(Token)
	*p.lastToken = tok
	return tok, nil
}
