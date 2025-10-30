package resp

import (
	"bufio"
	"fmt"
	"io"
	"math/big"
	"strings"
)

var ErrInvalidToken = fmt.Errorf("invalid token")

// ParseTokens allows parsing a list of tokens which can include
// multiple commands at once.
func ParseTokens(tokens []Token) ([]Command, error) {
	return parseTokens(tokens)
}

// ParseStream parses a single command from the byte stream.
// It stops right after a valid command is found, not until end of stream.
func ParseStream(r io.Reader) (Command, error) {
	return parseStream(r)
}

func parseStream(r io.Reader) (Command, error) {
	p := parser{newStreamIter(r)}
	return p.parseCmd()
}

func parseTokens(tokens []Token) ([]Command, error) {
	p := parser{&listIter{tokens: tokens}}
	return p.parseCmds()
}

type parser struct {
	Iter
}

func (p *parser) parseCmds() ([]Command, error) {
	cmds := make([]Command, 0)
	for {
		cmd, err := p.parseCmd()
		if err == io.EOF {
			break
		}
		cmds = append(cmds, cmd)
	}
	return cmds, nil
}

func (p *parser) parseCmd() (Command, error) {
	curr, err := p.Peek()
	if err != nil {
		return Command{}, err
	}

	switch curr.Type {
	case TokenTypeArray:
		d, err := p.parse()
		if err != nil {
			return Command{}, err
		}
		cmd, err := NewCommand(d.(Array))
		if err != nil {
			return Command{}, err
		}
		return cmd, nil
	case TokenTypeValue:
		cmd, err := p.parseInline(curr.Literal)
		if err != nil {
			return Command{}, err
		}
		p.Next()
		return cmd, nil
	default:
		return Command{}, fmt.Errorf("invalid starting token: %v", curr.Type)
	}
}

func (p *parser) parse() (any, error) {
	curr, err := p.Next()
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
		return str, nil
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
		nt, err := p.Next()
		if err != nil {
			return "", err
		}
		sb.WriteString(nt.Literal)
		i += 1
	}
	if sb.Len() > size {
		return "", fmt.Errorf("invalid bulk string size, got %v but actually %v", sb.Len(), size)
	}

	return sb.String(), nil
}

func (p *parser) parseVerbatimString(size int) (string, error) {
	if size <= 0 {
		return "", nil
	}
	sb := strings.Builder{}
	encl, err := p.Next()
	if err != nil {
		return "", err
	}
	if len(encl.Literal) < 4 {
		return "", fmt.Errorf("invalid encoding header, data len smaller than 4")
	}
	enc, ld := encl.Literal[:3], encl.Literal[4:]
	if enc == "txt" {
		sb.WriteString(ld)

		i := 0
		for sb.Len() < size {
			sb.WriteString("\r\n")
			nt, err := p.Next()
			if err != nil {
				return "", err
			}
			sb.WriteString(nt.Literal)
			i += 1
		}
		if sb.Len() > size {
			return "", fmt.Errorf("invalid bulk string size, got %v but actually %v", sb.Len(), size)
		}

		return sb.String(), nil
	} else {
		return "", fmt.Errorf("unsupported encoding: %v", enc)
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
			return Array{}, err
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
			return Map{}, err
		}
		m.Items[k] = v
	}

	return m, nil
}

func (p *parser) parseMapEntry() (any, any, error) {
	key, err := p.parse()
	if err != nil {
		return nil, nil, fmt.Errorf("invalid map key: %w", err)
	}
	val, err := p.parse()
	if err != nil {
		return nil, nil, fmt.Errorf("invalid map value: %w", err)
	}
	return key, val, nil
}

func (p *parser) parseSet(n int) (Set, error) {
	s := Set{Size: n, Items: make(map[any]struct{})}
	for range n {
		it, err := p.parse()
		if err != nil {
			return Set{}, nil
		}
		s.Items[it] = struct{}{}
	}

	return s, nil
}

type Iter interface {
	Next() (Token, error)
	Peek() (Token, error)
}

type listIter struct {
	tokens []Token
	curr   int
}

func (p *listIter) Next() (Token, error) {
	if p.isEnd() {
		return Token{}, io.EOF
	}
	p.curr += 1
	return p.tokens[p.curr-1], nil
}

func (p *listIter) Peek() (Token, error) {
	if p.isEnd() {
		return Token{}, io.EOF
	}
	return p.tokens[p.curr], nil
}

func (p *listIter) isEnd() bool {
	return p.curr >= len(p.tokens)
}

type streamIter struct {
	sc        *scanner
	r         io.Reader
	lastToken *Token
}

func newStreamIter(r io.Reader) *streamIter {
	sc := &scanner{str: bufio.NewReader(r)}
	si := &streamIter{
		r:         r,
		sc:        sc,
		lastToken: nil,
	}
	return si
}

func (p *streamIter) Next() (Token, error) {
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

func (p *streamIter) Peek() (Token, error) {
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
