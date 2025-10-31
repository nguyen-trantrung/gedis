package resp

import (
	"bufio"
	"fmt"
	"io"
	"math/big"
	"strconv"
)

var (
	ErrTooManyBytes error = fmt.Errorf("payload exceeded 512MB")
)

const (
	MB     = 1024 * 1024
	_512MB = 512 * MB
)

type scanner struct {
	str *bufio.Reader
}

type line struct {
	curr int
	l    []byte
}

// Tokens reads until end of stream and returns a list of tokens found
func Scan(str io.Reader) ([]Token, error) {
	return tokens(str)
}

func tokens(str io.Reader) ([]Token, error) {
	sc := scanner{
		str: bufio.NewReader(str),
	}
	return sc.scanLines()
}

func (s *scanner) scanLines() ([]Token, error) {
	tokens := make([]Token, 0)
	for {
		l, err := s.nextLine()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		t, err := l.scanToken()
		if err == io.EOF {
			continue
		} else if err != nil {
			return nil, err
		}
		tokens = append(tokens, t)
	}
	return tokens, nil
}

func (s *scanner) nextLine() (*line, error) {
	line := &line{
		l: make([]byte, 0),
	}
	for {
		b, err := s.str.ReadByte()
		if err != nil {
			return nil, err
		}
		line.l = append(line.l, b)
		if len(line.l) >= _512MB {
			return nil, ErrTooManyBytes
		}
		if len(line.l) >= 2 && line.l[len(line.l)-2] == '\r' && line.l[len(line.l)-1] == '\n' {
			// Remove \r\n from the line
			line.l = line.l[:len(line.l)-2]
			break
		}
	}
	return line, nil
}

func (s *line) scanToken() (Token, error) {
	if len(s.l) == 0 {
		return Token{}, fmt.Errorf("%w: empty line", ErrProtocolError)
	}
	t := s.next()
	switch t {
	case '+':
		data := s.l[s.curr:]
		return Token{
			Type:    TokenTypeSimpleString,
			Value:   string(data),
			Literal: "+" + string(data),
			Size:    1 + len(data)}, nil
	case '-':
		data := s.l[s.curr:]
		return Token{
			Type:    TokenTypeSimpleError,
			Value:   string(data),
			Literal: "-" + string(data),
			Size:    1 + len(data)}, nil
	case ':':
		str := string(s.l[s.curr:])
		data, err := strconv.Atoi(str)
		if err != nil {
			return Token{}, fmt.Errorf("%w: invalid integer value: %v", ErrInvalidToken, str)
		}
		return Token{
			Type:    TokenTypeInteger,
			Value:   data,
			Literal: ":" + str,
			Size:    1 + len(str)}, nil
	case '$':
		str := string(s.l[s.curr:])
		data, err := strconv.Atoi(str)
		if err != nil {
			return Token{}, fmt.Errorf("%w: invalid size value: %v", ErrInvalidToken, err)
		}
		return Token{
			Type:    TokenTypeBulkString,
			Value:   data,
			Literal: "$" + str,
			Size:    1 + len(str)}, nil
	case '*':
		str := string(s.l[s.curr:])
		data, err := strconv.Atoi(str)
		if err != nil {
			return Token{}, fmt.Errorf("%w: invalid size value: %v", ErrInvalidToken, err)
		}
		return Token{
			Type:    TokenTypeArray,
			Value:   data,
			Literal: "*" + str,
			Size:    1 + len(str)}, nil
	case '_':
		if !s.isEnd() {
			return Token{}, fmt.Errorf("%w: invalid nil syntax", ErrInvalidToken)
		}
		return Token{
			Type:    TokenTypeNull,
			Value:   nil,
			Literal: "",
			Size:    0}, nil
	case '#':
		str := string(s.l[s.curr:])
		var b bool
		switch str {
		case "t":
			b = true
		case "f":
			b = false
		default:
			return Token{}, fmt.Errorf("%w: invalid boolean value: %v", ErrInvalidToken, str)
		}
		return Token{
			Type:    TokenTypeBoolean,
			Value:   b,
			Literal: "#" + str,
			Size:    1 + len(str)}, nil
	case ',':
		str := string(s.l[s.curr:])
		data, err := strconv.ParseFloat(str, 64)
		if err != nil {
			return Token{}, fmt.Errorf("%w: invalid size value: %v", ErrInvalidToken, err)
		}
		return Token{
			Type:    TokenTypeDouble,
			Value:   data,
			Literal: "," + str,
			Size:    1 + len(str)}, nil
	case '(':
		str := string(s.l[s.curr:])
		num := new(big.Int)
		_, ok := num.SetString(str, 10)
		if !ok {
			return Token{}, fmt.Errorf("%w: invalid big integer", ErrInvalidToken)
		}
		return Token{
			Type:    TokenTypeBigNumber,
			Value:   num,
			Literal: "(" + str,
			Size:    1 + len(str)}, nil
	case '!':
		str := string(s.l[s.curr:])
		data, err := strconv.Atoi(str)
		if err != nil {
			return Token{}, fmt.Errorf("%w: invalid size value: %v", ErrInvalidToken, err)
		}
		return Token{
			Type:    TokenTypeBulkError,
			Value:   data,
			Literal: "!" + str,
			Size:    1 + len(str)}, nil
	case '=':
		str := string(s.l[s.curr:])
		data, err := strconv.Atoi(str)
		if err != nil {
			return Token{}, fmt.Errorf("%w: invalid size value: %v", ErrInvalidToken, err)
		}
		return Token{
			Type:    TokenTypeVerbatimString,
			Value:   data,
			Literal: "=" + str,
			Size:    1 + len(str)}, nil
	case '%':
		str := string(s.l[s.curr:])
		data, err := strconv.Atoi(str)
		if err != nil {
			return Token{}, fmt.Errorf("%w: invalid size value: %v", ErrInvalidToken, err)
		}
		return Token{
			Type:    TokenTypeMap,
			Value:   data,
			Literal: "%" + str,
			Size:    1 + len(str)}, nil
	case '|':
		str := string(s.l[s.curr:])
		data, err := strconv.Atoi(str)
		if err != nil {
			return Token{}, fmt.Errorf("%w: invalid size value: %v", ErrInvalidToken, err)
		}
		return Token{
			Type:    TokenTypeAttributes,
			Value:   data,
			Literal: "|" + str,
			Size:    1 + len(str)}, nil
	case '~':
		str := string(s.l[s.curr:])
		data, err := strconv.Atoi(str)
		if err != nil {
			return Token{}, fmt.Errorf("%w: invalid size value: %v", ErrInvalidToken, err)
		}
		return Token{
			Type:    TokenTypeSet,
			Value:   data,
			Literal: "~" + str,
			Size:    1 + len(str)}, nil
	case '>':
		str := string(s.l[s.curr:])
		data, err := strconv.Atoi(str)
		if err != nil {
			return Token{}, fmt.Errorf("%w: invalid size value: %v", ErrInvalidToken, err)
		}
		return Token{
			Type:    TokenTypePush,
			Value:   data,
			Literal: ">" + str,
			Size:    1 + len(str)}, nil
	default:
		str := string(s.l[s.curr-1:])
		return Token{
			Type:    TokenTypeValue,
			Value:   str,
			Literal: str,
			Size:    len(str),
		}, nil
	}
}

func (s *line) isEnd() bool {
	return s.curr >= len(s.l)
}

func (s *line) next() byte {
	if s.isEnd() {
		return 0
	}
	s.curr += 1
	return s.l[s.curr-1]
}
