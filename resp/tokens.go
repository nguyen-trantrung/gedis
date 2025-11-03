package resp

type TokenType int

const (
	TokenTypeUnknown TokenType = iota
	TokenTypeSimpleString
	TokenTypeSimpleError
	TokenTypeInteger
	TokenTypeBulkString
	TokenTypeArray
	TokenTypeNull
	TokenTypeBoolean
	TokenTypeDouble
	TokenTypeBigNumber
	TokenTypeBulkError
	TokenTypeVerbatimString
	TokenTypeMap
	TokenTypeAttributes
	TokenTypeSet
	TokenTypePush
	TokenTypeValue
)

func (t TokenType) String() string {
	switch t {
	case TokenTypeSimpleString:
		return "SimpleString"
	case TokenTypeSimpleError:
		return "SimpleError"
	case TokenTypeInteger:
		return "Integer"
	case TokenTypeBulkString:
		return "BulkString"
	case TokenTypeArray:
		return "Array"
	case TokenTypeNull:
		return "Null"
	case TokenTypeBoolean:
		return "Boolean"
	case TokenTypeDouble:
		return "Double"
	case TokenTypeBigNumber:
		return "BigNumber"
	case TokenTypeBulkError:
		return "BulkError"
	case TokenTypeVerbatimString:
		return "VerbatimString"
	case TokenTypeMap:
		return "Map"
	case TokenTypeAttributes:
		return "Attributes"
	case TokenTypeSet:
		return "Set"
	case TokenTypePush:
		return "Push"
	case TokenTypeValue:
		return "Value"
	default:
		return "Unknown"
	}
}

type Token struct {
	Type    TokenType
	Value   any
	Literal string
	Size    int
}
