package resp

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
