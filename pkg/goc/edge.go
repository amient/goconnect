package goc

type Edge struct {
	Context *Context
	Source  *Collection
	Fn      Fn
	Dest    *Collection
}
