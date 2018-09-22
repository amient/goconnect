package goc

type Edge struct {
	Source  <-chan *Element
	Fn      *Fn
	Context *Context
}
