package goc

type Edge struct {
	Source *Context
	Dest   *Context
	Fn     Fn
}
