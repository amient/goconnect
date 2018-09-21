package prototype

import "github.com/amient/goconnect/pkg/goc"

type Collection struct {
	pipe     *Pipe
	elements <-chan *goc.Element
	acks     <-chan *goc.Stamp
}

func (c *Collection) Elements() <-chan *goc.Element {
	return c.elements
}

