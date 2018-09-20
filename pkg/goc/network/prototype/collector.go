package prototype

import "github.com/amient/goconnect/pkg/goc"

func NewCollector() *Collector {
	return &Collector{
		down: make(chan *goc.Element, 1),
		up:   make(chan *goc.Stamp, 1), //TODO configurable/adaptible
	}
}

type Collector struct {
	down chan *goc.Element
	up   chan *goc.Stamp
}

func (c *Collector) Emit(element *goc.Element) {
	c.down <- element
}

func (c *Collector) Ack(element *goc.Element) {
	c.up <- &element.Stamp
}

func (c *Collector) Close() {
	close(c.down)
}

