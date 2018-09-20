package prototype

import (
	"github.com/amient/goconnect/pkg/goc"
)

func NewCollector() *Collector {
	return &Collector{
		emits: make(chan *goc.Element, 1),
		acks:  make(chan *goc.Stamp, 1), //TODO configurable/adaptible
	}
}

type Collector struct {
	emits chan *goc.Element
	acks  chan *goc.Stamp
}

func (c *Collector) Emit(element *goc.Element) {
	c.emits <- element
}

func (c *Collector) Ack(element *goc.Element) {
	c.acks <- &element.Stamp
}

func (c *Collector) Close() {
	close(c.emits)
}

