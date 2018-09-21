package goc

type Collection struct {
	elements <-chan *Element
	acks     <-chan *Stamp
}

func (c *Collection) Elements() <-chan *Element {
	return c.elements
}

func NewCollection(collector *Collector) *Collection {
	acks := make(chan *Stamp)
	return &Collection{
		elements: collector.Wrap(acks),
		acks: acks,
	}
}

