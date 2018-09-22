package goc

type Collection struct {
	elements <-chan *Element
	acks     <-chan *Stamp
}

func (c *Collection) Elements() <-chan *Element {
	if c == nil {
		return nil
	} else {
		return c.elements
	}
}

func NewCollection(context *Context) *Collection {
	acks := make(chan *Stamp)
	return &Collection{
		elements: context.Attach(),
		acks:     acks,
	}
}
