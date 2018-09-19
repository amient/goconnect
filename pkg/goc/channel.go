package goc

type Downstream interface {
	// -->
	Down() <-chan *Element
	SendUp(*Stamp) error
}

type Upstream interface {
	// <--
	Up() <-chan *Stamp
	SendDown(*Element) error
}



func NewLocalDownstream() *LocalChannel {
	return &LocalChannel{
		down: make(chan *Element),
		up:   make(chan *Stamp),
	}
}

type LocalChannel struct {
	down chan *Element
	up   chan *Stamp
}

func (c *LocalChannel) Up() <-chan *Stamp {
	return c.up
}

func (c *LocalChannel) SendDown(element *Element) error {
	c.down <- element
	return nil
}

func (c *LocalChannel) Down() <-chan *Element {
	return c.down
}

func (c *LocalChannel) SendUp(stamp *Stamp) error {
	c.up <- stamp
	return nil
}

func (c *LocalChannel) Close() error {
	close(c.down)
	close(c.up)
	return nil
}

//type ElementWise func(input *Element, collector Elements)

//type RootTransform struct {
//	collector Elements
//	output    Channel
//	fn			RootFn
//}
//
//func (r *RootTransform) ConnectDownstream(output Channel) {
//	r.output = output
//}
//
//func (r *RootTransform) Run() {
//	r.collector = make(Elements)
//	go func() {
//		r.fn.Run(r.collector)
//	}()
//
//	go func() {
//		for emitted := range r.collector {
//			switch emitted.signal {
//			case FinalCheckpoint:
//				//if t, is := fn.(GroupFn); is {
//				//	//FIXME triggering has to be possible in other ways
//				//	for _, outputElement := range t.Trigger() {
//				//		outputElement.Stamp = p.highStamp
//				//		p.output <- outputElement
//				//		p.highStamp.Lo = 0 //make it invalid
//				//	}
//				//}
//				p.Emit()
//				//TODO up.terminate <- true
//				return
//			case NoSignal:
//				//initial stamping of elements
//				if emitted.Stamp.Hi == 0 {
//					s := atomic.AddUint64(&p.uniqueStamp, 1)
//					emitted.Stamp.Hi = s
//					emitted.Stamp.Lo = s
//				}
//				if emitted.Stamp.Unix == 0 {
//					emitted.Stamp.Unix = time.Now().Unix()
//				}
//
//				//TODO fn.pendingAck(emitted.Stamp, emitted.Checkpoint)
//
//				//merged stamp is used forTransform.Trigger()
//				p.highStamp.merge(emitted.Stamp)
//
//				//pass the element after it has been intercepted to the fn
//				p.output <- emitted
//			}
//		}
//	}()
//}
//

//func NewElementWiseTransform() *ElementWiseTransform {
//	return &ElementWiseTransform{
//		collector: make(Elements),
//	}
//}
//
//type ElementWiseTransform struct {
//	input       Channel
//	output      Channel
//	collector   Elements
//	uniqueStamp uint64
//	highStamp   Stamp
//}
//
//func (p *ElementWiseTransform) ConnectDownstream(output Channel) {
//	p.output = output
//}
//
//func (p *ElementWiseTransform) ConnectUpstream(input Channel) {
//	p.input = input
//}
//
//func (p *ElementWiseTransform) Emit(emitted *Element) {
//
//}
//
//func (p *ElementWiseTransform) Run() {
//	go func() {
//		for emitted := range p.collector {
//
//		}
//	}()
//}
