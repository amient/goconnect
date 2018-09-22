package goc

import (
	"log"
	"sync/atomic"
	"time"
)

type Receiver interface {
	Elements() <-chan *Element
}

type Sender interface {
	Send(element *Element)
	Close() error
}

type Connector interface {
	GetNodeID() uint16
	GetReceiver(handlerId uint16) Receiver
	GetNumPeers() uint16
	NewSender(nodeId uint16, handlerId uint16) Sender
}

func NewContext(connector Connector, handlerId uint16) *Context {
	return &Context{
		handlerId: handlerId,
		connector: connector,
		emits:     make(chan *Element, 1),
		acks:      make(chan *Stamp, 1), //TODO configurable/adaptible
	}
}

type Context struct {
	handlerId      uint16
	connector      Connector
	emits          chan *Element
	acks           chan *Stamp
	autoi          uint64
	pending        chan *Element
	highestPending uint64
}

func (c *Context) GetNodeID() uint16 {
	return c.connector.GetNodeID()
}

func (c *Context) GetReceiver() Receiver {
	return c.connector.GetReceiver(c.handlerId)
}

func (c *Context) GetNumPeers() uint16 {
	return c.connector.GetNumPeers()
}

func (c *Context) GetSender(targetNodeId uint16) Sender {
	return c.connector.NewSender(targetNodeId, c.handlerId)
}

func (c *Context) GetSenders() []Sender {
	senders := make([]Sender, c.GetNumPeers())
	for peer := uint16(1); peer <= c.GetNumPeers(); peer++ {
		senders[peer-1] = c.GetSender(peer)
	}
	return senders
}

func (c *Context) Emit(element *Element) {
	c.emits <- element
}

func (c *Context) Emit2(value interface{}, checkpoint Checkpoint) {
	c.emits <- &Element{
		Value:      value,
		Checkpoint: checkpoint,
	}

}

func (c *Context) Ack(stamp *Stamp) {
	c.acks <- stamp
}

func (c *Context) Close() {
	close(c.emits)
}
func (c *Context) Attach() <-chan *Element {
	if c == nil {
		return nil
	}

	c.pending = make(chan *Element, 1000) //TODO this buffer is important so make it configurable but must be >= stream.cap

	//handling elements
	stampedOutput := make(chan *Element, 10)
	go c.downstream(stampedOutput)
	go c.upstream()

	return stampedOutput
}

func (c *Context) downstream(stampedOutput chan *Element) {
	defer close(stampedOutput)
	for element := range c.emits {
		element.Stamp.AddTrace(c.GetNodeID())
		//initial stamping of elements
		if element.Stamp.Hi == 0 {
			s := atomic.AddUint64(&c.autoi, 1)
			element.Stamp.Hi = s
			element.Stamp.Lo = s
		}
		if element.Stamp.Unix == 0 {
			element.Stamp.Unix = time.Now().Unix()
		}
		//checkpointing
		element.ack = c.Ack
		if element.Stamp.Hi > c.highestPending {
			c.highestPending = element.Stamp.Hi
		}
		//TODO if !c.isPassthrough {
		//stream.log("STAGE[%d] Incoming %d", stream.stage, element.Stamp)
		c.pending <- element
		//}

		stampedOutput <- element
	}
}


func (c *Context) upstream() {
	//handling acks
	for stamp := range c.acks {
		log.Println("TODO process ACK", stamp)
		//TODO reconcile
		//acks <- stamp
	}
}