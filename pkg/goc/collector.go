package goc

import (
	"log"
	"sync/atomic"
	"time"
)

func NewCollector(nodeId uint16) *Collector {
	return &Collector{
		NodeID: nodeId,
		emits:  make(chan *Element, 1),
		acks:   make(chan *Stamp, 1), //TODO configurable/adaptible
	}
}

type Collector struct {
	NodeID uint16
	emits  chan *Element
	acks   chan *Stamp
	autoi  uint64
}

func (c *Collector) Emit(element *Element) {
	c.emits <- element
}

func (c *Collector) Emit2(value interface{}, checkpoint Checkpoint) {
	c.emits <- &Element{
		Value:      value,
		Checkpoint: checkpoint,
	}

}

func (c *Collector) Ack(stamp *Stamp) {
	c.acks <- stamp
}

func (c *Collector) Close() {
	close(c.emits)
}
func (c *Collector) Wrap(acks chan *Stamp) <-chan *Element {

	//handling elements
	stampedOutput := make(chan *Element, 10)
	go func() {
		defer close(stampedOutput)
		for element := range c.emits {
			element.Stamp.AddTrace(c.NodeID)
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
			//TODO stream.pendingAck(element)

			stampedOutput <- element
		}
	}()

	//handling acks
	go func() {
		for stamp := range c.acks {
			log.Println("TODO process ACK", stamp)
			//TODO reconcile
			//acks <- stamp
		}
	}()


	return stampedOutput
}
