package goc

import (
	"fmt"
	"io"
	"log"
	"sync/atomic"
	"time"
)

type Receiver interface {
	Elements() <-chan *Element
	Ack(Stamp) error
}

type Sender interface {
	Acks() <-chan *Stamp
	Send(element *Element)
	Close() error
}

type Connector interface {
	GetNodeID() uint16
	GetReceiver(stage uint16) Receiver
	GetNumPeers() uint16
	NewSender(nodeId uint16, stage uint16) Sender
	GenerateStageID() uint16
}

func NewContext(connector Connector, fn Fn) *Context {
	context := Context{
		stage:          connector.GenerateStageID(),
		connector:      connector,
		fn:             fn,
		emits:          make(chan *Element, 1),
		commits:        make(chan Watermark),
		commitRequests: make(chan bool),
		terminate:      make(chan bool),
		completed:      make(chan bool),
	}
	if fn == nil {
		context.isPassthrough = true
	} else {
		_, isMapFn := fn.(MapFn)
		_, isTransform := fn.(Transform)
		_, isForEach := fn.(ForEach)
		_, isForEachFn := fn.(ForEachFn)
		context.isPassthrough = !isTransform && (isMapFn || isForEachFn || isForEach)
	}
	return &context
}

type Context struct {
	stage          uint16
	connector      Connector
	fn             Fn
	up             *Context
	emits          chan *Element
	acks           chan *Stamp
	commitRequests chan bool
	commits        chan Watermark
	autoi          uint64
	isPassthrough  bool
	isCommitable   bool
	pending        chan *Element
	terminate      chan bool
	terminating    bool
	closed         bool
	completed      chan bool
	highestPending uint64
	highestAcked   uint64
}

func (c *Context) Closed() bool {
	return c.closed
}
func (c *Context) GetNodeID() uint16 {
	return c.connector.GetNodeID()
}

func (c *Context) GetStage() uint16 {
	return c.stage
}

func (c *Context) GetNumPeers() uint16 {
	return c.connector.GetNumPeers()
}

func (c *Context) MakeReceiver() Receiver {
	receiver := c.connector.GetReceiver(c.stage)
	go func() {
		for !c.closed {
			select {
			case checkpoint, ok := <-c.Commits():
				if ok {
					for _, stamp := range checkpoint {
						receiver.Ack(stamp.(Stamp))
					}
				}
			}
		}
	}()
	return receiver
}

func (c *Context) MakeSender(targetNodeId uint16) Sender {
	sender := c.connector.NewSender(targetNodeId, c.stage)
	go func() {
		for stamp := range sender.Acks() {
			c.up.Ack(stamp)
		}
	}()
	return sender
}

func (c *Context) MakeSenders() []Sender {
	senders := make([]Sender, c.GetNumPeers())
	for peer := uint16(1); peer <= c.GetNumPeers(); peer++ {
		senders[peer-1] = c.MakeSender(peer)
	}
	return senders
}

func (c *Context) Emit(element *Element) {
	c.emits <- element
}

func (c *Context) Ack(stamp *Stamp) {
	if c.isPassthrough {
		c.up.Ack(stamp)
	} else {
		c.acks <- stamp
	}
}

func (c *Context) Terminate() {
	if c.isPassthrough {
		c.close()
	} else {
		c.terminate <- true
	}
}

func (c *Context) Commits() <-chan Watermark {
	c.isCommitable = true
	c.commitRequests <- true
	return c.commits
}

func (c *Context) Attach() <-chan *Element {
	if c == nil {
		return nil
	}

	if !c.isPassthrough {
		c.checkpointer(100000) //TODO configurable capacity
	}

	output := make(chan *Element, 10)
	go func() {
		defer close(output)
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
			if !c.isPassthrough {
				c.log("Incoming %d", element.Stamp)
				c.pending <- element
			}

			output <- element
		}
	}()

	return output
}

func (c *Context) checkpointer(cap int) {

	c.pending = make(chan *Element, 1000) //TODO this buffer is important so make it configurable but must be >= stream.cap
	c.acks = make(chan *Stamp, 10000)

	go func() {
		pendingSuspendable := c.pending
		pending := Stamp{}
		pendingCheckpoints := make(map[uint64]*Checkpoint, cap)
		acked := make(map[uint64]bool, cap)
		watermark := make(Watermark)
		pendingCommitReuqest := false

		doCommit := func() {
			if len(watermark) > 0 {
				pendingCommitReuqest = false
				if c.isCommitable {
					//c.log("Commit Checkpoint: %v", watermark)
					c.commits <- watermark
				}
				watermark = make(map[int]interface{})
			}
		}

		maybeTerminate := func() {
			if c.terminating {
				if len(watermark) == 0 && c.highestPending == c.highestAcked {
					clean := (pendingCommitReuqest || !c.isCommitable) && !pending.Valid()
					if clean {
						c.close()
					}
				}
			}
		}

		resolveAcks := func() {
			//resolve acks <> pending
			for ; pending.Valid() && pendingCheckpoints[pending.Lo] != nil && acked[pending.Lo]; {
				lo := pending.Lo
				pending.Lo += 1
				chk := pendingCheckpoints[lo]
				delete(pendingCheckpoints, lo)
				delete(acked, lo)
				if stamp, is := watermark[chk.Part].(Stamp); is {
					watermark[chk.Part] = stamp.Merge(chk.Data.(Stamp))
				} else {
					watermark[chk.Part] = chk.Data
				}
				//c.log("DELETING PART %d DATA %v \n", chk.Part, chk.Data)
			}
			if len(pendingCheckpoints) < cap && pendingSuspendable == nil {
				//release the backpressure after capacity is freed
				pendingSuspendable = c.pending
			}

			//commit if pending commit request or not commitable in which case commit requests are never fired
			if pendingCommitReuqest || !c.isCommitable {
				doCommit()
			}
			maybeTerminate()
		}

		for !c.closed {
			select {
			case <-c.terminate:
				c.log("Terminating")
				c.terminating = true
				maybeTerminate()
			case <-c.commitRequests:
				pendingCommitReuqest = true
				doCommit()
				maybeTerminate()
			case stamp := <-c.acks:
				if stamp.Hi > c.highestAcked {
					c.highestAcked = stamp.Hi
				}
				//this doesn't have to block because it doesn't create any memory build-Up, if anything it frees memory
				for u := stamp.Lo; u <= stamp.Hi; u ++ {
					acked[u] = true
				}

				resolveAcks()

				c.log("ACK(%d) Pending: %v Acked: %v isCommitable=%v, commitRequested=%v\n", stamp, pending.String(), acked, c.isCommitable, pendingCommitReuqest)

				if c.up != nil && !c.isCommitable {
					//commitables have to ack their upstream manually
					c.up.Ack(stamp)
				}

			case e := <-pendingSuspendable:
				if c.closed {
					panic(fmt.Errorf("STAGE[%d] Illegal Pending %v", c.stage, e))
				}
				pending.Merge(e.Stamp)
				for u := e.Stamp.Lo; u <= e.Stamp.Hi; u++ {
					pendingCheckpoints[u] = &e.Checkpoint
				}

				resolveAcks()

				c.log("Pending: %v Checkpoints: %v\n", pending, len(pendingCheckpoints))
				if len(pendingCheckpoints) == cap {
					//in order to apply backpressure this channel needs to be nilld but right now it hangs after second pendingSuspendable
					pendingSuspendable = nil
					//stream.log("STAGE[%d] Applying backpressure, pending acks: %d\n", stream.stage, len(pendingCheckpoints))
				} else if len(pendingCheckpoints) > cap {
					panic(fmt.Errorf("illegal accumulator state, buffer size higher than %d", cap))
				}

			}
		}
	}()
}


func (c *Context) close() {
	if ! c.closed {
		c.log("Close()")
		c.completed <- true
		close(c.emits)
		if !c.isPassthrough {
			close(c.commits)
			close(c.acks)
			close(c.pending)
			c.acks = nil
			c.pending = nil
		}
		c.closed = true
		if fn, ok := c.fn.(io.Closer); ok {
			if err := fn.Close(); err != nil {
				panic(err)
			}
		}

	}
}

func (c *Context) log(f string, args ... interface{}) {
	args2 := make([]interface{}, len(args) + 2)
	args2[0] = c.GetNodeID()
	args2[1] = c.stage
	for i := range args {
		args2[i+2] = args[i]
	}
	if c.stage > 0 {
		log.Printf("NODE[%d] STAGE[%d] "+f, args2...)
	}
}



