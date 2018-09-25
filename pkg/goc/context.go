package goc

import (
	"fmt"
	"io"
	"log"
	"reflect"
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
	GetReceiver(stage uint16) Receiver
	GetNumPeers() uint16
	NewSender(nodeId uint16, stage uint16) Sender
	GenerateStageID() uint16
}

func NewContext(connector Connector, fn Fn) *Context {
	context := Context{
		stage:     connector.GenerateStageID(),
		connector: connector,
		fn:        fn,
		emits:     make(chan *Element, 1),
		terminate: make(chan bool, 2),
		completed: make(chan bool, 1),
	}
	if fn == nil {
		context.isPassthrough = true
	} else {
		_, context.isPassthrough = fn.(MapFn)
		context.commitable, context.isCommitable = fn.(Commitable)
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
	autoi          uint64
	isPassthrough  bool
	commitable     Commitable
	isCommitable   bool
	pending        chan *Element
	terminate      chan bool
	terminating    bool
	closed         bool
	completed      chan bool
	highestPending uint64
	highestAcked   uint64
}

func (c *Context) GetNodeID() uint16 {
	return c.connector.GetNodeID()
}

func (c *Context) GetReceiver() Receiver {
	return c.connector.GetReceiver(c.stage)
}

func (c *Context) GetNumPeers() uint16 {
	return c.connector.GetNumPeers()
}

func (c *Context) GetSender(targetNodeId uint16) Sender {
	return c.connector.NewSender(targetNodeId, c.stage)
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

func (c *Context) Ack(stamp *Stamp) {
	if c.isPassthrough {
		c.up.Ack(stamp)
	} else {
		c.acks <- stamp
	}
}

func (c *Context) Close() {
	if ! c.closed {
		c.closed = true
		close(c.emits)
		if fn, ok := c.fn.(io.Closer); ok {
			if err := fn.Close(); err != nil {
				panic(err)
			}
		}
	}
}

func (c *Context) Attach() <-chan *Element {
	if c == nil {
		return nil
	}

	if c.isPassthrough {
		log.Printf("Initializing Passthru Stage %d %v\n", c.stage, reflect.TypeOf(c.fn))
	} else {
		log.Printf("Initializing Buffered Stage %d %v\n", c.stage, reflect.TypeOf(c.fn))
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
				c.log("STAGE[%d] Incoming %d", c.stage, element.Stamp)
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
	commits := make(chan map[int]interface{})
	commitRequests := make(chan bool)

	go func() {
		pendingSuspendable := c.pending
		terminated := false
		pending := Stamp{}
		pendingCheckpoints := make(map[uint64]*Checkpoint, cap)
		acked := make(map[uint64]bool, cap)
		checkpoint := make(map[int]interface{})
		pendingCommitReuqest := false

		if c.isCommitable {
			go func() {
				commitRequests <- true
				for checkpoint := range commits {
					c.log("STAGE[%d] Commit Checkpoint: %v", c.stage, checkpoint)
					c.commitable.Commit(checkpoint)
					commitRequests <- true
				}
			}()
		}

		doCommit := func() {
			if len(checkpoint) > 0 {
				pendingCommitReuqest = false
				if c.isCommitable {
					commits <- checkpoint
				}
				checkpoint = make(map[int]interface{})
			}
		}

		maybeTerminate := func() {
			if c.terminating {
				if len(checkpoint) == 0 && c.highestPending == c.highestAcked {
					clean := (pendingCommitReuqest || !c.isCommitable) && !pending.Valid()
					if clean {
						terminated = true
						c.log("STAGE[%d] Completed", c.stage)
						close(commits)
						close(commitRequests)
						commitRequests = nil
						close(c.acks)
						close(c.pending)
						c.acks = nil
						c.pending = nil
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
				checkpoint[chk.Part] = chk.Data
				//c.log("STAGE[%d] DELETING PART %d DATA %v \n", c.stage, chk.Part, chk.Data)
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

		for !terminated {
			select {
			case <-c.terminate:
				c.log("STAGE[%d] Terminating", c.stage)
				c.terminating = true
				maybeTerminate()
			case <-commitRequests:
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
					//if c, ok := pendingCheckpoints[u]; ok {
					//	c.acked = true
					//}
				}

				resolveAcks()

				c.log("STAGE[%d] ACK(%d) Pending: %v Acked: %v\n", c.stage, stamp, pending.String(), acked)

				if c.up != nil {
					c.up.Ack(stamp)
				}

			case e := <-pendingSuspendable:
				if terminated {
					panic(fmt.Errorf("STAGE[%d] Illegal Pending %v", c.stage, e))
				}
				pending.Merge(e.Stamp)
				for u := e.Stamp.Lo; u <= e.Stamp.Hi; u++ {
					pendingCheckpoints[u] = &e.Checkpoint
				}

				resolveAcks()

				c.log("STAGE[%d] Pending: %v Checkpoints: %v\n", c.stage, pending, len(pendingCheckpoints))
				if len(pendingCheckpoints) == cap {
					//in order to apply backpressure this channel needs to be nilld but right now it hangs after second pendingSuspendable
					pendingSuspendable = nil
					//stream.log("STAGE[%d] Applying backpressure, pending acks: %d\n", stream.stage, len(pendingCheckpoints))
				} else if len(pendingCheckpoints) > cap {
					panic(fmt.Errorf("illegal accumulator state, buffer size higher than %d", cap))
				}

			}

		}
		c.completed <- true
	}()
}

func (c *Context) log(f string, args ... interface{}) {
	if c.stage > 0 {
		log.Printf(f, args...)
	}
}
