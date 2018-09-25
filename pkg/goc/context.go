package goc

import (
	"fmt"
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
	GetReceiver(stage uint16) Receiver
	GetNumPeers() uint16
	NewSender(nodeId uint16, stage uint16) Sender
	GenerateStageID() uint16
}

func NewContext(connector Connector, fn Fn) *Context {
	stage := connector.GenerateStageID()
	return &Context{
		stage:     stage,
		connector: connector,
		fn:        fn,
		emits:     make(chan *Element, 1),
	}
}

type Context struct {
	stage          uint16
	connector      Connector
	fn             Fn
	up             *Context
	emits          chan *Element
	acks           chan *Stamp
	autoi          uint64
	pending        chan *Element
	terminate      chan bool
	terminating    bool
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
	//c.acks <- stamp
}

func (c *Context) Close() {
	close(c.emits)
}

func (c *Context) Attach() <-chan *Element {
	if c == nil {
		return nil
	}

	//go c.checkpointer()

	stampedOutput := make(chan *Element, 10)
	go func() {
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
			//c.log("STAGE[%d] Incoming %d", c.stage, element.Stamp)
			//c.pending <- element
			//}

			stampedOutput <- element
		}
	}()

	return stampedOutput
}

func (c *Context) checkpointer() {
	//var commitable Commitable
	var isCommitable bool = false //FIXME
	//if c.Fn == nil {
	//	stream.isPassthrough = true
	//} else {
	//	_, stream.isPassthrough = stream.Fn.(MapFn)
	//	commitable, isCommitable = stream.Fn.(Commitable)
	//}
	//
	//if stream.isPassthrough {
	//	log.Printf("Initializing Passthru Stage %d %v\n", stage, stream.Type)
	//	return
	//} else {
	//	log.Printf("Initializing Buffered Stage %d %v\n", stage, stream.Type)
	//}

	cap := 100000
	c.pending = make(chan *Element, 1000) //TODO this buffer is important so make it configurable but must be >= stream.cap
	c.acks = make(chan *Stamp, 10000)
	c.terminate = make(chan bool, 2)
	c.completed = make(chan bool, 1)
	commits := make(chan map[int]interface{})
	commitRequests := make(chan bool)

	pendingSuspendable := c.pending
	terminated := false
	pending := Stamp{}
	pendingCheckpoints := make(map[uint64]*Checkpoint, 10000) //TODO configurable capacity
	checkpoint := make(map[int]interface{})
	pendingCommitReuqest := false

	if isCommitable {
		go func() {
			commitRequests <- true
			for checkpoint := range commits {
				c.log("STAGE[%d] Commit Checkpoint: %v", c.stage, checkpoint)
				//commitable.Commit(checkpoint)
				commitRequests <- true
			}
		}()
	}

	doCommit := func() {
		if len(checkpoint) > 0 {
			pendingCommitReuqest = false
			//TODO if isCommitable {
			//commits <- checkpoint
			//}s
			checkpoint = make(map[int]interface{})
		}
	}

	maybeTerminate := func() {
		if c.terminating {
			if len(checkpoint) == 0 && c.highestPending == c.highestAcked {
				clean := (pendingCommitReuqest || !isCommitable) && !pending.Valid()
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
		for ; pending.Valid() && pendingCheckpoints[pending.Lo] != nil && pendingCheckpoints[pending.Lo].acked; {
			lo := pending.Lo
			pending.Lo += 1
			chk := pendingCheckpoints[lo]
			delete(pendingCheckpoints, lo)
			checkpoint[chk.Part] = chk.Data
			//c.log("STAGE[%d] DELETING PART %d DATA %v \n", c.stage, chk.Part, chk.Data)
		}
		if len(pendingCheckpoints) < cap && pendingSuspendable == nil {
			//release the backpressure after capacity is freed
			pendingSuspendable = c.pending
		}

		//commit if pending commit request or not commitable in which case commit requests are never fired
		if pendingCommitReuqest || !isCommitable {
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
				if c, ok := pendingCheckpoints[u]; ok {
					c.acked = true
				}
			}

			resolveAcks()

			c.log("STAGE[%d] ACK(%d) Pending: %v Checkpoints: %v\n", c.stage, stamp, pending.String(), pendingCheckpoints)
			//FIXME
			//if stream.Up != nil {
			//	stream.Up.ack(stamp)
			//}

		case e := <-pendingSuspendable:
			if terminated {
				panic(fmt.Errorf("STAGE[%d] Illegal Pending %v", c.stage, e))
			}
			pending.Merge(e.Stamp)
			for u := e.Stamp.Lo; u <= e.Stamp.Hi; u++ {
				pendingCheckpoints[u] = &e.Checkpoint
			}

			resolveAcks()

			//c.log("STAGE[%d] Pending: %v Checkpoints: %v\n", c.stage, pending, len(pendingCheckpoints))
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
}

func (c *Context) log(f string, args ... interface{}) {
	if c.stage > 0 {
		log.Printf(f, args...)
	}
}
