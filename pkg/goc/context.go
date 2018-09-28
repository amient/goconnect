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
	Ack(Stamp) error
}

type Sender interface {
	Acks() <-chan *Stamp
	Send(element *Element)
	Close() error
}

type Connector interface {
	GetNodeID() uint16
	MakeReceiver(stage uint16) Receiver
	GetNumPeers() uint16
	NewSender(nodeId uint16, stage uint16) Sender
	GenerateStageID() uint16
}

func NewContext(connector Connector, fn Fn) *Context {
	context := Context{
		stage:          connector.GenerateStageID(),
		connector:      connector,
		fn:             fn,
		terminate:      make(chan bool),
		completed:      make(chan bool),
	}
	if fn == nil {
		context.isPassthrough = true
	} else {
		context.commitable, context.isCommitable = fn.(Commitable)
		_, isMapFn := fn.(MapFn)
		_, isTransform := fn.(Transform)
		_, isForEach := fn.(ForEach)
		_, isForEachFn := fn.(ForEachFn)
		context.isPassthrough = !isTransform && (isMapFn || isForEachFn || isForEach)
	}
	return &context
}

type Context struct {
	Emit           func(element *Element)
	up             *Context
	stage          uint16
	output         chan *Element
	connector      Connector
	receiver       Receiver
	fn             Fn
	isPassthrough  bool
	isCommitable   bool
	commitable     Commitable
	autoi          uint64
	highestPending uint64
	highestAcked   uint64
	acks           chan *Stamp
	terminate      chan bool
	terminating    bool
	closed         bool
	completed      chan bool
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

func (c *Context) GetReceiver() Receiver {
	c.isCommitable = true
	if c.receiver == nil {
		c.receiver = c.connector.MakeReceiver(c.stage)
	}
	return c.receiver
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
func (c *Context) Ack(stamp *Stamp) {
	if c.isPassthrough {
		c.up.Ack(stamp)
	} else {
		c.acks <- stamp
	}
}

func (c *Context) Terminate() {
	if c.isPassthrough {
		c.log("Terminate - closing")
		c.close()
	} else {
		c.log("Terminate - await completion")
		c.terminate <- true
	}
}

func (c *Context) Start() {

	c.output = make(chan *Element)
	pending := make(chan *Element)

	c.Emit = func(element *Element) {
		element.Stamp.AddTrace(c.GetNodeID())
		//initial stamping of elements
		if !element.Stamp.Valid() {
			element.Stamp.Uniq = atomic.AddUint64(&c.autoi, 1)
		}
		if element.Stamp.Unix == 0 {
			element.Stamp.Unix = time.Now().Unix()
		}
		//checkpointing
		element.ack = c.Ack
		if element.Stamp.Uniq > c.highestPending {
			c.highestPending = element.Stamp.Uniq
		}

		c.log("OUTPUT stamp: %v checkpoint: %v", element.Stamp, element.Checkpoint)

		if !c.isPassthrough {
			pending <- element
		}

		c.output <- element

	}

	go c.runFn()

	if !c.isPassthrough {
		c.acks = make(chan *Stamp, 10000) //TODO configurable ack capacity
		var commits chan Watermark
		var commitRequests chan bool
		if c.isCommitable {
			commits = make(chan Watermark)
			commitRequests = make(chan bool)
			go func() {
				commitRequests <- true
				defer close(commits)
				defer close(commitRequests)
				for !c.closed {
					select {
					case checkpoint, ok := <-commits:
						if ok {
							if c.receiver != nil {
								//TODO built-in checkpoint behaviour for network receivers
								for _, stamp := range checkpoint {
									c.receiver.Ack(stamp.(Stamp))
								}
							}
							if c.commitable != nil {
								c.commitable.Commit(checkpoint)
							}
							commitRequests <- true
						}
					}
				}
			}()
		}

		go c.checkpointer(100000, pending, commitRequests, commits) //TODO configurable capacity

	}

}

func (c *Context) runFn() {
	defer close(c.output)
	defer c.Terminate()
	switch fn := c.fn.(type) {
	case Root:
		fn.Do(c)
	case Transform:
		fn.Run(c.up.output, c)
	case ForEach:
		fn.Run(c.up.output, c)

	case ElementWiseFn:
		for e := range c.up.output {
			fn.Process(e, c)
		}
	case ForEachFn:
		for e := range c.up.output {
			fn.Process(e)
		}
	case MapFn:
		for e := range c.up.output {
			out := fn.Process(e)
			out.Stamp = e.Stamp
			out.Checkpoint = e.Checkpoint
			c.Emit(out)
		}
	case FilterFn:
		for e := range c.up.output {
			if fn.Pass(e) {
				c.Emit(e)
			}
		}
	default:
		panic(fmt.Errorf("unsupported Stage Type %q", reflect.TypeOf(fn)))
	}
}

func (c *Context) checkpointer(cap int, pending chan *Element, commitRequests chan bool, commits chan Watermark) {
	pendingSuspendable := pending
	pendingCheckpoints := make(map[uint64]*Checkpoint, cap)
	acked := make(map[uint64]bool, cap)
	watermark := make(Watermark)
	pendingCommitReuqest := false

	doCommit := func() {
		if len(watermark) > 0 {
			pendingCommitReuqest = false
			if c.isCommitable {
				c.log("Commit Checkpoint: %v", watermark)
				commits <- watermark
			}
			watermark = make(map[int]interface{})
		}
	}

	maybeTerminate := func() {
		if c.terminating {
			if len(watermark) == 0 && c.highestPending == c.highestAcked {
				clean := (pendingCommitReuqest || !c.isCommitable) && len(pendingCheckpoints) == 0
				if clean {
					c.log("Completed - closing")
					close(pending)
					close(c.acks)
					c.acks = nil
					c.close()
				}
			}
		}
	}

	resolveAcks := func(uniq uint64, action string) {
		//resolve acks <> pending
		if p, exists := pendingCheckpoints[uniq]; exists {
			if _, alsoExists := acked[uniq]; alsoExists {
				delete(pendingCheckpoints, uniq)
				delete(acked, uniq)
				watermark[p.Part] = p.Data
				c.log(action+"(%v) RESOLVED PART %d DATA %v PENDING %d pendingCommitReuqest = %v\n", uniq, p.Part, p.Data, len(pendingCheckpoints), pendingCommitReuqest)
			}
		}

		if len(pendingCheckpoints) < cap && pendingSuspendable == nil {
			//release the backpressure after capacity is freed
			pendingSuspendable = pending
		}

		//commit if pending commit request or not commitable in which case commit requests are never fired
		if pendingCommitReuqest || !c.isCommitable {
			doCommit()
		}

		//c.log("%s(%d) Pending: %v Acked: %v isCommitable=%v, commitRequested=%v\n", action, uniq, len(pendingCheckpoints), acked, c.isCommitable, pendingCommitReuqest)

		maybeTerminate()
	}

	for !c.closed {
		select {
		case <-c.terminate:
			c.terminating = true
			maybeTerminate()
		case <-commitRequests:
			pendingCommitReuqest = true
			doCommit()
			maybeTerminate()
		case stamp := <-c.acks:
			if stamp.Uniq > c.highestAcked {
				c.highestAcked = stamp.Uniq
			}
			acked[stamp.Uniq] = true
			resolveAcks(stamp.Uniq, "ACK")

			if c.up != nil && !c.isCommitable {
				//commitables have to ack their upstream manually
				c.up.Ack(stamp)
			}

		case e := <-pendingSuspendable:
			if c.closed {
				panic(fmt.Errorf("STAGE[%d] Illegal Pending %v", c.stage, e))
			}

			pendingCheckpoints[e.Stamp.Uniq] = &e.Checkpoint

			resolveAcks(e.Stamp.Uniq, "EMIT")

			if len(pendingCheckpoints) == cap {
				//in order to apply backpressure this channel needs to be nilld but right now it hangs after second pendingSuspendable
				pendingSuspendable = nil
				//stream.log("STAGE[%d] Applying backpressure, pending acks: %d\n", stream.stage, len(pendingCheckpoints))
			} else if len(pendingCheckpoints) > cap {
				panic(fmt.Errorf("illegal accumulator state, buffer size higher than %d", cap))
			}

		}
	}
}

func (c *Context) close() {
	if ! c.closed {
		c.completed <- true
		c.closed = true
		if fn, ok := c.fn.(io.Closer); ok {
			if err := fn.Close(); err != nil {
				panic(err)
			}
		}

	}
}

func (c *Context) log(f string, args ... interface{}) {
	args2 := make([]interface{}, len(args)+2)
	args2[0] = c.GetNodeID()
	args2[1] = c.stage
	for i := range args {
		args2[i+2] = args[i]
	}
	if c.stage > 0 {
		log.Printf("NODE[%d] STAGE[%d] "+f, args2...)
	}
}
