package goc

import (
	"fmt"
	"io"
	"log"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

type Receiver interface {
	Elements() <-chan *Element
	Ack(upstreamNodeId uint16, uniq uint64) error
}

type Sender interface {
	Acks() <-chan uint64
	Send(element *Element)
	Eos()
	Close() error
}

type Connector interface {
	GetNodeID() uint16
	MakeReceiver(stage uint16) Receiver
	GetNumPeers() uint16
	NewSender(nodeId uint16, stage uint16) Sender
	GenerateStageID() uint16
}

func NewContext(connector Connector, def *Def) *Context {
	context := Context{
		stage:     connector.GenerateStageID(),
		connector: connector,
		def:       def,
		output:    make(chan *Element),
		terminate: make(chan bool),
		completed: make(chan bool),
	}
	if def.Fn == nil {
		context.isPassthrough = true
	} else {
		context.commitable, context.isCommitable = def.Fn.(Commitable)
		_, isMapFn := def.Fn.(MapFn)
		_, isTransform := def.Fn.(Transform)
		_, isForEach := def.Fn.(ForEach)
		_, isFilterFn := def.Fn.(FilterFn)
		_, isForEachFn := def.Fn.(ForEachFn)
		context.isPassthrough = !isTransform && (isMapFn || isForEachFn || isForEach || isFilterFn)
	}
	if !context.isPassthrough {
		context.acks = make(chan uint64, 10000) //TODO configurable ack capacity
	}
	return &context
}

type Context struct {
	Emit      func(element *Element)
	up        *Context
	stage     uint16
	output    chan *Element
	connector Connector
	receiver  Receiver
	def       *Def
	//fn             Fn
	isPassthrough  bool
	isCommitable   bool
	commitable     Commitable
	autoi          uint64
	highestPending uint64
	highestAcked   uint64
	acks           chan uint64
	terminate      chan bool
	terminating    bool
	closed         bool
	completed      chan bool
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
func (c *Context) Ack(uniq uint64) {
	if c.isPassthrough {
		c.up.Ack(uniq)
	} else {
		if c.acks == nil {
			panic("acks already closed")
		}
		c.acks <- uniq
	}
}

func (c *Context) Terminate() {
	c.terminate <- true
}

func (c *Context) Start() {

	pending := make(chan Pending)

	c.Emit = func(element *Element) {

		if element.Stamp.Unix == 0 {
			element.Stamp.Unix = time.Now().Unix()
		}

		if c.up == nil {
			element.Stamp.Uniq = atomic.AddUint64(&c.autoi, 1)
		}

		if !c.isPassthrough {
			pending <- Pending{
				Uniq:           element.Stamp.Uniq,
				Checkpoint:     &element.Checkpoint,
				UpstreamNodeId: element.FromNodeId,
			}
		}

		if element.Checkpoint.Data == nil {
			//	c.log("OUTPUT stamp: %v", element.Stamp)
		} else {
			//	c.log("OUTPUT stamp: %v checkpoint: %v", element.Stamp, element.Checkpoint)
		}
		element.ack = c.Ack
		if element.Stamp.Uniq > c.highestPending {
			c.highestPending = element.Stamp.Uniq
		}
		c.output <- element

	}

	verticalGroupStart := sync.WaitGroup{}
	verticalGroupFinish := sync.WaitGroup{}
	for i := 1; i <= c.def.maxVerticalParallelism; i++ {
		verticalGroupStart.Add(1)
		verticalGroupFinish.Add(1)
		go c.runFn(&verticalGroupStart, &verticalGroupFinish)
	}

	verticalGroupStart.Wait()

	go func() {
		verticalGroupFinish.Wait()
		close(c.output)
		if c.isPassthrough {
			c.close()
		} else {
			//c.log("Terminate - await completion")
			c.Terminate()
		}
	}()


	if !c.isPassthrough {
		var commits chan Watermark
		var commitRequests chan bool
		//c.log("COMMITABLE=%v", c.isCommitable)
		if c.isCommitable {
			commits = make(chan Watermark)
			commitRequests = make(chan bool)
			go c.committer(commitRequests, commits)
		}
		go c.checkpointer(100000, pending, commitRequests, commits) //TODO configurable capacity
	}

}

func (c *Context) runFn(starting *sync.WaitGroup, finishing *sync.WaitGroup) {
	starting.Done()
	switch fn := c.def.Fn.(type) {
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
	case GroupFn:
		maxNumElements := 5 //TODO this is only one king of trigger - defined by max num elements
		buffer := make([]uint64, 0, maxNumElements)
		trigger := func() {
			swap := buffer
			buffer = make([]uint64, 0, maxNumElements)
			for _, e := range fn.Trigger() {
				e.ack = func(uniq uint64) {
					for _, u := range swap {
						c.Ack(u)
					}
				}
				c.output <- e
			}
		}
		for e := range c.up.output {
			fn.Process(e)
			buffer = append(buffer, e.Stamp.Uniq)
			if len(buffer) == cap(buffer) {
				trigger()
			}
		}
		trigger()

	case ForEachFn:
		for e := range c.up.output {
			fn.Process(e)
		}
	case MapFn:
		for e := range c.up.output {
			out := fn.Process(e)
			out.Stamp = e.Stamp
			c.Emit(out)
		}
	case FilterFn:
		for e := range c.up.output {
			if fn.Pass(e) {
				c.Emit(e)
			} else {
				e.Ack()
			}
		}
	default:
		panic(fmt.Errorf("unsupported Stage Type %q", reflect.TypeOf(fn)))
	}
	finishing.Done()
}

func (c *Context) checkpointer(cap int, pending chan Pending, commitRequests chan bool, commits chan Watermark) {
	pendingSuspendable := pending
	pendingCheckpoints := make(map[uint64]*Pending, cap)
	acked := make(map[uint64]bool, cap)
	watermark := make(Watermark)
	pendingCommitReuqest := false

	doCommit := func() {
		if len(watermark) > 0 {
			pendingCommitReuqest = false
			if c.isCommitable {
				//c.log("Commit Checkpoint: %v", watermark)
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
					//c.log("Completed - closing")
					close(pending)
					close(c.acks)
					c.acks = nil
					c.close()
				}
			} else {
				//c.log("Awaiting Completion highestPending: %d, highestAcked: %d", c.highestPending, c.highestAcked)
			}
		}
	}

	resolveAcks := func(uniq uint64, action string) {
		//resolve acks <> pending
		if p, exists := pendingCheckpoints[uniq]; exists {
			if _, alsoExists := acked[uniq]; alsoExists {
				delete(pendingCheckpoints, uniq)
				delete(acked, uniq)
				watermark[p.Checkpoint.Part] = p.Checkpoint.Data
				if p.UpstreamNodeId > 0 {
					c.receiver.Ack(p.UpstreamNodeId, p.Uniq)
				} else if c.up != nil && !c.isCommitable {
					//commitables have to ack their upstream manually
					c.up.Ack(uniq)
				}

				//c.log(action+"(%v) RESOLVED PART %d DATA %v PENDING %d pendingCommitReuqest = %v\n", uniq, p.Checkpoint.Part, p.Checkpoint.Data, len(pendingCheckpoints), pendingCommitReuqest)
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
		case uniq := <-c.acks:
			if uniq > c.highestAcked {
				c.highestAcked = uniq
			}
			acked[uniq] = true
			resolveAcks(uniq, "ACK")

		case p := <-pendingSuspendable:
			pendingCheckpoints[p.Uniq] = &p

			resolveAcks(p.Uniq, "EMIT")

			if len(pendingCheckpoints) == cap {
				//in order to apply backpressure this channel needs to be nilld but right now it hangs after second pendingSuspendable
				pendingSuspendable = nil
				//c.log("STAGE[%d] Applying backpressure, pending acks: %d\n", c.stage, len(pendingCheckpoints))
			} else if len(pendingCheckpoints) > cap {
				panic(fmt.Errorf("illegal accumulator state, buffer size higher than %d", cap))
			}

		}
	}
}

func (c *Context) committer(commitRequests chan bool, commits chan Watermark) {
	commitRequests <- true
	defer close(commits)
	defer close(commitRequests)
	for !c.closed {
		select {
		case checkpoint, ok := <-commits:
			if ok {
				if c.commitable != nil {
					c.commitable.Commit(checkpoint)
				}
				commitRequests <- true
			}
		}
	}
}

func (c *Context) close() {
	if ! c.closed {
		c.completed <- true
		c.closed = true
		if fn, ok := c.def.Fn.(io.Closer); ok {
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
