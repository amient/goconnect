package network

import (
	"github.com/amient/goconnect/pkg/goc"
	"github.com/amient/goconnect/pkg/goc/io"
	"log"
	"sync"
	"sync/atomic"
	"testing"
)

func TestSomething(t *testing.T) {

	var nodes = []string{"127.0.0.1:19001", "127.0.0.1:19002"}

	instances := make([]*Instance, 0)
	for _, node := range nodes {
		if instance, err := NewInstance(node, nodes); err != nil {
			log.Println(err)
		} else {
			instances = append(instances, instance)
		}
	}

	//coordinate the cluster
	log.Printf("Resolving Nodes (%d running)", len(instances))
	for _, instance := range instances {
		instance.Resolve(nodes)
	}

	//declare pipelines
	log.Println("Declaring Pipelines")
	for _, instance := range instances {
		s1 := instance.Apply(nil, RootStage)
		s2 := instance.Apply(s1, NetRoundRobin)
		s3 := instance.Apply(s2, NetMerge)
		instance.Apply(s3, SinkStage)
	}

	//materialize
	log.Println("Materializing Pipelines")
	for _, instance := range instances {
		instance.Materialize()
	}

	//run
	log.Println("Running all instances")
	group := new(sync.WaitGroup)
	for _, instance := range instances {
		group.Add(1)
		go func(instance *Instance) {
			instance.Run()
			group.Done()
		}(instance)
	}

	group.Wait()


}

func NewInstance(node string, nodes []string) (*Instance, error) {
	server := NewServer(node)
	if err := server.Start(); err != nil {
		return nil, err
	} else {
		return &Instance{
			server: server,
			nodes:  nodes,
			graph:  make([]*Edge, 0, 20),
		}, nil
	}
}

type StageConstructor func(*Instance) Stage

type Instance struct {
	server     *Server
	nodes      []string
	graph      []*Edge
	receiverId int32
}

type Edge struct {
	from      chan *goc.Element
	to        *Stage
	collector *Collector
}

func (instance *Instance) Resolve(nodes []string) {
	for i, node := range nodes {
		s := NewSender(node, 0).Start()
		s.SendId(i, instance.server)
	}
	<-instance.server.assigned
}

func (instance *Instance) Apply(up chan *goc.Element, f StageConstructor) chan *goc.Element {
	collector := NewCollector()
	stage := f(instance)
	instance.graph = append(instance.graph, &Edge{up, &stage, collector})
	return collector.down
}

func (instance *Instance) Materialize() {
	for _, edge := range instance.graph {
		(*edge.to).Materialize()
	}
}

func (instance *Instance) Run() {
	stages := sync.WaitGroup{}
	for _, edge := range instance.graph {
		stages.Add(1)
		go func(edge *Edge){
			(*edge.to).Run(edge.from, edge.collector)
			edge.collector.Close()
			stages.Done()
		}(edge)
	}
	stages.Wait()
	instance.server.Close()
}

func (instance *Instance) NewReceiverID() uint16 {
	return uint16(atomic.AddInt32(&instance.receiverId,1))
}


func NewCollector() *Collector {
	return &Collector{
		down: make(chan *goc.Element, 1),
		up:   make(chan *goc.Stamp, 1),
	}
}

type Collector struct {
	down chan *goc.Element
	up   chan *goc.Stamp
}

func (c *Collector) Emit(element *goc.Element) {
	c.down <- element
}

func (c *Collector) Ack(element *goc.Element) {
	c.up <- &element.Stamp
}

func (c *Collector) Close() {
	close(c.down)
}

type Stage interface {
	Run(input <-chan *goc.Element, collector *Collector)
	Materialize()
}

func NewVoid() Stage {
	return &Void{}
}

type Void struct{}

func (v *Void) Run(input <-chan *goc.Element, collector *Collector) {}
func (v *Void) Materialize() {}

func RootStage(instance *Instance) Stage {
	if instance.server.NodeId != 1 {
		//runs on the first assigned node
		return NewVoid()
	}
	return &rootStage{data: []string{"aaa", "bbb", "ccc"}}
}

type rootStage struct {
	data []string
}

func (r *rootStage) Materialize() {}

func (r *rootStage) Run(input <-chan *goc.Element, collector *Collector) {
	intercept := make(chan *goc.Element, 1)
	go func() {
		io.From(r.data).Run(intercept)
	}()

	//autoi := uint64(0)
	for e := range intercept {
		//autoi += 1
		//e.Stamp.Unix = time.Now().Unix()
		//e.Stamp.Lo = autoi
		//e.Stamp.Hi = autoi
		//collector.Emit(e)
		collector.Emit(&goc.Element{Value: []byte(e.Value.(string))})
	}
}


type netRoundRobin struct {
	instance *Instance
	send     []*Sender
	recv     *Receiver
}

func NetRoundRobin(instance *Instance) Stage {
	return &netRoundRobin{
		instance: instance,
		recv:     instance.server.NewReceiver(instance.NewReceiverID(), "round-robin"),
	}
}

func (n *netRoundRobin) Materialize() {
	n.send = make([]*Sender, len(n.instance.nodes))
	for i, addr := range n.instance.nodes {
		n.send[i] = NewSender(addr, n.recv.ID).Start()
	}
}

func (n *netRoundRobin) Run(input <-chan *goc.Element, collector *Collector) {
	go func() {
		i := 0
		for e := range input {
			n.send[i].SendDown(e)
			if i += 1; i >= len(n.send) {
				i = 0
			}
		}
		for _, s := range n.send {
			s.Close()
		}
	}()

	for e := range n.recv.Down() {
		collector.Emit(e)
	}
}

type netMerge struct {
	send *Sender
	recv *Receiver
}

func NetMerge(instance *Instance) Stage {
	LastNodeID := len(instance.nodes)
	ReceiverId := instance.NewReceiverID()
	addr := instance.nodes[LastNodeID-1]
	var recv *Receiver
	if instance.server.NodeId == LastNodeID {
		//log.Printf("merge send --FROM-- %v --TO-- %v", instance.server.addr, addr)
		recv = instance.server.NewReceiver(ReceiverId,"merge")
	}
	return &netMerge{
		recv: recv,
		send: NewSender(addr, ReceiverId),
	}
}

func (n *netMerge) Materialize() {
	n.send.Start()
}

func (n *netMerge) Run(input <-chan *goc.Element, collector *Collector) {

	go func() {
		for e := range input {
			n.send.SendDown(e)
		}
		n.send.Close()
	}()

	if n.recv != nil {
		for e := range n.recv.Down() {
			collector.Emit(e)
		}
	}

}

func SinkStage(instance *Instance) Stage {
	return &sinkStage{}
}

type sinkStage struct{}

func (s *sinkStage) Materialize() {}

func (s *sinkStage) Run(input <-chan *goc.Element, collector *Collector) {
	for e := range input {
		log.Println(">>>", string(e.Value.([]byte)), e.Stamp, e.Checkpoint)
		//TODO collector.Ack(e)
	}
}
