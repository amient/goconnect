package main

import (
	"github.com/amient/goconnect/pkg/goc"
	"github.com/amient/goconnect/pkg/goc/io"
	"github.com/amient/goconnect/pkg/goc/network"
	"github.com/amient/goconnect/pkg/goc/network/prototype"
	"github.com/amient/goconnect/pkg/goc/util"
	"log"
	"strings"
	"sync"
)

func main() {

	var nodes = []string{"127.0.0.1:19001", "127.0.0.1:19002"}

	//TODO declare pipeline prior to deployment
	//r1 := RootStage([]string{"aaa", "bbb", "ccc"})
	//pipe := new(prototype.Pipe)
	//s1 := pipe.Root(RootStage([]string{"aaa", "bbb", "ccc"}))
	//s2 := s1.Apply(new(NetRoundRobin))
	//s3 := s2.Apply(func(input []byte) []byte)
	//s4 := s3.Apply(new(NetMergeOrdered))
	//s4.Apply(new(stdOutSink))
	//pipe.Run(nodes)


	instances := make([]*prototype.Node, 0)
	for _, node := range nodes {
		if instance, err := prototype.NewNode(node, nodes); err != nil {
			log.Println(err)
		} else {
			instances = append(instances, instance)
		}
	}

	//join the cluster
	log.Printf("Joining Cluser of %d nodes with %d running in this process", len(nodes), len(instances))
	cluster := sync.WaitGroup{}
	for _, instance := range instances {
		cluster.Add(1)
		go func(node *prototype.Node) {
			node.Join(nodes)
			cluster.Done()
		}(instance)
	}
	cluster.Wait()

	log.Println("Declaring Pipelines")
	for _, instance := range instances {
		s1 := instance.Apply(nil, RootStage([]string{"aaa", "bbb", "ccc"}))
		s2 := instance.Apply(s1, new(NetRoundRobin))
		s3 := instance.Apply(s2, new(UpperCase))
		s4 := instance.Apply(s3, new(NetMergeOrdered))
		instance.Apply(s4, new(stdOutSink))
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
		go func(instance *prototype.Node) {
			instance.Run()
			group.Done()
		}(instance)
	}

	group.Wait()

}

type rootStage struct {
	assigned bool
	data     []string
}

func RootStage(data []string) prototype.Stage {
	return &rootStage{data: data}
}

func (r *rootStage) Initialize(instance *prototype.Node) {
	//runs on the first assigned node
	r.assigned = instance.GetNodeID() == 1
}

func (r *rootStage) Materialize() {}

func (r *rootStage) Run(input <-chan *goc.Element, collector *prototype.Collector) {
	if r.assigned {
		intercept := make(chan *goc.Element, 1)
		go func() {
			io.From(r.data).Run(intercept)
		}()

		for e := range intercept {
			e.Value = []byte(e.Value.(string))
			collector.Emit(e)
		}
	}
}

type NetRoundRobin struct {
	instance *prototype.Node
	send     []*network.Sender
	recv     *network.Receiver
}

func (n *NetRoundRobin) Initialize(node *prototype.Node) {
	n.instance = node
	n.recv = node.GetReceiver("round-robin")
	n.send = make([]*network.Sender, node.NumPeers())
	for i, addr := range node.GetPeers() {
		n.send[i] = network.NewSender(addr, n.recv.ID)
	}
}

func (n *NetRoundRobin) Materialize() {
	for _, s := range n.send {
		if  err := s.Start(); err != nil {
			panic(err)
		}
	}
}

func (n *NetRoundRobin) Run(input <-chan *goc.Element, collector *prototype.Collector) {
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

type UpperCase struct{}
func (t *UpperCase) Initialize(instance *prototype.Node) {}
func (t *UpperCase) Materialize()                        {}
func (t *UpperCase) Run(input <-chan *goc.Element, collector *prototype.Collector) {
	for e := range input {
		collector.Emit(&goc.Element{
			Stamp: e.Stamp,
			Value: []byte(strings.ToUpper(string(e.Value.([]byte)))),
		})
	}
}


type NetMergeOrdered struct {
	send *network.Sender
	recv *network.Receiver
	mergeOnThisNode bool
}

func (n *NetMergeOrdered) Initialize(node *prototype.Node) {
	LastNodeID := node.NumPeers()
	n.mergeOnThisNode = node.GetNodeID() == LastNodeID
	addr := node.GetPeer(LastNodeID)
	n.send = network.NewSender(addr, node.GetAllocatedReceiverID())
	if n.mergeOnThisNode {
		//log.Printf("merge send --FROM-- %v --TO-- %v", node.server.addr, addr)
		n.recv = node.GetReceiver("merge")
	}

}

func (n *NetMergeOrdered) Materialize() {
	n.send.Start()
}

func (n *NetMergeOrdered) Run(input <-chan *goc.Element, collector *prototype.Collector) {
	go func() {
		for e := range input {
			n.send.SendDown(e)
		}
		n.send.Close()
	}()


	if n.mergeOnThisNode {
		buf := util.NewOrderedElementSet(100) // TODO auto-configure and grow the buffer
		for e := range n.recv.Down() {
			buf.AddElement(e, collector)
		}
	}

}


type stdOutSink struct{}

func (s *stdOutSink) Initialize(instance *prototype.Node) {}
func (s *stdOutSink) Materialize()                        {}
func (s *stdOutSink) Run(input <-chan *goc.Element, collector *prototype.Collector) {
	for e := range input {
		println(string(e.Value.([]byte)), e.Stamp.String())
		//collector.Ack(e)
		//log.Println("acked")
	}
}
