package prototype

import (
	"github.com/amient/goconnect/pkg/goc"
	"github.com/amient/goconnect/pkg/goc/io"
	"github.com/amient/goconnect/pkg/goc/network"
	"log"
	"sync"
	"testing"
)

func TestSomviething(t *testing.T) {

	var nodes = []string{"127.0.0.1:19001", "127.0.0.1:19002"}

	instances := make([]*Node, 0)
	for _, node := range nodes {
		if instance, err := NewNode(node, nodes); err != nil {
			log.Println(err)
		} else {
			instances = append(instances, instance)
		}
	}

	//join the cluster
	log.Printf("Joining Cluser of % nodes with %d running in this process", len(nodes), len(instances))
	cluster := sync.WaitGroup{}
	for _, instance := range instances {
		cluster.Add(1)
		go func(node *Node) {
			node.Join(nodes)
			cluster.Done()
		}(instance)
	}
	cluster.Wait()


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
		go func(instance *Node) {
			instance.Run()
			group.Done()
		}(instance)
	}

	group.Wait()

}


type rootStage struct {
	data []string
}

func RootStage(instance *Node) Stage {
	if instance.server.NodeId != 1 {
		//runs on the first assigned node
		return NewVoid()
	}
	return &rootStage{data: []string{"aaa", "bbb", "ccc"}}
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
	instance *Node
	send     []*network.Sender
	recv     *network.Receiver
}

func NetRoundRobin(instance *Node) Stage {
	return &netRoundRobin{
		instance: instance,
		recv:     instance.server.NewReceiver(instance.NewReceiverID(), "round-robin"),
	}
}

func (n *netRoundRobin) Materialize() {
	n.send = make([]*network.Sender, len(n.instance.nodes))
	for i, addr := range n.instance.nodes {
		var err error
		if n.send[i], err = network.NewSender(addr, n.recv.ID).Start(); err != nil {
			panic(err)
		}
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
	send *network.Sender
	recv *network.Receiver
}

func NetMerge(instance *Node) Stage {
	LastNodeID := len(instance.nodes)
	mergeOnThisNode := instance.server.NodeId == LastNodeID
	ReceiverId := instance.NewReceiverID()
	addr := instance.nodes[LastNodeID-1]
	var recv *network.Receiver
	if mergeOnThisNode {
		//log.Printf("merge send --FROM-- %v --TO-- %v", instance.server.addr, addr)
		recv = instance.server.NewReceiver(ReceiverId, "merge")
	}
	return &netMerge{
		recv: recv,
		send: network.NewSender(addr, ReceiverId),
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

type sinkStage struct{}

func SinkStage(instance *Node) Stage {
	return &sinkStage{}
}

func (s *sinkStage) Materialize() {}

func (s *sinkStage) Run(input <-chan *goc.Element, collector *Collector) {
	for e := range input {
		log.Println(">>>", string(e.Value.([]byte)), e.Stamp, e.Checkpoint)
		//collector.Ack(e)
		//log.Println("acked")
	}
}
