package prototype

import (
	"github.com/amient/goconnect/pkg/goc"
	"log"
	"sync"
)

type PipeEdge struct {
	from <-chan *goc.Element
	to   *Stage
}

type Pipe struct {
	edges []*PipeEdge
}

func NewPipe() *Pipe {
	return &Pipe{
		edges: make([]*PipeEdge, 0, 10),
	}
}

//func (pipe *Pipe) Root(stage *Stage) chan<- *goc.Element {
//	pipe.edges = append(pipe.edges, &PipeEdge{
//		src: nil,
//		stage: stage,
//	})
//}

func (pipe *Pipe) Run(nodes []string, declarePipeline func(node *Node)) {

	//start all nodes that can listen on this host
	instances := make([]*Node, 0)
	for _, node := range nodes {
		if instance, err := NewNode(node, nodes); err != nil {
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
		go func(node *Node) {
			node.Join(nodes)
			cluster.Done()
		}(instance)
	}
	cluster.Wait()

	//apply pipeline definition
	log.Println("Declaring Pipelines")
	for _, instance := range instances {
		declarePipeline(instance)
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
