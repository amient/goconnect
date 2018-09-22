package network

import (
	"github.com/amient/goconnect/pkg/goc"
	"log"
	"sync"
)


func Runner(pipeline *goc.Pipeline, addrs ...string) {

	nodes := JoinCluster(addrs...)

	log.Println("Applying pipeline to all nodes")
	for _, node := range nodes {
		node.Deploy(pipeline)
	}

	RunLocal(nodes)
	//FIXME this must terminate only when acks have been processed

}

func JoinCluster(nodes ...string) []*Node {
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

	return instances
}

func RunLocal(nodes []*Node) {
	//run
	log.Println("Running all nodes")
	group := new(sync.WaitGroup)
	for _, node := range nodes {
		group.Add(1)
		go func(instance *Node) {
			instance.Run()
			group.Done()
		}(node)
	}
	group.Wait()
}
