package network

import (
	"github.com/amient/goconnect/pkg/goc"
	"log"
	"sync"
)


func Runner(pipeline *goc.Pipeline, addrs ...string) {

	localNodes := JoinCluster(addrs...)

	graphs := make([]goc.Graph, len(localNodes))
	for _, node := range localNodes {
		graphs = append(graphs, goc.BuildGraph(node, pipeline))
	}

	log.Println("Running all graphs")
	goc.RunGraphs(graphs)

	log.Println("Run graphs completed")
	for _, node := range localNodes {
		node.server.Close()
	}


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

