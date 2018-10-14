package network

import (
	"github.com/amient/goconnect/pkg/goc"
	"log"
	"strings"
	"sync"
	"time"
)

func Runner(pipeline *goc.Pipeline, addrLists ...string) {

	addrs := make([]string, 0)
	for _, list := range addrLists {
		addrs = append(addrs, strings.Split(list, ",")...)
	}

	localNodes := JoinCluster(addrs...)

	graphs := make([]goc.Graph, len(localNodes))
	for i, node := range localNodes {
		graphs[i] = goc.ConnectStages(node, pipeline)
	}

	start := time.Now()
	log.Println("Running all graphs")
	goc.RunGraphs(graphs...)

	log.Printf("All stages completed in %f0.0 s", time.Now().Sub(start).Seconds())
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

	if len(instances) == 0 {
		panic("no instances assigned")
	}

	//join the cluster
	log.Printf("Joining Cluser of %d nodes with %d running in this process", len(nodes), len(instances))
	join := sync.WaitGroup{}
	for _, instance := range instances {
		join.Add(1)
		go func(instance *Node) {
			instance.Join(nodes)
			join.Done()
		}(instance)
	}
	join.Wait()

	return instances
}
