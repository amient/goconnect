package goc

import (
	"log"
	"os"
	"os/signal"
	"reflect"
	"syscall"
)

type Graph []*Context

func ConnectStages(connector Connector, pipeline *Pipeline) Graph {
	graph := make(Graph, len(pipeline.Streams))
	log.Printf("Applying pipline of %d stages to node %d", len(graph), connector.GetNodeID())
	for _, stream := range pipeline.Streams {
		graph[stream.Id] = NewContext(connector, stream.Fn)
		if stream.Id > 0 {
			graph[stream.Id].up = graph[stream.Up.Id]
		}
	}
	return graph
}

func RunGraphs(graphs ...Graph) {
	//this method assumes a single source in each graph
	sources := make([]*Context, len(graphs))
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	cases := []reflect.SelectCase{
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(sigterm)},
	}
	runningStages := 0
	for i, graph := range graphs {
		sources[i] = graph[0]
		for _, ctx := range graph {
			if ctx.isPassthrough {
				log.Printf("Context[%d] Passthru Stage %d %v\n", runningStages, ctx.stage, reflect.TypeOf(ctx.fn))
			} else {
				log.Printf("Context[%d] Buffered Stage %d %v\n", runningStages, ctx.stage, reflect.TypeOf(ctx.fn))
			}
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.completed)})
			ctx.Start()
			runningStages++
		}
	}

	for {
		if chosen, value, _ := reflect.Select(cases); chosen == 0 {
			log.Printf("Caught signal %v: Cancelling\n", value.Interface())
			for _, source := range sources {
				if !source.closed {
					source.Terminate()
				}
			}
		} else {
			runningStages--
			log.Printf("A Stage Completed [%v], num running stages: %v", chosen, runningStages)
			if runningStages == 0 {
				return
			}
		}

	}
}

