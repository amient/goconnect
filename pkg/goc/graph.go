package goc

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"reflect"
	"syscall"
)

type Graph []*Context

func BuildGraph(connector Connector, pipeline *Pipeline) Graph {
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
	cases := make([]reflect.SelectCase, len(graphs)+1)
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	cases[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(sigterm)}
	for i, graph := range graphs {
		sources[i] = graph[0]
		RunGraph(graph)
		cases[i+1] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(sources[i].completed)}
	}

	runningGraphs := len(graphs)
	for {
		if chosen, value, _ := reflect.Select(cases); chosen == 0 {
			log.Printf("Caught signal %v: Cancelling\n", value.Interface())
			for _, source := range sources {
				if !source.closed {
					source.terminate <- true
				}
			}
		} else {
			runningGraphs--
			println(chosen, "completed, num running: ", runningGraphs)
			for i := len(graphs[chosen-1]) - 1; i >= 0; i-- {
				graphs[chosen-1][i].Close()
			}
			if runningGraphs == 0 {
				return
			}
		}

	}
}

func RunGraph(graph Graph) {
	for _, ctx := range graph {
		if ctx.isPassthrough {
			log.Printf("Initializing Passthru Stage %d %v\n", ctx.stage, reflect.TypeOf(ctx.fn))
		} else {
			log.Printf("Initializing Buffered Stage %d %v\n", ctx.stage, reflect.TypeOf(ctx.fn))

		}

		go func(context *Context) {
			switch fn := context.fn.(type) {
			case Root:
				fn.Do(context)
			case Transform:
				fn.Run(context.up.Attach(), context)
			case ForEach:
				fn.Run(context.up.Attach(), context)
			case ElementWiseFn:
				for e := range context.up.Attach() {
					fn.Process(e, context)
				}
			case ForEachFn:
				for e := range context.up.Attach() {
					fn.Process(e)
				}
			case MapFn:
				for e := range context.up.Attach() {
					out := fn.Process(e)
					out.Stamp = e.Stamp
					out.Checkpoint = e.Checkpoint
					context.Emit(out)
				}
			case FilterFn:
				for e := range context.up.Attach() {
					if fn.Pass(e) {
						context.Emit(e)
					}
				}
			default:
				panic(fmt.Errorf("Unsupported Stage Type %q", reflect.TypeOf(fn)))
			}
			context.terminate <- true
		}(ctx)
	}

}
