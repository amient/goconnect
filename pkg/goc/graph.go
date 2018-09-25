package goc

import (
	"fmt"
	"log"
	"reflect"
	"sync"
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

func RunGraphs(graphs []Graph) {
	group := new(sync.WaitGroup)
	for _, graph := range graphs {
		group.Add(1)
		go func(graph Graph) {
			RunGraph(graph)
			group.Done()
		}(graph)
	}
	group.Wait()
}

func RunGraph(graph Graph) {
	w := sync.WaitGroup{}
	for _, ctx := range graph {
		w.Add(1)
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
			default:
				t := reflect.TypeOf(fn)
				if t.Kind() == reflect.Func && t.NumIn() == 1 && t.NumOut() == 1 {
					//simple mapper function
					v := reflect.ValueOf(fn)
					for e := range context.up.Attach() {
						context.Emit(&Element{
							Stamp: e.Stamp,
							Value: v.Call([]reflect.Value{reflect.ValueOf(e.Value)})[0].Interface(),
						})
					}
				} else {
					panic(fmt.Errorf("Unsupported Stage Type %q", t))
				}
			}
			context.Close()
			w.Done()
		}(ctx)
	}
	w.Wait()

}
