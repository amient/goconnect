package goc

import (
	"fmt"
	"log"
	"reflect"
	"sync"
)
type Graph []*Edge

type Edge struct {
	Source *Context
	Dest   *Context
	Fn     Fn
}

func BuildGraph(connector Connector, pipeline *Pipeline) Graph {
	graph := make(Graph, len(pipeline.Streams))
	log.Printf("Applying pipline of %d stages to node %d", len(graph), connector.GetNodeID())
	for _, stream := range pipeline.Streams {
		dest := NewContext(connector)
		edge := Edge{Fn: stream.Fn, Dest: dest}
		if stream.Id > 0 {
			//println(stream.Up.Id, "->", stream.Id )
			edge.Source = graph[stream.Up.Id].Dest
		}
		graph[stream.Id] = &edge
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

func RunGraph(edges []*Edge) {
	w := sync.WaitGroup{}
	for _, e := range edges {
		w.Add(1)
		go func(fn Fn, input <- chan *Element, context *Context) {
			switch stage := fn.(type) {
			case Root:
				stage.Do(context)
			case Transform:
				stage.Run(input, context)
			case ForEach:
				stage.Run(input, context)
			case ElementWiseFn:
				for e := range input {
					stage.Process(e, context)
				}
			case ForEachFn:
				for e := range input {
					stage.Process(e)
				}
			case MapFn:
				for e := range input {
					out := stage.Process(e)
					out.Stamp = e.Stamp
					out.Checkpoint = e.Checkpoint
					context.Emit(out)
				}
			default:
				t := reflect.TypeOf(stage)
				if t.Kind() == reflect.Func && t.NumIn() == 1 && t.NumOut() == 1 {
					//simple mapper function
					v := reflect.ValueOf(stage)
					for e := range input {
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
		}(e.Fn, e.Source.Attach(), e.Dest)
	}
	w.Wait()

}
