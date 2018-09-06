package main

import (
	"github.com/amient/goconnect/pkg/goc"
	"github.com/amient/goconnect/pkg/goc/coder/gocxml"
	"github.com/amient/goconnect/pkg/goc/io"
	"github.com/amient/goconnect/pkg/goc/io/std"
	"strings"
	"time"
)

func main() {

	list := []string{
		"<name>Adam</name>",
		"<name>Alice</name>",
		"<name>Alex</name>",
		"<name>Bart</name>",
		"<name>Bob</name>",
		"<name>Brittney</name>",
		"<name>Brenda</name>",
		"<name>Chad</name>",
	}

	//root source of text elements
	messages := io.FromList(list)

	//decode strings to xml by applying a coder
	xmls := messages.Apply(gocxml.StringDecoder())

	//extract names with custom Map fn
	extracted := xmls.Map(func(input gocxml.Node) string {
		return input.Children()[0].Children()[0].Text()
	})

	//remove all names containing letter 'B' with custom Filter fn
	filtered := extracted.Filter(func(input string) bool {
		return !strings.Contains(input, "B")
	})

	//do total aggregation using custom Transform fn
	total := filtered.Transform(func(input chan string, output chan int) {
		l := 0
		for b := range input {
			l += len(b)
		}
		output <- l
	})

	//output the aggregation result by applying a general StdOutSink transform
	sink1 := total.Apply(std.StdOutSink())

	////////////////////////////////////////////////

	goc.RunPipeline(time.Second, sink1)

}

//TODO next step is adding checkpointer with pipeline options of optimistic or pessimistic one
//TODO maybe forking outputs is possible without breaking guarantees
//TODO probably merging from multiple inputs would be possible if all of them support optimistic commit
//sink2 := xmls.Transform(FnEW(func(input xmlc.Node) error {
//	fmt.Println(sink2)
//	return nil
//}))


