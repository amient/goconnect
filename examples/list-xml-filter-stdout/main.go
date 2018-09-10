package main

import (
	"github.com/amient/goconnect/pkg/goc"
	"github.com/amient/goconnect/pkg/goc/coder/gocxml"
	"github.com/amient/goconnect/pkg/goc/io"
	"github.com/amient/goconnect/pkg/goc/io/std"
	"strings"
	"time"
)

var data = []string{
	"<name>Adam</name>", "<name>Albert</name>", "<name>Alice</name>", "<name>Alex</name>",
	"<name>Bart</name>", "<name>Bob</name>", "<name>Brittney</name>", "<name>Brenda</name>",
	"<name>Cecilia</name>", "<name>Chad</name>", "<name>Elliot</name>", "<name>Wojtek</name>",
}

func main() {

	pipeline := goc.NewPipeline()

	//root source of text elements
	// TODO generated lists are one of the examples which must be coordinated and run on any one instance
	messages := pipeline.Root(io.Iterable(data))

	//decode strings to xml by applying a coder
	xmls := messages.Apply(gocxml.StringDecoder())
	//TODO this stage should be injected by the coder analysis step

	//extract names with custom Map fn
	extracted := xmls.Map(func(input gocxml.Node) string {
		return input.Children()[0].Children()[0].Text()
	})

	//remove all names containing letter 'B' with custom Filter fn
	filtered := extracted.Filter(func(input string) bool {
		return !strings.Contains(input, "B")
	})

	//do total aggregation using custom Fn fn
	total := filtered.Transform(func(input chan string, output chan int) {
		l := 0
		for b := range input {
			l += len(b)
		}
		output <- l
	})

	//output the aggregation result by applying a general StdOutSink transform
	//TODO StdOOut sink must be network-merged to the single instance which last joined the group
	total.Apply(std.StdOutSink())

	pipeline.Run(5 * time.Second)

}

//TODO next step is adding networking-friendly checkpointer with pipeline options of optimistic or pessimistic one
