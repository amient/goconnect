package main

import (
	"fmt"
	"github.com/amient/goconnect/pkg/coder/xmlc"
	"github.com/amient/goconnect/pkg/goc"
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

	messages := goc.FromList(list)

	xmls := messages.Apply(goc.Fn(func(input string) xmlc.Node {
		var node, err = xmlc.ReadNodeFromString(input)
		if err != nil {
			panic(err)
		}
		return node
	}))

	bytes := xmls.Apply(goc.Fn(func(input xmlc.Node) []byte {
		s, err := xmlc.WriteNodeAsString(input)
		if err != nil {
			panic(err)
		}
		return []byte(s)
	}))

	total := bytes.Apply(goc.Agg(func(input chan []byte, output chan int) {
		l := 0
		for b := range input {
			l += len(b)
		}
		output <- l
	}))


	sink1 := total.Apply(goc.Fn(func(element int) error {
		fmt.Println("Total processed bytes:", element)
		return nil
	}))

	goc.RunPipeline(sink1)

}

//TODO next step is adding checkpointer with pipeline options of optimistic or pessimistic one
//TODO maybe forking outputs is possible without breaking guarantees
//TODO probably merging from multiple inputs would be possible if all of them support optimistic commit
//sink2 := xmls.Apply(Fn(func(input xmlc.Node) error {
//	fmt.Println(sink2)
//	return nil
//}))


