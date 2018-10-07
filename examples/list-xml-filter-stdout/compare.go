package main

import (
	"github.com/amient/goconnect/pkg/goc/coder/gocxml"
	"log"
	"strings"
	"time"
)

func main() {

	start := time.Now()

	var data = []string{
		"<name>Adam</name>", "<name>Albert</name>", "<name>Alice</name>", "<name>Alex</name>",
		"<name>Bart</name>", "<name>Bob</name>", "<name>Brittney</name>", "<name>Brenda</name>",
		"<name>Cecilia</name>", "<name>Chad</name>", "<name>Elliot</name>", "<name>Wojtek</name>",
	}

	minBuf := 8

	cap := len(data)
	s1 := make(chan string, minBuf)
	go func() {
		defer close(s1)
		for i := 0; i < 500000; i++ {
			s1 <- data[i%cap]
		}
	}()

	s2 := make(chan gocxml.Node, minBuf)
	go func() {
		defer close(s2)
		for input := range s1 {
			n, _ := gocxml.ReadNodeFromString(input)
			s2 <- n
		}
	}()

	s3 := make(chan string, minBuf)
	go func() {
		defer close(s3)
		for input := range s2 {
			s3 <- input.Children()[0].Children()[0].Text()
		}
	}()

	s4 := make(chan string, minBuf)
	go func() {
		defer close(s4)
		for input := range s3 {
			if !strings.Contains(input, "B") {
				s4 <- input
			}
		}
	}()

	agg := 0
	n := 0
	for input := range s4 {
		agg += len(input)
		n++
		if n% 100000 == 0 {
			println(agg)
		}
	}
	println(agg)

	log.Printf("Test completed %d in %f0.0 s", agg, time.Now().Sub(start).Seconds())
}
