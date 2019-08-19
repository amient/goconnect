package main

import (
	"github.com/amient/goconnect/coder/xml"
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

	minBuf := 32

	cap := len(data)
	s1 := make(chan string, minBuf)
	go func() {
		defer close(s1)
		for i := 0; i < 500000; i++ {
			s1 <- data[i%cap]
		}
	}()

	s2 := make(chan xml.Node, minBuf)
	go func() {
		defer close(s2)
		for input := range s1 {
			n, _ := xml.ReadNodeFromString(input)
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

	s5 := make(chan int, minBuf)
	go func() {
		defer close(s5)
		agg := 0
		n := 0
		for input := range s4 {
			agg += len(input)
			n++
			if n% 50000 == 0 {
				s5 <- agg
			}
		}
		s5 <- agg
	}()

	s6 := make(chan int, minBuf)
	go func() {
		defer close(s6)
		for input := range s5 {
			if input > 210000 {
				s6 <- input
			}
		}
	}()

	for input := range s6 {
		println(input)
	}

	log.Printf("Test completed in %f0.0 s", time.Now().Sub(start).Seconds())
}
