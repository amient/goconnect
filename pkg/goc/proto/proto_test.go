package proto

import (
	"github.com/amient/goconnect/pkg/goc"
	"github.com/amient/goconnect/pkg/goc/io"
	"github.com/amient/goconnect/pkg/goc/network"
	"log"
	"testing"
	"time"
)


func TestSomething(t *testing.T) {

	r1 := stage1()
	t2 := stage2(r1)
	for e := range t2 {
		log.Println(string(e.Value.([]byte)), e.Stamp)
	}
}

func stage1() chan *goc.Element {
	intercept := make(chan *goc.Element, 1)
	go io.From([][]byte{[]byte("a"),[]byte("b"),[]byte("c")}).Run(intercept)

	output := make(chan *goc.Element, 1)
	go func() {
		autoi := uint64(0)
		for e := range intercept {
			autoi += 1
			e.Stamp.Unix = time.Now().Unix()
			e.Stamp.Lo = autoi
			e.Stamp.Hi = autoi
			output <- e
		}
		close(output)
	}()
	return output
}

func stage2(input chan *goc.Element) chan *goc.Element {
	send := network.NetSend()
	recv := network.NetRecv("127.0.0.1:0")

	go func() {
		send.Start(1, recv.Start())
		for e := range stage1() {
			send.SendDown(e)
		}
		send.Close()
	}()

	output := make(chan *goc.Element, 1)
	go func() {
		for e := range recv.Down() {
			output <- e
		}
		close(output)
	}()
	return output
}

