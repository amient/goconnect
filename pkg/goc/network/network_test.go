package network

import (
	"github.com/amient/goconnect/pkg/goc"
	"log"
	"reflect"
	"testing"
	"time"
)

func TestNetworkTools(t *testing.T) {
	ch := make(goc.Channel, 1)
	receiver := NetRecv("127.0.0.1:0", ch)
	sender := NetSend(1, receiver.Start())

	fixture := goc.Element {
		Stamp: goc.Stamp{
			Unix: time.Now().Unix(),
			Lo:   1,
			Hi:   1,
		},
		Value: []byte("Hello World"),
	}

	sender.Channel <- &fixture

	received := <-ch
	if (*received).Stamp != fixture.Stamp {
		log.Println(fixture.Stamp)
		log.Println((*received).Stamp)
		panic("Are not equal")
	}

	if !reflect.DeepEqual((*received).Value, fixture.Value) {
		log.Println(fixture.Value)
		log.Println((*received).Value)
		panic("Are not equal")
	}

	if (*received).Checkpoint.Part != 1 {
		panic("Actual node is not 1")
	}

	receiver.Close()
}
