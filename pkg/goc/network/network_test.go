package network

import (
	"github.com/amient/goconnect/pkg/goc"
	"log"
	"reflect"
	"testing"
	"time"
)

func TestNetworkTools(t *testing.T) {

	receiver := NetRecv("127.0.0.1:0")
	sender := NetSend()
	sender.Start(1, receiver.Start())

	fixture := goc.Element {
		Stamp: goc.Stamp{
			Unix: time.Now().Unix(),
			Lo:   1,
			Hi:   10,
		},
		Value: []byte("Hello World"),
	}

	sender.SendDown(&fixture)

	received := <- receiver.Down()
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

	receiver.Close()
	sender.Close()
}
