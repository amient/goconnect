package network

import (
	"github.com/amient/goconnect/pkg/goc"
	"log"
	"reflect"
	"testing"
	"time"
)

func TestNetworkTools(t *testing.T) {

	server := NewServer("127.0.0.1:10000")
	handler := server.NewReceiver(1)
	sender := newSender("127.0.0.1:10000", handler.id, 1)

	server.Start()
	sender.Start()

	fixture := goc.Element{
		Stamp: goc.Stamp{
			Unix: time.Now().Unix(),
			Lo:   1,
			Hi:   10,
		},
		Value: []byte("Hello World"),
	}

	sender.Send(&fixture)


	received := <-handler.Elements()
	if (*received).Stamp.Lo != fixture.Stamp.Lo || (*received).Stamp.Hi != fixture.Stamp.Hi || reflect.DeepEqual((*received).Stamp.Trace, fixture.Stamp.Trace) {
		log.Println(fixture.Stamp)
		log.Println((*received).Stamp)
		panic("Are not equal")
	}

	if !reflect.DeepEqual((*received).Value, fixture.Value) {
		log.Println(fixture.Value)
		log.Println((*received).Value)
		panic("Are not equal")
	}

	server.Close()
	sender.Close()
}
