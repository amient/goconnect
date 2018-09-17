package tools

import (
	"github.com/amient/goconnect/pkg/goc"
	"testing"
	"time"
)

func TestNetworkTools(t *testing.T) {
	ch := make(goc.Channel, 1)
	addr := NewNetIn("127.0.0.1:0", ch)
	fixture := goc.Element {
		Timestamp: time.Now(),
		Stamp: goc.Stamp{
			Lo: 1,
			Hi: 1,
		},
		Value: "Hello World",
	}
	NewClient(1, addr) <- &fixture
	println(<-ch)
}
