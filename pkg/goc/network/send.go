package network

import (
	"github.com/amient/goconnect/pkg/goc"
	"log"
	"net"
)

func NetSend(addr net.Addr) *Sender {
	return &Sender{
		addr: addr,
		//channel: make(goc.Channel, 1),
	}
}

type Sender struct {
	addr   net.Addr
	conn   net.Conn
	duplex *Duplex
}

func (sender *Sender) Start(node uint16, stream *goc.Stream) {
	var err error
	if sender.conn, err = net.Dial("tcp", sender.addr.String()); err != nil {
		panic(err)
	}
	sender.duplex = NewDuplex(sender.conn)
	sender.duplex.writeUInt16(node) //node id
	go func() {
		d := sender.duplex
		for {
			switch d.readUInt16() {
			case 0: //eos
				return
			case 1: //magic
				stamp := goc.Stamp{
					Unix: int64(d.readUInt64()),
					Lo:   d.readUInt64(),
					Hi:   d.readUInt64(),
				}
				log.Printf("Returning %v", stamp)
				stream.Ack(stamp)
			default:
				panic("unknown magic byte")
			}
		}
	}()
}

func (sender *Sender) Send(e *goc.Element) {
	sender.duplex.writeUInt16(1) //magic
	sender.duplex.writeUInt64(uint64(e.Stamp.Unix))
	sender.duplex.writeUInt64(e.Stamp.Lo)
	sender.duplex.writeUInt64(e.Stamp.Hi)
	sender.duplex.writeSlice(e.Value.([]byte))
	sender.duplex.writer.Flush()
}

func (sender *Sender) Close() error {
	return sender.duplex.Close()
}
