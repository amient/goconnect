package network

import (
	"fmt"
	"github.com/amient/goconnect/pkg/goc"
	"net"
)

func NewSender(addr string, handlerId uint16) *Sender {
	return &Sender{
		addr:      addr,
		handlerId: handlerId,
		stamps:    make(chan *goc.Stamp, 1),
	}
}

type Sender struct {
	addr      string
	handlerId uint16
	conn      net.Conn
	duplex    *Duplex
	stamps    chan *goc.Stamp
}

func (sender *Sender) Up() <-chan *goc.Stamp {
	return sender.stamps
}

func (sender *Sender) SendDown(e *goc.Element) {
	sender.duplex.writeUInt16(2) //magic
	sender.duplex.writeUInt64(uint64(e.Stamp.Unix))
	sender.duplex.writeUInt64(e.Stamp.Lo)
	sender.duplex.writeUInt64(e.Stamp.Hi)
	sender.duplex.writeUInt16(e.Stamp.TraceLen())
	for _, nodeId := range e.Stamp.Trace {
		sender.duplex.writeUInt16(nodeId)
	}
	sender.duplex.writeSlice(e.Value.([]byte))
	sender.duplex.writer.Flush()
}

func (sender *Sender) Close() error {
	close(sender.stamps)
	return sender.duplex.Close()
}

func (sender *Sender) Start() error {
	var err error
	if sender.conn, err = net.Dial("tcp", sender.addr); err != nil {
		return err
	}
	//println("Open", sender.addr, sender.handlerId)
	sender.duplex = NewDuplex(sender.conn)
	sender.duplex.writeUInt16(sender.handlerId)
	sender.duplex.writer.Flush()
	//this needs to block until the handler is open
	sender.duplex.readUInt16()

	go func() {
		d := sender.duplex
		for {
			magic := d.readUInt16()
			switch magic {
			case 0: //eos
				return
			case 1: //incoming ack from downstream
				stamp := goc.Stamp{
					Unix: int64(d.readUInt64()),
					Lo:   d.readUInt64(),
					Hi:   d.readUInt64(),
				}
				sender.stamps <- &stamp
			default:
				panic(fmt.Errorf("unknown magic byte %d", magic))
			}
		}
	}()
	return nil
}
func (sender *Sender) SendNodeIdentify(nodeId int, receiver *Server) {
	sender.duplex.writeUInt16(1) //magic
	sender.duplex.writeUInt16(uint16(nodeId))
	sender.duplex.writeUInt64(uint64(receiver.Rand))
	sender.duplex.writer.Flush()
	sender.duplex.readUInt16()
	sender.Close()
}
