package network

import (
	"fmt"
	"github.com/amient/goconnect/pkg/goc"
	"log"
	"net"
)

func newSender(addr string, handlerId uint16, nodeId uint16) *TCPSender {
	return &TCPSender{
		addr:      addr,
		handlerId: handlerId,
		nodeId:    nodeId,
		acks:      make(chan *goc.Stamp, 1),
	}
}

type TCPSender struct {
	addr      string
	handlerId uint16
	nodeId    uint16
	conn      net.Conn
	duplex    *Duplex
	acks      chan *goc.Stamp
}

func (sender *TCPSender) Acks() <-chan *goc.Stamp {
	return sender.acks
}

func (sender *TCPSender) Close() error {
	log.Printf("NODE[%d] TCPSender.Close(%d)", sender.nodeId, sender.handlerId)
	close(sender.acks)
	return sender.duplex.Close()
}

func (sender *TCPSender) Start() error {
	var err error
	if sender.conn, err = net.Dial("tcp", sender.addr); err != nil {
		return err
	}
	//println("Open", sender.addr, sender.handlerId)
	sender.duplex = NewDuplex(sender.conn)
	sender.duplex.writeUInt16(sender.handlerId)
	sender.duplex.writeUInt16(sender.nodeId)
	sender.duplex.Flush()
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
					Uniq:   d.readUInt64(),
				}
				sender.acks <- &stamp
			default:
				panic(fmt.Errorf("unknown magic byte %d", magic))
			}
		}
	}()
	return nil
}

func (sender *TCPSender) Send(e *goc.Element) {
	sender.duplex.writeUInt16(2) //magic
	sender.duplex.writeUInt64(uint64(e.Stamp.Unix))
	sender.duplex.writeUInt64(e.Stamp.Uniq)
	sender.duplex.writeUInt16(e.Stamp.TraceLen())
	for _, nodeId := range e.Stamp.Trace {
		sender.duplex.writeUInt16(nodeId)
	}
	sender.duplex.writeSlice(e.Value.([]byte))
	sender.duplex.Flush()
}

func (sender *TCPSender) SendNodeIdentify(nodeId int, receiver *Server) {
	sender.duplex.writeUInt16(1) //magic
	sender.duplex.writeUInt16(uint16(nodeId))
	sender.duplex.writeUInt64(uint64(receiver.Rand))
	sender.duplex.Flush()
	sender.duplex.readUInt16()
	sender.Close()
}
