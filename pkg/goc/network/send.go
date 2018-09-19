package network

import (
	"github.com/amient/goconnect/pkg/goc"
	"net"
)

func NetSend() *Sender {
	return &Sender{
		stamps: make(chan *goc.Stamp, 1),
	}
}

type Sender struct {
	conn   net.Conn
	duplex *Duplex
	stamps chan *goc.Stamp
}

func (sender *Sender) Up() <-chan *goc.Stamp {
	return sender.stamps
}

func (sender *Sender) SendDown(e *goc.Element) {
	sender.duplex.writeUInt16(1) //magic
	sender.duplex.writeUInt64(uint64(e.Stamp.Unix))
	sender.duplex.writeUInt64(e.Stamp.Lo)
	sender.duplex.writeUInt64(e.Stamp.Hi)
	sender.duplex.writeSlice(e.Value.([]byte))
	sender.duplex.writer.Flush()
}

func (sender *Sender) Close() error {
	close(sender.stamps)
	return sender.duplex.Close()
}

func (sender *Sender) Start(node uint16, addr net.Addr) {
	var err error
	if sender.conn, err = net.Dial("tcp", addr.String()); err != nil {
		panic(err)
	}
	sender.duplex = NewDuplex(sender.conn)
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
				sender.stamps <- &stamp
			default:
				panic("unknown magic byte")
			}
		}
	}()
}
