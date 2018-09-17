package tools

import (
	"bufio"
	"encoding/binary"
	"github.com/amient/goconnect/pkg/goc"
	"net"
	"time"
)

func NewNetIn(addr string, ch goc.Channel) net.Addr {
	return listener{}.Start(addr, ch)
}

type listener struct {
	addr net.Addr
	ch   goc.Channel
}

func (l listener) Start(addr string, ch goc.Channel) net.Addr {
	if ln, err := net.Listen("tcp", addr); err != nil {
		panic(err)
	} else {
		l.addr = ln.Addr()
		//s := strings.Split(ln.Addr().String(), ":")
		//l.port, _ = strconv.Atoi(s[len(s)-1])
		go func() {
			for {
				if connection, err := ln.Accept(); err != nil {
					panic(err)
				} else {
					newConnection(connection, l.ch)
				}
			}
		}()
		return l.addr
	}

}

func newConnection(conn net.Conn, ch goc.Channel) {
	c := connection{
		reader: bufio.NewReader(conn),
		buf:    make([]byte, 0, 8),
	}
	c.handle(ch)
}

type connection struct {
	reader *bufio.Reader
	buf []byte
}

func (c *connection) handle(ch goc.Channel) {

	for {
		part := int(c.readUInt16())
		ts := time.Unix(int64(c.readUInt64()), 0)
		lo := c.readUInt64()
		hi := c.readUInt64()
		value := c.readSlice()
		ch <- &goc.Element{
			Timestamp: ts,
			Checkpoint: goc.Checkpoint{
				Part: part,
				Data: hi,
			},
			Stamp: goc.Stamp{
				Lo: lo,
				Hi: hi,
			},
			Value: value,
		}
	}
}

func (c *connection) readSlice() []byte {
	len := int(c.readUInt32())
	data := make([]byte, 0, len)
	c.readFully(data, len)
	return data
}

func (c *connection) readUInt16() uint16 {
	c.readFully(c.buf, 2)
	return binary.BigEndian.Uint16(c.buf)
}

func (c *connection) readUInt32() uint32 {
	c.readFully(c.buf, 4)
	return binary.BigEndian.Uint32(c.buf)
}

func (c *connection) readUInt64() uint64 {
	c.readFully(c.buf, 8)
	return binary.BigEndian.Uint64(c.buf)
}

func (c *connection) readFully(into []byte, len int) {
	for n := 0; n < len; {
		if p, err := c.reader.Read(into[n:]); err != nil {
			panic(err)
		} else {
			n += p
		}
	}
}
