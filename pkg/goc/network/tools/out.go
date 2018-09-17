package tools

import (
	"bufio"
	"encoding/binary"
	"github.com/amient/goconnect/pkg/goc"
	"net"
)

func NewClient(node uint16, addr net.Addr) goc.Channel {
	if conn, err := net.Dial("tcp", addr.String()); err != nil {
		panic(err)
	} else {
		c := sender{
			writer: bufio.NewWriter(conn),
			ch:     make(goc.Channel),
			buf:    make([]byte, 0, 8),
		}

		go func() {
			for e := range c.ch {
				c.writeUInt16(node)
				c.writeUInt64(uint64(e.Timestamp.Unix()))
				c.writeUInt64(e.Stamp.Lo)
				c.writeUInt64(e.Stamp.Hi)
				c.writeSlice(e.Value.([]byte))
			}
		}()
		return c.ch
	}
}

type sender struct {
	writer *bufio.Writer
	ch     goc.Channel
	buf    []byte
}

func (s sender) writeUInt16(i uint16) {
	binary.BigEndian.PutUint16(s.buf, i)
	s.writeFully(s.buf, 2)

}

func (s sender) writeUInt64(i uint64) {
	binary.BigEndian.PutUint64(s.buf, i)
	s.writeFully(s.buf, 8)
}

func (s sender) writeSlice(bytes []byte) {
	s.writeFully(bytes, len(bytes))
}
func (s sender) writeFully(bytes []byte, len int) {

	for written := 0; written < len; {
		if w, err:= s.writer.Write(bytes[written:]); err != nil {
			panic(err)
		} else {
			written += w
		}
	}
}
