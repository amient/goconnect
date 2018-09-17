package network

import (
	"bufio"
	"encoding/binary"
	"github.com/amient/goconnect/pkg/goc"
	"net"
)

func NetSend(node uint16, addr net.Addr) *Sender {
	if conn, err := net.Dial("tcp", addr.String()); err != nil {
		panic(err)
	} else {
		sender := Sender{
			writer:  bufio.NewWriter(conn),
			Channel: make(goc.Channel, 1),
			buf:     make([]byte, 8),
		}
		go func() {
			for e := range sender.Channel {
				sender.writeUInt16(node)
				sender.writeUInt64(uint64(e.Stamp.Unix))
				sender.writeUInt64(e.Stamp.Lo)
				sender.writeUInt64(e.Stamp.Hi)
				sender.writeSlice(e.Value.([]byte))
				sender.writer.Flush()
			}
		}()
		return &sender
	}
}

type Sender struct {
	writer  *bufio.Writer
	Channel goc.Channel
	buf     []byte
}

func (s *Sender) writeUInt16(i uint16) {
	binary.BigEndian.PutUint16(s.buf, i)
	s.writeFully(s.buf, 2)
}

func (s *Sender) writeUInt32(i uint32) {
	binary.BigEndian.PutUint32(s.buf, i)
	s.writeFully(s.buf, 4)
}

func (s *Sender) writeUInt64(i uint64) {
	binary.BigEndian.PutUint64(s.buf, i)
	s.writeFully(s.buf, 8)
}

func (s *Sender) writeSlice(bytes []byte) {
	s.writeUInt32(uint32(len(bytes)))
	s.writeFully(bytes, len(bytes))
}
func (s *Sender) writeFully(bytes []byte, len int) {
	for written := 0; written < len; {
		if w, err := s.writer.Write(bytes[written:]); err != nil {
			panic(err)
		} else if w > 0 {
			written += w
		}
	}
}
