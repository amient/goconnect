package network

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"strings"
)

func NewDuplex(conn net.Conn) *Duplex {
	return &Duplex{
		conn:     conn,
		writer:   bufio.NewWriter(conn),
		writeBuf: make([]byte, 8),
		reader:   bufio.NewReader(conn),
		readBuf:  make([]byte, 8),
	}
}

type Duplex struct {
	conn     net.Conn
	writer   *bufio.Writer //sending elements
	reader   *bufio.Reader //receiving acks
	writeBuf []byte
	readBuf  []byte
}

func (duplex *Duplex) Close() error {
	return duplex.conn.Close()
}

func (duplex *Duplex) writeUInt16(i uint16) {
	binary.BigEndian.PutUint16(duplex.writeBuf, i)
	duplex.writeFully(duplex.writeBuf, 2)
}

func (duplex *Duplex) writeUInt32(i uint32) {
	binary.BigEndian.PutUint32(duplex.writeBuf, i)
	duplex.writeFully(duplex.writeBuf, 4)
}

func (duplex *Duplex) writeUInt64(i uint64) {
	binary.BigEndian.PutUint64(duplex.writeBuf, i)
	duplex.writeFully(duplex.writeBuf, 8)
}

func (duplex *Duplex) writeSlice(bytes []byte) {
	duplex.writeUInt32(uint32(len(bytes)))
	duplex.writeFully(bytes, len(bytes))
}

func (duplex *Duplex) writeFully(bytes []byte, len int) {
	for written := 0; written < len; {
		if w, err := duplex.writer.Write(bytes[written:]); err != nil {
			panic(err)
		} else if w > 0 {
			written += w
		}
	}
}

func (duplex *Duplex) readSlice() []byte {
	l := int(duplex.readUInt32())
	data := make([]byte, l)
	duplex.readFully(data, l)
	return data
}

func (duplex *Duplex) readUInt16() uint16 {
	if duplex.readFully(duplex.readBuf, 2) {
		return binary.BigEndian.Uint16(duplex.readBuf)
	} else {
		return 0 //eos
	}
}

func (duplex *Duplex) readUInt32() uint32 {
	duplex.readFully(duplex.readBuf, 4)
	return binary.BigEndian.Uint32(duplex.readBuf)
}

func (duplex *Duplex) readUInt64() uint64 {
	duplex.readFully(duplex.readBuf, 8)
	return binary.BigEndian.Uint64(duplex.readBuf)
}

func (duplex *Duplex) readFully(into []byte, len int) bool {
	for n := 0; n < len; {
		if p, err := duplex.reader.Read(into[n:]); err != nil {
			if err == io.EOF {
				return false
			} else if strings.Contains(err.Error(), "closed") {
				//FIXME the io.EOF above should be enough but for some reason on the Receiver this is also required
				return false
			} else {
				panic(err)
			}
		} else if p > 0 {
			n += p
		}
	}
	return true
}
