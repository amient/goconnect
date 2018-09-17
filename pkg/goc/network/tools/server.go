package tools

import (
	"bufio"
	"encoding/binary"
	"github.com/amient/goconnect/pkg/goc"
	"net"
	"strconv"
	"strings"
)

type Server struct {
	addr *net.IPAddr
	port int
	ch   goc.InputChannel
}

func (server *Server) Start(port int, ch goc.InputChannel) int {
	if ln, err := net.Listen("tcp", "0.0.0.0:"+strconv.Itoa(port)); err != nil {
		panic(err)
	} else {
		s := strings.Split(ln.Addr().String(), ":")
		server.port, _ = strconv.Atoi(s[len(s)-1])
		go func() {
			for {
				connection, _ := ln.Accept()
				server.handleConnection(connection)

			}
		}()
		return server.port
	}

}

func (server *Server) handleConnection(conn net.Conn) {
	r := bufio.NewReader(conn)
	buf := make([]byte, 0, 8)
	for {

		len := int(readUInt32(r, buf))
		data := make([]byte, 0, len)
		readFully(r, data, len)
		server.ch <- &goc.Element {
			Stamp: goc.Stamp {

			}
		}
		//TODO emit data

	}
}

func readUInt32(r *bufio.Reader, buf []byte) uint32 {
	readFully(r, buf, 4)
	return binary.BigEndian.Uint32(buf)
}

func readFully(reader *bufio.Reader, buf []byte, len int) {
	if len == -1 {
		len = cap(buf)
	}
	for n := 0; n < len; {
		if p, err := reader.Read(buf[n:]); err != nil {
			panic(err)
		} else {
			n += p
		}
	}
}
