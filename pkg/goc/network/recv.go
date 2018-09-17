package network

import (
	"bufio"
	"encoding/binary"
	"github.com/amient/goconnect/pkg/goc"
	"net"
	"strings"
	"time"
)

func NetRecv(addr string, ch goc.Channel) *Receiver {
	return &Receiver{
		addr:     addr,
		ch:       ch,
		handlers: make([]handler, 0, 1)}
}

type Receiver struct {
	Addr     net.Addr
	ln       net.Listener
	addr     string
	ch       goc.Channel
	handlers []handler
}

func (recv *Receiver) Start() net.Addr {
	var err error
	if recv.ln, err = net.Listen("tcp", recv.addr); err != nil {
		panic(err)
	} else {
		recv.Addr = recv.ln.Addr()
		//s := strings.Split(ln.Addr().String(), ":")
		//recv.port, _ = strconv.Atoi(s[len(s)-1])

		go func() {
			if conn, err := recv.ln.Accept(); err != nil {
				panic(err)
			} else {
				h := handler{
					conn: conn,
					reader: bufio.NewReader(conn),
					buf:    make([]byte, 8),
				}
				recv.handlers = append(recv.handlers, h)
				h.handle(recv.ch)
			}
		}()
	}
	return recv.Addr
}

func (recv *Receiver) Close() error {
	for _, h := range recv.handlers {
		if err := h.Close(); err != nil {
			panic(err)
		}
	}
	return recv.ln.Close()
}

type handler struct {
	conn net.Conn
	reader *bufio.Reader
	buf    []byte
}

func (h *handler) handle(ch goc.Channel) {
	for {
		if node := int(h.readUInt16()); node == 0 {
			//eos
			return
		} else {
			ts := time.Unix(int64(h.readUInt64()), 0)
			lo := h.readUInt64()
			hi := h.readUInt64()
			value := h.readSlice()
			//log.Printf("Incoming node: %v, ts: %v, lo: %v, hi: %v, data: %v", node, ts, lo, hi, len(value))
			ch <- &goc.Element{
				Checkpoint: goc.Checkpoint{
					Part: node,
					Data: hi,
				},
				Stamp: goc.Stamp{
					Unix: ts.Unix(),
					Lo:   lo,
					Hi:   hi,
				},
				Value: value,
			}
		}
	}
}

func (h *handler) readSlice() []byte {
	l := int(h.readUInt32())
	data := make([]byte, l)
	h.readFully(data, l)
	return data
}

func (h *handler) readUInt16() uint16 {
	if h.readFully(h.buf, 2) {
		return binary.BigEndian.Uint16(h.buf)
	} else {
		return 0 //eos
	}
}

func (h *handler) readUInt32() uint32 {
	h.readFully(h.buf, 4)
	return binary.BigEndian.Uint32(h.buf)
}

func (h *handler) readUInt64() uint64 {
	h.readFully(h.buf, 8)
	return binary.BigEndian.Uint64(h.buf)
}

func (h *handler) readFully(into []byte, len int) bool {
	for n := 0; n < len; {
		if p, err := h.reader.Read(into[n:]); err != nil {
			if strings.Contains(err.Error(), "closed") {
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

func (h *handler) Close() error {
	return h.conn.Close()
}