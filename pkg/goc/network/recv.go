package network

import (
	"github.com/amient/goconnect/pkg/goc"
	"log"
	"net"
)

func NetRecv(addr string) *Receiver {
	return &Receiver{
		addr:     addr,
		handlers: make(map[uint16]*Duplex)}
}

type Receiver struct {
	Addr     net.Addr
	ln       net.Listener
	addr     string
	handlers map[uint16]*Duplex
}

func (recv *Receiver) Start(output goc.Channel) net.Addr {
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
				d := NewDuplex(conn)
				node := d.readUInt16()
				recv.handlers[node] = d
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
						value := d.readSlice()
						log.Printf("RECV %v element: %v", stamp, value)
						output <- &goc.Element{
							Checkpoint: goc.Checkpoint{
								Part: int(node),
								Data: stamp,
							},
							Stamp: stamp,
							Value: value,
						}
					default:
						panic("unknown magic byte")
					}
				}
			}
		}()
	}
	return recv.Addr
}

func (recv *Receiver) Send(node uint16, stamp *goc.Stamp) error {
	upstream := recv.handlers[node]
	upstream.writeUInt16(1) //magic
	upstream.writeUInt64(uint64(stamp.Unix))
	upstream.writeUInt64(stamp.Lo)
	upstream.writeUInt64(stamp.Hi)
	return upstream.writer.Flush()
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
	conn   net.Conn
	duplex *Duplex
}

func (h *handler) Close() error {
	return h.conn.Close()
}
