package network

import (
	"github.com/amient/goconnect/pkg/goc"
	"net"
)

func NetRecv(addr string) *Receiver {
	return &Receiver{
		addr: addr,
		down: make(chan *goc.Element, 1),
	}
}

type Receiver struct {
	Addr   net.Addr
	down   chan *goc.Element
	ln     net.Listener
	addr   string
	duplex *Duplex
}

func (recv *Receiver) Down() <- chan *goc.Element {
	return recv.down
}

func (recv *Receiver) SendUp(stamp *goc.Stamp) error {
	recv.duplex.writeUInt16(1) //magic
	recv.duplex.writeUInt64(uint64(stamp.Unix))
	recv.duplex.writeUInt64(stamp.Lo)
	recv.duplex.writeUInt64(stamp.Hi)
	return recv.duplex.writer.Flush()
}

func (recv *Receiver) Close() error {
	close(recv.down)
	if recv.duplex != nil {
		recv.duplex.Close()
	}
	return recv.ln.Close()
}


func (recv *Receiver) Start() net.Addr {
	var err error
	if recv.ln, err = net.Listen("tcp", recv.addr); err != nil {
		panic(err)
	} else {
		recv.Addr = recv.ln.Addr()
		go func() {
			if conn, err := recv.ln.Accept(); err != nil {
				panic(err)
			} else {
				recv.duplex = NewDuplex(conn)
				for {
					switch recv.duplex.readUInt16() {
					case 0: //eos
						recv.Close()
						return
					case 1: //magic
						unix := int64(recv.duplex.readUInt64())
						lo := recv.duplex.readUInt64()
						hi := recv.duplex.readUInt64()
						value := recv.duplex.readSlice()
						stamp := goc.Stamp{
							Unix: unix,
							Lo:   lo,
							Hi:   hi,
						}

						//log.Printf("RECV %v element: %v", stamp, value)
						recv.down <- &goc.Element{
							/*Checkpoint: goc.Checkpoint{
								Part: int(node),
								Data: stamp,
							},*/
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

