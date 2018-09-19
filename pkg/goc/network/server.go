package network

import (
	"fmt"
	"github.com/amient/goconnect/pkg/goc"
	"log"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

func NewServer(addr string) *Server {
	recv := Server{
		Rand:      rand.Int63(),
		addr:      addr,
		assigned:  make(chan bool, 1),
		quit:      make(chan bool, 1),
		receivers: make(map[uint16]*Receiver),
	}
	recv.NewReceiver(0, "default") //default Receiver for node discovery
	return &recv
}

type Server struct {
	Rand      int64
	NodeId    int
	Addr      net.Addr
	ln        *net.TCPListener
	addr      string
	assigned  chan bool
	quit      chan bool
	receivers map[uint16]*Receiver
	lock      sync.Mutex
}

func (server *Server) Close() error {
	log.Printf("CLOSE SERVER[%v]", server.addr)
	server.quit <- true
	<-server.quit
	return nil
}

func (server *Server) Start() error {
	var err error
	ipv4addr, _ := net.ResolveTCPAddr("tcp4", server.addr)
	if server.ln, err = net.ListenTCP("tcp", ipv4addr); err != nil {
		return err
	} else {
		server.Addr = server.ln.Addr()
		server.ln.SetDeadline(time.Now().Add(1e9))
		go func() {
			for {
				select {
				case <-server.quit:
					server.ln.Close()
					server.quit <- true
					return
				default:
					server.ln.SetDeadline(time.Now().Add(1e9))
					if conn, err := server.ln.Accept(); err != nil {
						if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
							continue
						}
						panic(err)
					} else {
						duplex := NewDuplex(conn)
						handlerId := duplex.readUInt16()
						server.lock.Lock()
						if receiver, exists := server.receivers[handlerId]; !exists {
							panic(fmt.Errorf("ERROR[%v/%d] Receiver not registered", server.addr, handlerId))
						} else {
							//reply that the channel has been setup
							duplex.writeUInt16(handlerId)
							duplex.writer.Flush()
							server.lock.Unlock()
							go receiver.handle(duplex, conn)
						}
					}
				}
			}
		}()
	}
	return nil
}
func (server *Server) NewReceiver(handlerId uint16, info string) *Receiver {
	server.lock.Lock()
	defer server.lock.Unlock()
	server.receivers[handlerId] = &Receiver{
		ID:     handlerId,
		server: server,
		down:   make(chan *goc.Element, 100), //TODO the capacity should be the number of nodes
	}
	if handlerId > 0 {
		//log.Printf("REGISTER[%v/%d] %s", server.addr, handlerId, info)
	}
	return server.receivers[handlerId]

}

type Receiver struct {
	ID       uint16
	server   *Server
	down     chan *goc.Element
	refCount int32
	//duplex *Duplex
}

//func (h *Receiver) SendUp(stamp *goc.Stamp) error {
//	h.duplex.writeUInt16(1) //magic
//	h.duplex.writeUInt64(uint64(stamp.Unix))
//	h.duplex.writeUInt64(stamp.Lo)
//	h.duplex.writeUInt64(stamp.Hi)
//	return h.duplex.writer.Flush()
//}

func (h *Receiver) handle(duplex *Duplex, conn net.Conn) {
	//refCount :=
	atomic.AddInt32(&h.refCount, 1)
	if h.ID > 0 {
		//log.Printf("OPEN[%v/%d] refCount=%d", h.server.addr, h.ID, refCount)
		defer h.Close()
	}

	defer duplex.Close()
	defer conn.Close()
	for {
		//log.Printf("NEXT[%v/%d]", h.server.addr, h.NodeId)
		switch duplex.readUInt16() {
		case 0: //eos
			//log.Printf("EOF[%v/%d]", h.server.addr, h.NodeId)
			return
		case 1: //magic NodeId
			id := duplex.readUInt16() + 1
			rand := int64(duplex.readUInt64())
			if rand == h.server.Rand {
				//log.Printf("Assigning NodeId %d to node %v", id, conn.LocalAddr().String())
				if h.server.NodeId != int(id) {
					h.server.NodeId = int(id)
					h.server.assigned <- true
				}
			}
			return
		case 2: //magic
			unix := int64(duplex.readUInt64())
			lo := duplex.readUInt64()
			hi := duplex.readUInt64()
			value := duplex.readSlice()
			stamp := goc.Stamp{
				Unix: unix,
				Lo:   lo,
				Hi:   hi,
			}

			h.down <- &goc.Element{
				Stamp: stamp,
				Value: value,
			}
			//log.Printf("RECEIVED[%v/%d] %v element: %v", h.server.addr, h.NodeId, stamp, value)
		default:
			panic("unknown magic byte")
		}
	}

}

func (h *Receiver) Down() <-chan *goc.Element {
	if h.down == nil {
		panic("output channel not initialized")
	}
	return h.down
}

func (h *Receiver) Close() {
	refCount := atomic.AddInt32(&h.refCount, -1)
	//log.Printf("CLOSE[%v/%d] refCount=%d", h.server.addr, h.ID, refCount)
	if refCount == 0 {
		delete(h.server.receivers, h.ID)
		close(h.down)
	}
}
