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
		Assigned:  make(chan bool, 1),
		quit:      make(chan bool, 1),
		receivers: make(map[uint16]*TCPReceiver),
	}
	recv.NewReceiver(0) //default TCPReceiver for node discovery
	return &recv
}

type Server struct {
	ID        uint16
	Addr      net.Addr
	Rand      int64
	Assigned  chan bool
	ln        *net.TCPListener
	addr      string
	quit      chan bool
	receivers map[uint16]*TCPReceiver
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
						remoteNodeId := duplex.readUInt16()
						server.lock.Lock()
						receiver, exists := server.receivers[handlerId]
						server.lock.Unlock()
						if !exists {
							panic(fmt.Errorf("ERROR[%v] TCPReceiver not registered %d", server.addr, handlerId))
						} else {
							//reply that the channel has been setup
							duplex.writeUInt16(handlerId)
							duplex.Flush()
							receiver.duplex[remoteNodeId] = duplex
							go receiver.handle(duplex, conn)
						}
					}
				}
			}
		}()
	}
	return nil
}
func (server *Server) NewReceiver(handlerId uint16) *TCPReceiver {
	server.lock.Lock()
	defer server.lock.Unlock()
	server.receivers[handlerId] = &TCPReceiver{
		id:     handlerId,
		server: server,
		duplex: make(map[uint16]*Duplex),
		down:   make(chan *goc.Element, 100), //TODO the capacity should be the number of nodes
	}
	if handlerId > 0 {
		//log.Printf("REGISTER[%v] %d", server.addr, handlerId)
	}
	return server.receivers[handlerId]

}

type TCPReceiver struct {
	id       uint16
	server   *Server
	down     chan *goc.Element
	refCount int32
	duplex   map[uint16]*Duplex
}

func (h *TCPReceiver) ID() uint16 {
	return h.id
}

func (h *TCPReceiver) Ack(stamp goc.Stamp) error {
	upstreamNode := stamp.Trace[int(h.ID()-2)]
	log.Printf("TCP-ACK[%v/%d] upstream node =%d", h.server.addr, h.ID, upstreamNode)
	duplex := h.duplex[upstreamNode]
	duplex.writeUInt16(1) //magic
	duplex.writeUInt64(uint64(stamp.Unix))
	duplex.writeUInt64(stamp.Lo)
	duplex.writeUInt64(stamp.Hi)
	return duplex.Flush()

}

func (h *TCPReceiver) handle(duplex *Duplex, conn net.Conn) {
	//refCount :=
	atomic.AddInt32(&h.refCount, 1)
	if h.id > 0 {
		//log.Printf("OPEN[%v/%d] refCount=%d", h.server.addr, h.ID, refCount)
		defer h.Close()
	}

	defer duplex.Close()
	defer conn.Close()
	for {
		//log.Printf("NEXT[%v/%d]", h.server.addr, h.NodeID)
		magic := duplex.readUInt16()
		switch magic {
		case 0: //eof
			//log.Printf("EOF[%v/%d]", h.server.addr, h.NodeID)
			return
		case 1: //node identification
			id := duplex.readUInt16() + 1
			rand := int64(duplex.readUInt64())
			if rand == h.server.Rand {
				log.Printf("Assignment: Node ID %d is listening on %v", id, conn.LocalAddr().String())
				if h.server.ID != id {
					h.server.ID = id
					h.server.Assigned <- true
				}
			}
			return
		case 2: //downstream element
			//read stamp first
			unix := int64(duplex.readUInt64())
			lo := duplex.readUInt64()
			hi := duplex.readUInt64()
			numTraceSteps := duplex.readUInt16()
			stamp := goc.Stamp{
				Unix:  unix,
				Lo:    lo,
				Hi:    hi,
				Trace: goc.NewTrace(numTraceSteps),
			}
			for i := uint16(0); i < numTraceSteps; i++ {
				stamp.AddTrace(duplex.readUInt16())
			}
			//element value second
			value := duplex.readSlice()
			h.down <- &goc.Element{
				Stamp: stamp,
				Value: value,
			}
			//log.Printf("RECEIVED[%v/%d] %v element: %v", h.server.addr, h.NodeID, stamp, value)
		default:
			panic(fmt.Errorf("unknown magic byte %d", magic))
		}
	}

}

func (h *TCPReceiver) Elements() <-chan *goc.Element {
	return h.down
}

func (h *TCPReceiver) Close() error {
	refCount := atomic.AddInt32(&h.refCount, -1)
	//log.Printf("CLOSE[%v/%d] refCount=%d", h.server.addr, h.ID, refCount)
	if refCount == 0 {
		delete(h.server.receivers, h.id)
		close(h.down)
		h.down = nil
	}
	return nil
}
