package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

var (
	port int = 8454
)

func main() {

	port := new(Server).Start()

	if conn, err := net.Dial("tcp", "127.0.0.1:"+fmt.Sprint(port)); err != nil {
		panic(err)
	} else {

		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Text to send: ")
		text, _ := reader.ReadString('\n')
		// send to socket
		fmt.Fprintf(conn, text+"\n")
		// listen for reply
		message, _ := bufio.NewReader(conn).ReadString('\n')
		fmt.Print("Message from server: " + message)
	}
}

type Server struct {
	addr *net.IPAddr
	port int
}

func (server *Server) handleConnection(conn net.Conn) {
	for {
		// will listen for message to process ending in newline (\n)
		message, _ := bufio.NewReader(conn).ReadString('\n')
		// output message received
		fmt.Print("Message Received:", string(message))
		// sample process for string received
		newmessage := strings.ToUpper(message)
		// send new string back to client
		conn.Write([]byte(newmessage + "\n"))
	}
}
func (server *Server) Start() int {
	if ln, err := net.Listen("tcp", "0.0.0.0:0"); err != nil {
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
