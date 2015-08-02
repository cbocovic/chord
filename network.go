package chord

import (
	"fmt"
	"io"
	"net"
	"runtime/debug"
)

//Send opens a connection to addr, sends msg, and then returns the
//reply
func Send(msg []byte, addr string) (reply []byte, err error) {

	if addr == "" {
		debug.PrintStack()
		panic("ahhh")
	}

	conn, err := net.Dial("tcp", addr)
	checkError(err)
	if err != nil {
		//TODO: look up conventions on errors for Go.
		return
	}
	defer conn.Close()
	_, err = conn.Write(msg)
	if err != nil {
		return
	}

	reply = make([]byte, 100000) //TODO: use framing here
	n, err := conn.Read(reply)
	if err != nil {
		return
	}
	reply = reply[:n]

	return

}

//send for a node checks existing open connections
func (node *ChordNode) send(msg []byte, addr string) (reply []byte, err error) {
	if addr == "" {
		debug.PrintStack()
		panic("ahhh")
	}

	conn, ok := node.connections[addr]
	if !ok {
		//fmt.Printf("Connection from %s to %s didn't exist. Creating new...\n", node.ipaddr, addr)
		conn, err = net.Dial("tcp", addr)
		if err != nil {
			checkError(err)
			return
		}
		node.connections[addr] = conn
		//fmt.Printf("node %s has %d connections.\n", node.ipaddr, len(node.connections))
	}

	_, err = conn.Write(msg)
	if err != nil {
		//might have timed out
		//fmt.Printf("Connection from %s to %s is no good. Creating new...\n", node.ipaddr, addr)
		conn, err = net.Dial("tcp", addr)
		if err != nil {
			checkError(err)
			return
		}
		_, err = conn.Write(msg)
		if err != nil {
			checkError(err)
			return
		}
		node.connections[addr] = conn
	}

	reply = make([]byte, 100000) //TODO: use framing here
	n, err := conn.Read(reply)
	if err != nil {
		return
	}
	reply = reply[:n]

	return

}

//Listens at an address for incoming messages
func (node *ChordNode) listen(addr string) {
	fmt.Printf("Chord node %x is listening on %s...\n", node.id, addr)
	c := make(chan []byte)
	c2 := make(chan []byte)
	go func() {
		defer fmt.Printf("No longer listening...\n")
		for {
			message := <-c
			node.parseMessage(message, c2)
		}
	}()

	//listen to TCP port
	listener, err := net.Listen("tcp", addr)
	checkError(err)
	go func() {
		defer fmt.Printf("No longer listening...\n")
		for {
			if conn, err := listener.Accept(); err == nil {
				go handleMessage(conn, c, c2)
			} else {
				checkError(err)
				continue
			}
		}
	}()
}

func handleMessage(conn net.Conn, c chan []byte, c2 chan []byte) {

	//Close conenction when function exits
	defer conn.Close()
	for {

		//Create data buffer of type byte slice
		data := make([]byte, 100000) //TODO: use framing here
		n, err := conn.Read(data)
		if n >= 4095 {
			fmt.Printf("Ran out of buffer room.\n")
		}
		if err == io.EOF { //exit cleanly
			return
		}
		if err != nil {
			fmt.Printf("Uh oh in handle message.\n")
			checkError(err)
			return
		}

		c <- data[:n]

		//wait for message to come back
		response := <-c2

		n, err = conn.Write(response)
		if err != nil {
			return
		}
		if n > 100000 {
			fmt.Printf("Uh oh. Wrote %d bytes.\n", n)
		}
	}
}
