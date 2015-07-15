package chord

import (
	"fmt"
	"net"
)

//Send opens a connection to addr, sends msg, and then returns the
//reply
func send(msg []byte, addr string) (reply []byte, err error) {

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		//TODO: look up conventions on errors for Go.
		return
	}
	_, err = conn.Write(msg)
	if err != nil {
		return
	}

	reply = make([]byte, 4096) //TODO: use framing here
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
				continue
			}
		}
	}()
}

func handleMessage(conn net.Conn, c chan []byte, c2 chan []byte) {

	//Close conenction when function exits
	defer conn.Close()

	//Create data buffer of type byte slice
	data := make([]byte, 4096) //TODO: use framing here
	n, err := conn.Read(data)
	if n >= 4095 {
		fmt.Printf("Ran out of buffer room.\n")
	}
	checkError(err)

	c <- data[:n]

	//wait for message to come back
	response := <-c2

	_, err = conn.Write(response)
	if err != nil {
		return
	}
}
