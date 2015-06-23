package chord

import (
	"fmt"
	"net"
	"os"
)

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func Listen(addr string) {
	fmt.Printf("Started ProtoBuf Server")
	c := make(chan *ChordMsg)
	go func() {
		for {
			message := <-c
			parseMessage(message)
		}
	}()

	//listen to TCP port
	listener, err := net.Listen("tcp", addr)
	checkError(err)
	for {
		if conn, err := listener.Accept(); err == nil {
			go handleMessage(conn, c)
		} else {
			continue
		}
	}
}

func handleMessage(conn net.Conn, c chan *ChordMsg) {
	fmt.Println("Connection Established")

	//Close conenction when function exits
	defer conn.Close()

	//Create data buffer of type byte slice (why 4096 bytes???)
	data := make([]byte, 4096)
	n, err := conn.Read(data)
	checkError(err)
	fmt.Println("Decoding Protobuf message")

	protodata := new(ChordMsg)

	err = proto.Unmarshal(data[0:n], protodata)
	checkError(err)

	c <- protodata
}

func parseMessage(msg *ChordMsg) {

}
