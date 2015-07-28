package main

import (
	"flag"
	"fmt"
	"github.com/cbocovic/chord"
	"io"
	"runtime"
	"time"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	//set up flags
	numPtr := flag.Int("num", 100, "the size of the DHT you wish to test")

	flag.Parse()
	num := *numPtr
	fmt.Printf("Joining %d servers!\n", num)

	list := make([]*chord.ChordNode, num)

	me := new(chord.ChordNode)
	me = chord.Create("127.0.0.1:8888")
	list[0] = me

	for i := 1; i < num; i++ {
		//join node to network or start a new network
		time.Sleep(time.Second)
		node := new(chord.ChordNode)
		addr := fmt.Sprintf("127.0.0.1:%d", 8888+i)
		node = chord.Join(addr, "127.0.0.1:8888")
		list[i] = node
		fmt.Printf("Joined server: %s.\n", addr)
	}
	//block until receive input
Loop:
	for {
		var cmd string
		var port int
		_, err := fmt.Scan(&cmd)
		switch {
		case cmd == "info":
			//print out successors and predecessors
			fmt.Printf("Node\t\t Successor\t\t Predecessor\n")
			for _, node := range list {
				fmt.Printf("%s\n", node.Info())
			}
		case cmd == "fingers":
			//print out finger table
			fmt.Printf("Enter port of desired node: ")
			fmt.Scan(&port)
			node := list[port-8888]
			fmt.Printf("\n%s", node.ShowFingers())
		case cmd == "succ":
			//print out successor list
			fmt.Printf("Enter port of desired node: ")
			fmt.Scan(&port)
			node := list[port-8888]
			fmt.Printf("\n%s", node.ShowSucc())
		case err == io.EOF:
			break Loop
		}

	}
	for _, node := range list {
		node.Finalize()
	}

}
