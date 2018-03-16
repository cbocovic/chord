/**
 *Copyright (c) 2018 Cecylia Bocovich
 *
 *Permission is hereby granted, free of charge, to any person obtaining a copy
 *of this software and associated documentation files (the "Software"), to deal
 *in the Software without restriction, including without limitation the rights
 *to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *copies of the Software, and to permit persons to whom the Software is
 *furnished to do so, subject to the following conditions:

 *The above copyright notice and this permission notice shall be included in all
 *copies or substantial portions of the Software.

 *THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *SOFTWARE.
 */

package main

import (
	"flag"
	"fmt"
	"github.com/cbocovic/chord"
	"io"
	//"runtime"
	"time"
)

func main() {
	//runtime.GOMAXPROCS(runtime.NumCPU())

	var startaddr string

	//set up flags
	numPtr := flag.Int("num", 1, "the size of the DHT you wish to test")
	startPtr := flag.Int("start", 1, "ipaddr to start from")

	flag.Parse()
	num := *numPtr
	start := *startPtr

	low := (1 + start) % 256
	middle := ((1 + start) / 256) % 256
	high := ((1 + start) / (256 * 256)) % 256
	startaddr = fmt.Sprintf("127.%d.%d.%d:8888", high, middle, low)

	fmt.Printf("Joining %d server starting at %s!\n", 1, startaddr)

	list := make([]*chord.ChordNode, num) //num)
	if start == 1 {

		me := new(chord.ChordNode)
		me = chord.Create(startaddr)
		list[0] = me
	} else {
		me := new(chord.ChordNode)
		me = chord.Join(startaddr, "127.0.0.2:8888")
		list[0] = me
	}

	for i := 1; i < num; i++ {
		//join node to network or start a new network
		time.Sleep(time.Second)
		node := new(chord.ChordNode)
		low := (1 + start + i) % 256
		middle := ((1 + start + i) / 256) % 256
		high := ((1 + start + i) / (256 * 256)) % 256
		addr := fmt.Sprintf("127.%d.%d.%d:8888", high, middle, low)

		fmt.Printf("Joining %d server starting at %s!\n", 1, addr)
		node = chord.Join(addr, startaddr)
		list[i] = node
		fmt.Printf("Joined server: %s.\n", addr)
	}
	//block until receive input
Loop:
	for {
		var cmd string
		var index int
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
			fmt.Printf("Enter index of desired node: ")
			fmt.Scan(&index)
			if index >= 0 && index < len(list) {
				node := list[index]
				fmt.Printf("\n%s", node.ShowFingers())
			}
		case cmd == "succ":
			//print out successor list
			fmt.Printf("Enter index of desired node: ")
			fmt.Scan(&index)
			if index >= 0 && index < len(list) {
				node := list[index]
				fmt.Printf("\n%s", node.ShowSucc())
			}
		case err == io.EOF:
			break Loop
		}

	}
	for _, node := range list {
		node.Finalize()
	}

}
