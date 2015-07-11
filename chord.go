/* Package chord
 *
 * This package is a collection of structures and functions associated
 * with the Chord distributed lookup protocol. Each ChordServer contains
 * the following data items in order to achieve logarithmic lookups:
 *		* id - the identifier of the node, modulo the max size (N) of the DHT
 *		* ipaddr - the InternetAddress of a the node
 *
 *		* predecessor - the ChordNode immediately before it in the id ring
 *		* successor - the ChordNode immediately after it in the id ring
 *		* fingerTable - routing information about log(N) other nodes in the DHT
 *
 * We define the following functions in this file:
 *		Static functions:
 *			* lookup(key ulong, node InternetAddress) - a function to lookup a key at a particular
 *					node in the DHT. Returns the Internet Address of the node that owns that key
 *
 *		Receiver functions:
 *			* join(id ulong, node InternetAddress) - joins the DHT with the identifier id by
 *					contacting
 */

package chord

import (
	"crypto/sha256"
	"fmt"
	"math/big"
	"os"
	"time"
)

//Finger type denoting identifying information about a ChordNode
type Finger struct {
	id     [32]byte
	ipaddr string
}

//ChordNode type denoting a Chord server. Each server has a predecessor, successor, fingertable
// containing information about log(N) other nodes in the network, identifier, and InternetAddress.
type ChordNode struct {
	predecessor *Finger
	successor   *Finger
	fingerTable [256]Finger

	id     [32]byte
	ipaddr string
}

//error checking function
func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

//Lookup returns the address of the ChordNode that is responsible
//for the key. The procedure begins at the address denoted by start.
func Lookup(key [32]byte, start string) (addr string, err error) {

	addr = start

	msg := getfingersMsg()
	reply, err := send(msg, start)
	if err != nil {
		return "", err
	}
	ft, err := parseFingers(reply)
	checkError(err)

	//loop through finger table and see what the closest finger is
	for i := len(ft) - 1; i > 0; i-- {
		f := ft[i]
		if i == 0 {
			break
		}
		if inRange(f.id, ft[0].id, key) {
			return Lookup(key, f.ipaddr)
		}
	}

	return
}

//Create will start a new Chord ring and return the original ChordNode
func Create(myaddr string) *ChordNode {
	node := new(ChordNode)
	//create id by hashing ipaddr
	node.id = sha256.Sum256([]byte(myaddr))
	node.ipaddr = myaddr
	//succ := new(Finger)
	//succ.id = node.id
	//succ.ipaddr = node.ipaddr
	//node.successor = succ
	fmt.Printf("Created node with id: %x\n", node.id)
	node.listen(myaddr)
	fmt.Printf("Test\n")
	go node.maintain()
	fmt.Printf("Exiting create.\n")
	return node
}

//Join will add a ChordNode to the network from an existing node
//specified by addr.
func Join(myaddr string, addr string) *ChordNode {
	node := Create(myaddr)
	fmt.Printf("Finished creating node. Now to join...\n")

	successor, err := Lookup(node.id, addr)
	checkError(err)

	//find id of node
	msg := getidMsg()
	reply, err := send(msg, successor)
	checkError(err)

	//update node info to include successor
	succ := new(Finger)
	succ.id, _ = parseId(reply)
	fmt.Printf("Found successor: %x.\n", succ.id)
	succ.ipaddr = successor
	node.successor = succ
	node.fingerTable[0] = *succ

	//TODO: remove after testing
	//msg := pingMsg()

	//reply, err := send(msg, addr)
	//checkError(err)

	//if succ, err := parsePong(reply); succ == true && err == nil {
	//	fmt.Printf("Successfully joined!\n")
	//} else {
	//	fmt.Printf("Fail!\n")
	//}
	return node
}

//maintain will periodically perform maintenance operations
func (node *ChordNode) maintain() {
	fmt.Printf("Maintaining...\n")
	for {
		//stabilize
		node.stabilize()
		time.Sleep(5 * time.Second)
		//check predecessor
		//update fingers
	}
}

//stablize ensures that the node's successor's predecessor is itself
//If not, it updates its successor's predecessor.
func (node *ChordNode) stabilize() {
	//check to see if successor is still around
	if node.successor == nil {
		return
	}

	//ask sucessor for predecessor
	msg := getpredMsg()
	reply, err := send(msg, node.successor.ipaddr)
	checkError(err)

	ft, err := parseFingers(reply)
	checkError(err)
	if ft != nil {
		predOfSucc := ft[0]
		if predOfSucc.id != node.id {
			if inRange(predOfSucc.id, node.id, node.successor.id) {
				*node.successor = predOfSucc
			}
		} else { //everything is fine
			return
		}
	}

	//claim to be predecessor of succ
	me := new(Finger)
	me.id = node.id
	me.ipaddr = node.ipaddr
	msg = claimpredMsg(*me)
	send(msg, node.successor.ipaddr)

}

func (node *ChordNode) notify(newPred Finger) {
	//update predecessor
	node.predecessor = new(Finger)
	*node.predecessor = newPred
	if node.successor == nil {
		node.successor = new(Finger)
		*node.successor = newPred
	}
	//notify applications
}

func (node *ChordNode) checkPred() {

}

func (node *ChordNode) updateFingers() {

}

func (node *ChordNode) Finalize() {
	fmt.Printf("Exiting...\n")
}

//inRange checks to see if the value x is in (min, max)
func inRange(x [32]byte, min [32]byte, max [32]byte) bool {
	//There are 3 cases: min < x and x < max,
	//x < max and max < min, max < min and min < x
	xint := new(big.Int)
	maxint := new(big.Int)
	minint := new(big.Int)
	xint.SetBytes(x[:32])
	minint.SetBytes(min[:32])
	maxint.SetBytes(max[:32])

	if xint.Cmp(minint) == 1 && maxint.Cmp(xint) == 1 {
		return true
	}

	if maxint.Cmp(xint) == 1 && minint.Cmp(maxint) == 1 {
		return true
	}

	if minint.Cmp(maxint) == 1 && xint.Cmp(minint) == 1 {
		return true
	}

	return false
}

func (f Finger) String() string {
	return fmt.Sprintf("%x \t %s", f.id, f.ipaddr)
}

func (node *ChordNode) Info() string {
	var succ, pred string
	if node.successor != nil {
		succ = node.successor.String()
	} else {
		succ = "Unknown"
	}
	if node.predecessor != nil {
		pred = node.predecessor.String()
	} else {
		pred = "Unknown"
	}
	return fmt.Sprintf("Successor: %s\nPredecessor: %s\n", succ, pred)
}
