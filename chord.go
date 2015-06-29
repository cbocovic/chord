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
)

const CHORDMSG byte = 01

//Finger type denoting identifying information about a ChordNode
type Finger struct {
	id     [32]byte
	ipaddr string
}

//ChordNode type denoting a Chord server. Each server has a predecessor, successor, fingertable
// containing information about log(N) other nodes in the network, identifier, and InternetAddress.
type ChordNode struct {
	predecessor Finger
	successor   Finger
	fingerTable [256]Finger

	id [32]byte
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
	fmt.Printf("Created node with id: %x\n", node.id)
	node.listen(myaddr)
	node.Maintain()
	return node
}

//Join will add a ChordNode to the network from an existing node
//specified by addr.
func Join(myaddr string, addr string) *ChordNode {
	node := Create(myaddr)

	//lookup id in ring
	successor, err := Lookup(node.id, addr)
	checkError(err)

	//find id of node
	msg := getidMsg()
	reply, err := send(msg, successor)
	checkError(err)

	//update node info to include successor
	succ := new(Finger)
	succ.id, _ = parseId(reply)
	succ.ipaddr = successor
	node.successor = *succ
	node.fingerTable[0] = *succ

	return node
}

//maintain will periodically perform maintenance operations
//TODO: make unexported
func (node *ChordNode) Maintain() {

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
