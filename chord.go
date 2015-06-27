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
	"crypto/rand"
	"crypto/sha256"
	"fmt"
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
func Lookup(key uint64, start string) (addr string, err Error) {

	addr = start

	msg := getfingersMsg(key)
	reply, err := Send(msg, start)
	if err != nil {
		return null, err
	}
	ft, err := parseFingers(reply)
	checkError(err)

	//loop through finger table and see what the closest finger is
	for i := ft.len; i > 0; i-- {

	}

	return
}

//Create will start a new Chord ring and return the original ChordNode
func Create(myaddr string) *ChordNode {
	node := new(ChordNode)
	//create id by hashing ipaddr
	node.id = sha256.Sum256([]byte(myaddr))
	fmt.Printf("Created node with id: %x\n", node.id)
	listen()
	node.maintain()
	return &node
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
	reply, err := Send(msg, successor)
	checkError(err)

	//update node info to include successor
	succ := new(Finger)
	succ.id = reply
	succ.ipaddr = sucessor
	node.sucessor = succ
	node.fingers[0] = succ

	return node
}

//maintain will periodically perform maintenance operations
func (node *ChordNode) maintain() {

}
