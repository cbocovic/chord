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
	fingerTable [5]Finger

	id     [32]byte
	ipaddr string

	//channels for listener/processor/network maintenance routines
	listener    chan string
	maintenance chan string
}

//Lookup returns the address of the ChordNode that is responsible
//for the key. The procedure begins at the address denoted by start.
func Lookup(key uint64, start string) (addr string, err Error) {

	//TODO: construct protobuf message
	msg := lookupMsg(key)
	reply, err := Send(msg, start)
	if err != nil {
		return null, err
	}

	addr = start

	return
}

//Create will start a new Chord ring and return the original ChordNode
func Create() *ChordNode {
	node := new(ChordNode)
	return &node
}

//Join will add a ChordNode to the network from an existing node
//specified by addr.
func Join(addr string) *ChordNode {
	node := new(ChordNode)
	//TODO:set up identifier

	//TODO:set up listener
	//TODO:set up maintenance
	//TODO:lookup id in ring
	return node
}
