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
	"fmt"
)

const CHORDMSG byte = 01

//Finger type denoting identifying information about a ChordNode
type Finger struct {
	id     uint64
	ipaddr string
}

//ChordNode type denoting a Chord server. Each server has a predecessor, successor, fingertable
// containing information about log(N) other nodes in the network, identifier, and InternetAddress.
type ChordNode struct {
	predecessor Finger
	successor   Finger
	fingerTable [5]Finger

	id     uint64
	ipaddr string
}

//Send opens a connection to addr, sends msg, and then returns the
//reply
func Send(msg []byte, addr string) (reply string, err Error) {

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

//Join will add a ChordNode to the network from an existing node
//specified by addr.
func (node *ChordNode) Join(addr string) bool {
	//TODO: construct protobuf message
	//TODO: open a connection to addr
	conn, err := Dial("tcp", addr)
	if err != nil {
		//TODO: look up conventions on errors for Go.
		return false
	}
	n, err := conn.Write(data)
	if err != nil {
		return false
	}

	return true
}
