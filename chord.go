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
	"github.com/golang/protobuf/proto"
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

func (node *ChordNode) Join(addr string) bool {
	//TODO: construct protobuf message
	msg := &chord.ChordMsg{
		Proto:   proto.String("Chord"),
		Command: proto.String("join"),
		Args:    proto.Uint64(node.id),
	}
	data, err := proto.Marshall(msg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
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
