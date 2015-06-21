/**
 * This is a collection of possible messages to be sent over the CHORD
 * network.
 */

package chord

import (
	"fmt"
	"github.com/golang/protobuf/proto"
)

func lookupMsg(uint64 key) []byte {

	msg := &chord.ChordMsg{
		Proto:   proto.String("Chord"),
		Command: proto.String("join"),
		Args:    proto.Uint64(node.id),
	}
	data, err := proto.Marshall(msg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	return data

}
