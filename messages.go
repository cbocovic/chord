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
		Proto: proto.Uint32(1),
	}
	data, err := proto.Marshall(msg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	return data

}

func parseMessage(msg *NetworkMessage) {

	protocol := msg.GetProto()
	if protocol != 0 {
		//TODO: implement callbacks for applications. For now just returns
		return
	}

	//TODO: Implement a switch statement that checks to see what kind of message it is (only need to
	//handle commands)
	msg = msg.GetChordMessage()
	cmd = msg.GetCmd()
	switch {
	case cmd == Ping:
	case cmd == GetPred:
	case cmd == GetId:
	case cmd == GetFingers:
	case cmd == ClaimPred:
	}

}
