/**
 * This is a collection of possible messages to be sent over the CHORD
 * network.
 */

package chord

import (
	"fmt"
	"github.com/golang/protobuf/proto"
)

//lookupMsg constructs a message to perform the lookup of a key and returns the
//marshalled protocol buffer
func getfingersMsg() []byte {

	msg := new(NetworkMessage)
	msg.Proto = proto.Uint32(1)
	chordMsg := new(NetworkMessage_ChordMessage)
	chordMsg.cmd = NetworkMessage_GetFingers
	msg.Msg = chordMsg

	data, err := proto.Marshall(msg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	return data
}

func sendfingersMsg(fingers []Finger) []byte {

	msg := new(NetworkMessage)
	msg.Proto = proto.Uint32(1)
	chordMsg := new(NetworkMessage_ChordMessage)
	chordMsg.cmd = NetworkMessage_GetFingers
	sfMsg := new(SendFingersMessage)
	for _, finger := range fingers {
		fingerMsg := new(FingerMessage)
		fingerMsg.id = proto.String(finger.id)
		fingerMsg.address = proto.String(finger.ipaddr)
		sgMsg.fingers = append(sfMsg.fingers, fingerMsg)
	}
	chordMsg.sfmsg = sgMsg
	msg.Msg = chordMsg

	data, err := proto.Marshall(msg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	return data
}

//getidMsg constructs a message to ask a server for its chord id
func getidMsg() []byte {

	msg := new(NetworkMessage)
	msg.Proto = proto.Uint32(1)
	chordMsg := new(NetworkMessage_ChordMessage)
	chordMsg.cmd = NetworkMessage_GetId
	msg.Msg = chordMsg

	data, err := proto.Marshall(msg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	return data
}

//sendidMsg constructs a message to ask a server for its chord id
func sendidMsg(id []byte) []byte {

	msg := new(NetworkMessage)
	msg.Proto = proto.Uint32(1)
	chordMsg := new(NetworkMessage_ChordMessage)
	chordMsg.cmd = NetworkMessage_GetId
	sidMsg := new(SendIdMessage)
	sidMsg.id = proto.String(id)
	chordMsg.sidmsg = sidMsg
	msg.Msg = chordMsg

	data, err := proto.Marshall(msg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	return data
}

//pingMsg constructs a message to ping a server
func pingMsg() []byte {

	msg := new(NetworkMessage)
	msg.Proto = proto.Uint32(1)
	chordMsg := new(NetworkMessage_ChordMessage)
	chordMsg.cmd = NetworkMessage_Ping
	msg.Msg = chordMsg

	data, err := proto.Marshall(msg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	return data
}

//pongMsg constructs a message to reply to a ping
func pongMsg() []byte {

	msg := new(NetworkMessage)
	msg.Proto = proto.Uint32(1)
	chordMsg := new(NetworkMessage_ChordMessage)
	chordMsg.cmd = NetworkMessage_Pong
	msg.Msg = chordMsg

	data, err := proto.Marshall(msg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	return data
}

//parseMessage takes as input an unmarshalled protocol buffer and
//performs actions based on what the message contains.
func (node *ChordNode) parseMessage(msg *NetworkMessage, c chan []byte) {

	msg := new(NetworkMessage)

	err = proto.Unmarshall(data, msg)
	checkError(err)

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
		c <- pongMsg()
	case cmd == GetPred:
		c <- sendpredMsg(node.predecessor)
	case cmd == GetId:
		c <- sendidMsg(node.id)
	case cmd == GetFingers:
		c <- sendfingersMsg(node.fingers)
	case cmd == ClaimPred:
		//update finger table

	}
}

//parseFingers can be called to return a finger table from a received
//message after a getfingers call.
func parseFingers(data []byte) (ft []Finger, err error) {
	msg := new(NetworkMessage)
	err = proto.Unmarshall(data, msg)
	if msg.GetProto() != 0 {
		//TODO: return non-nil error
		return
	}
	msg := msg.GetMsg()
	msg := msg.GetSfmsg()
	fingers := msg.GetFingers()
	for _, finger := range fingers {
		ft = append(ft, finger)
	}
	return
}
