/**
 * This is a collection of possible messages to be sent over the CHORD
 * network.
 */

package chord

import (
	"github.com/golang/protobuf/proto"
	"log"
)

//lookupMsg constructs a message to perform the lookup of a key and returns the
//marshalled protocol buffer
func getfingersMsg() []byte {

	msg := new(NetworkMessage)
	msg.Proto = proto.Uint32(1)
	chordMsg := new(NetworkMessage_ChordMessage)
	*chordMsg.Cmd = NetworkMessage_GetFingers
	msg.Msg = chordMsg

	data, err := proto.Marshal(msg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	return data
}

func sendfingersMsg(fingers []Finger) []byte {

	msg := new(NetworkMessage)
	msg.Proto = proto.Uint32(1)
	chordMsg := new(NetworkMessage_ChordMessage)
	*chordMsg.Cmd = NetworkMessage_GetFingers
	sfMsg := new(SendFingersMessage)
	for _, finger := range fingers {
		fingerMsg := new(FingerMessage)
		fingerMsg.Id = proto.String(string(finger.id[:32]))
		fingerMsg.Address = proto.String(finger.ipaddr)
		sfMsg.Fingers = append(sfMsg.Fingers, fingerMsg)
	}
	chordMsg.Sfmsg = sfMsg
	msg.Msg = chordMsg

	data, err := proto.Marshal(msg)
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
	*chordMsg.Cmd = NetworkMessage_GetId
	msg.Msg = chordMsg

	data, err := proto.Marshal(msg)
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
	*chordMsg.Cmd = NetworkMessage_GetId
	sidMsg := new(SendIdMessage)
	sidMsg.Id = proto.String(string(id))
	chordMsg.Sidmsg = sidMsg
	msg.Msg = chordMsg

	data, err := proto.Marshal(msg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	return data
}

//TODO: rewrite
func getpredMsg() []byte {
	msg := new(NetworkMessage)
	msg.Proto = proto.Uint32(1)
	chordMsg := new(NetworkMessage_ChordMessage)
	*chordMsg.Cmd = NetworkMessage_ClaimPred
	msg.Msg = chordMsg

	data, err := proto.Marshal(msg)

	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	return data
}

//TODO: rewrite
func sendpredMsg() []byte {
	msg := new(NetworkMessage)
	msg.Proto = proto.Uint32(1)
	chordMsg := new(NetworkMessage_ChordMessage)
	*chordMsg.Cmd = NetworkMessage_ClaimPred
	msg.Msg = chordMsg

	data, err := proto.Marshal(msg)

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
	*chordMsg.Cmd = NetworkMessage_Ping
	msg.Msg = chordMsg

	data, err := proto.Marshal(msg)
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
	*chordMsg.Cmd = NetworkMessage_Pong
	msg.Msg = chordMsg

	data, err := proto.Marshal(msg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	return data
}

//parseMessage takes as input an unmarshalled protocol buffer and
//performs actions based on what the message contains.
func (node *ChordNode) parseMessage(data []byte, c chan []byte) {

	msg := new(NetworkMessage)

	err := proto.Unmarshal(data, msg)
	checkError(err)

	protocol := msg.GetProto()
	if protocol != 0 {
		//TODO: implement callbacks for applications. For now just returns
		return
	}

	chordmsg := msg.GetMsg()
	cmd := chordmsg.GetCmd()
	switch {
	case cmd == NetworkMessage_Ping:
		c <- pongMsg()
	case cmd == NetworkMessage_GetPred:
		c <- sendpredMsg() //node.predecessor)
	case cmd == NetworkMessage_GetId:
		c <- sendidMsg(node.id[:32])
	case cmd == NetworkMessage_GetFingers:
		c <- sendfingersMsg(node.fingerTable[:32])
	case cmd == NetworkMessage_ClaimPred:
		//update finger table

	}
}

//parseFingers can be called to return a finger table from a received
//message after a getfingers call.
func parseFingers(data []byte) (ft []Finger, err error) {
	msg := new(NetworkMessage)
	err = proto.Unmarshal(data, msg)
	if msg.GetProto() != 0 {
		//TODO: return non-nil error
		return
	}
	chordmsg := msg.GetMsg()
	sfmsg := chordmsg.GetSfmsg()
	fingers := sfmsg.GetFingers()
	for _, finger := range fingers {
		newfinger := new(Finger)
		copy(newfinger.id[:], []byte(*finger.Id))
		newfinger.ipaddr = *finger.Address
		ft = append(ft, *newfinger)
	}
	return
}

func parseId(data []byte) (id [32]byte, err error) {
	msg := new(NetworkMessage)
	err = proto.Unmarshal(data, msg)
	if msg.GetProto() != 0 {
		//TODO: return non-nil error
		return
	}
	chordmsg := msg.GetMsg()
	idmsg := chordmsg.GetSidmsg()
	arr := []byte(idmsg.GetId())
	copy(id[:], arr[:32])
	return
}

func parsePong(data []byte) (success bool, err error) {

	msg := new(NetworkMessage)
	err = proto.Unmarshal(data, msg)
	if msg.GetProto() != 0 {
		//TODO: return non-nil error
		return
	}
	chordmsg := msg.GetMsg()
	command := chordmsg.GetCmd()
	if command == NetworkMessage_Pong {
		success = true
	} else {
		success = false
	}

	return
}
