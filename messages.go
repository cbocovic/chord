/**
 * This is a collection of possible messages to be sent over the CHORD
 * network.
 */

package chord

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"log"
)

//lookupMsg constructs a message to perform the lookup of a key and returns the
//marshalled protocol buffer
func getfingersMsg() []byte {

	msg := new(NetworkMessage)
	msg.Proto = proto.Uint32(1)
	chordMsg := new(NetworkMessage_ChordMessage)
	command := NetworkMessage_Command(NetworkMessage_Command_value["GetFingers"])
	chordMsg.Cmd = &command
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
	command := NetworkMessage_Command(NetworkMessage_Command_value["GetFingers"])
	chordMsg.Cmd = &command
	sfMsg := new(SendFingersMessage)
	for _, finger := range fingers {
		if !finger.zero() {
			fingerMsg := new(FingerMessage)
			fingerMsg.Id = proto.String(string(finger.id[:32]))
			fingerMsg.Address = proto.String(finger.ipaddr)
			sfMsg.Fingers = append(sfMsg.Fingers, fingerMsg)
		}
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
	command := NetworkMessage_Command(NetworkMessage_Command_value["GetId"])
	chordMsg.Cmd = &command
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
	command := NetworkMessage_Command(NetworkMessage_Command_value["GetId"])
	chordMsg.Cmd = &command
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
	command := NetworkMessage_Command(NetworkMessage_Command_value["GetPred"])
	chordMsg.Cmd = &command
	msg.Msg = chordMsg

	data, err := proto.Marshal(msg)

	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	return data
}

//TODO: rewrite
func sendpredMsg(finger Finger) []byte {
	msg := new(NetworkMessage)
	msg.Proto = proto.Uint32(1)
	chordMsg := new(NetworkMessage_ChordMessage)
	command := NetworkMessage_Command(NetworkMessage_Command_value["GetPred"])
	chordMsg.Cmd = &command
	pMsg := new(PredMessage)
	fingerMsg := new(FingerMessage)
	fingerMsg.Id = proto.String(string(finger.id[:32]))
	fingerMsg.Address = proto.String(finger.ipaddr)
	pMsg.Pred = fingerMsg
	chordMsg.Cpmsg = pMsg
	msg.Msg = chordMsg

	data, err := proto.Marshal(msg)

	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	return data
}

func claimpredMsg(finger Finger) []byte {
	msg := new(NetworkMessage)
	msg.Proto = proto.Uint32(1)
	chordMsg := new(NetworkMessage_ChordMessage)
	command := NetworkMessage_Command(NetworkMessage_Command_value["ClaimPred"])
	chordMsg.Cmd = &command
	predMsg := new(PredMessage)
	fingerMsg := new(FingerMessage)
	fingerMsg.Id = proto.String(string(finger.id[:32]))
	fingerMsg.Address = proto.String(finger.ipaddr)
	predMsg.Pred = fingerMsg
	chordMsg.Cpmsg = predMsg
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
	command := NetworkMessage_Command(NetworkMessage_Command_value["Ping"])
	chordMsg.Cmd = &command
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
	command := NetworkMessage_Command(NetworkMessage_Command_value["Pong"])
	chordMsg.Cmd = &command
	msg.Msg = chordMsg

	data, err := proto.Marshal(msg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	return data
}

func getsuccessorsMsg() []byte {
	msg := new(NetworkMessage)
	msg.Proto = proto.Uint32(1)
	chordMsg := new(NetworkMessage_ChordMessage)
	command := NetworkMessage_Command(NetworkMessage_Command_value["GetSucc"])
	chordMsg.Cmd = &command
	msg.Msg = chordMsg

	data, err := proto.Marshal(msg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	return data

}

func nullMsg() []byte {
	msg := new(NetworkMessage)
	msg.Proto = proto.Uint32(1)

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
	if err != nil {
		fmt.Printf("Uh oh in parse message of node %s\n", node.ipaddr)
		return
	}

	protocol := msg.GetProto()
	if protocol != 1 {
		//TODO: implement callbacks for applications. For now just returns
		return
	}

	chordmsg := msg.GetMsg()
	cmd := int32(chordmsg.GetCmd())
	switch {
	case cmd == NetworkMessage_Command_value["Ping"]:
		c <- pongMsg()
		return
	case cmd == NetworkMessage_Command_value["GetPred"]:
		node.request <- Request{false, false, -1}
		pred := <-node.finger
		if pred.zero() {
			c <- nullMsg()
		} else {
			c <- sendpredMsg(pred) //node.predecessor)
		}
		return
	case cmd == NetworkMessage_Command_value["GetId"]:
		c <- sendidMsg(node.id[:32])
		return
	case cmd == NetworkMessage_Command_value["GetFingers"]:
		table := make([]Finger, 32*8+1)
		//fmt.Printf("Fingers of node %s:\n", node.ipaddr)
		for i := range table {
			node.request <- Request{false, false, i}
			f := <-node.finger
			//fmt.Printf("\t%s\n", f.String())
			table[i] = f
		}

		c <- sendfingersMsg(table)
		return
	case cmd == NetworkMessage_Command_value["ClaimPred"]:
		//extract finger
		newPred, err := parseFinger(data)
		checkError(err)
		if err != nil {
			c <- nullMsg()
			break
		}
		node.request <- Request{false, false, -1}
		pred := <-node.finger

		if pred.zero() || inRange(newPred.id, pred.id, node.id) {
			node.notify(newPred)
		}
		c <- nullMsg()
		//update finger table
		return
	case cmd == NetworkMessage_Command_value["GetSucc"]:
		table := make([]Finger, 32*8)
		for i := range table {
			node.request <- Request{false, true, i}
			f := <-node.finger
			table[i] = f
		}

		c <- sendfingersMsg(table)
		return

	}
	fmt.Printf("No matching commands.\n")
}

//parseFingers can be called to return a finger table from a received
//parseFingers can be called to return a finger table from a received
//message after a getfingers call.
func parseFingers(data []byte) (ft []Finger, err error) {
	msg := new(NetworkMessage)
	err = proto.Unmarshal(data, msg)
	if msg.GetProto() != 1 {
		//TODO: return non-nil error
		return
	}
	chordmsg := msg.GetMsg()
	if chordmsg == nil {
		return
	}
	sfmsg := chordmsg.GetSfmsg()
	fingers := sfmsg.GetFingers()
	prevfinger := new(Finger)
	for _, finger := range fingers {
		newfinger := new(Finger)
		copy(newfinger.id[:], []byte(*finger.Id))
		newfinger.ipaddr = *finger.Address
		if !newfinger.zero() && newfinger.ipaddr != prevfinger.ipaddr {
			ft = append(ft, *newfinger)
		}
		*prevfinger = *newfinger
	}
	return
}

func parseFinger(data []byte) (f Finger, err error) {
	msg := new(NetworkMessage)
	err = proto.Unmarshal(data, msg)
	if msg.GetProto() != 1 {
		//TODO: return non-nil error
		return
	}
	chordmsg := msg.GetMsg()
	if chordmsg == nil { //then received null msg instead. return nil
		return
	}
	cpmsg := chordmsg.GetCpmsg()
	finger := cpmsg.GetPred()
	copy(f.id[:], []byte(*finger.Id))
	f.ipaddr = *finger.Address

	return
}

func parseId(data []byte) (id [32]byte, err error) {
	msg := new(NetworkMessage)
	err = proto.Unmarshal(data, msg)
	if msg.GetProto() != 1 {
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
	checkError(err)
	if err != nil {
		return false, err
	}

	if msg.GetProto() != 1 {
		//TODO: return non-nil error
		fmt.Printf("Something went wrong!\n")
		return
	}
	chordmsg := msg.GetMsg()
	command := int32(chordmsg.GetCmd())
	if command == NetworkMessage_Command_value["Pong"] {
		success = true
	} else {
		success = false
	}

	return
}
