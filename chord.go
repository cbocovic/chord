/* Package chord
 *
 * This package is a collection of structures and functions associated
 * with the Chord distributed lookup protocol.
 */

package chord

import (
	"crypto/sha256"
	"fmt"
	"math/big"
	"net"
	"os"
	"runtime/debug"
	"time"
)

//Finger type denoting identifying information about a ChordNode
type Finger struct {
	id     [sha256.Size]byte
	ipaddr string
}

type Request struct {
	write bool
	succ  bool
	index int
}

//ChordNode type denoting a Chord server. Each server has a predecessor, successor, fingertable
// containing information about log(N) other nodes in the network, identifier, and InternetAddress.
type ChordNode struct {
	predecessor   *Finger
	successor     *Finger
	successorList [sha256.Size * 8]Finger
	fingerTable   [sha256.Size*8 + 1]Finger

	finger  chan Finger
	request chan Request

	id     [sha256.Size]byte
	ipaddr string

	connections  map[string]net.Conn
	applications map[byte]ChordApp
}

//error checking function
func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s\n", err.Error())
	}
}

//Lookup returns the address of the ChordNode that is responsible
//for the key. The procedure begins at the address denoted by start.
func Lookup(key [sha256.Size]byte, start string) (addr string, err error) {

	addr = start

	msg := getfingersMsg()
	reply, err := send(msg, start)
	checkError(err)
	if err != nil { //node failed
		return
	}

	ft, err := parseFingers(reply)
	checkError(err)
	if err != nil {
		return
	}
	//fmt.Printf("received %d finger(s).\n", len(ft))
	if len(ft) < 2 {
		return
	}
	if key == ft[0].id {
		addr = ft[0].ipaddr
		return
	}

	//loop through finger table and see what the closest finger is
	for i := len(ft) - 1; i > 0; i-- {
		f := ft[i]
		if i == 0 {
			break
		}
		if inRange(f.id, ft[0].id, key) { //see if f.id is closer than I am.
			//fmt.Printf("Key %x: found closer node: %s.\n", key, f.ipaddr)
			addr, err = Lookup(key, f.ipaddr)
			if err != nil { //node failed
				continue
			}
			return
		}
	}
	addr = ft[1].ipaddr

	return
}

//Lookup returns the address of the ChordNode that is responsible
//for the key. The procedure begins at the address denoted by start.
func (node *ChordNode) lookup(key [sha256.Size]byte, start string) (addr string, err error) {

	addr = start

	msg := getfingersMsg()
	reply, err := node.send(msg, start)
	checkError(err)
	if err != nil { //node failed
		return
	}

	ft, err := parseFingers(reply)
	checkError(err)
	if err != nil {
		return
	}
	//fmt.Printf("received %d finger(s).\n", len(ft))
	if len(ft) < 2 {
		return
	}
	if key == ft[0].id {
		addr = ft[0].ipaddr
		return
	}

	//loop through finger table and see what the closest finger is
	for i := len(ft) - 1; i > 0; i-- {
		f := ft[i]
		if i == 0 {
			break
		}
		if inRange(f.id, ft[0].id, key) { //see if f.id is closer than I am.
			//fmt.Printf("Key %x: found closer node: %s.\n", key, f.ipaddr)
			addr, err = node.lookup(key, f.ipaddr)
			if err != nil { //node failed
				continue
			}
			return
		}
	}
	addr = ft[1].ipaddr

	return
}

//Create will start a new Chord ring and return the original ChordNode
func Create(myaddr string) *ChordNode {
	node := new(ChordNode)
	//initialize node information
	node.id = sha256.Sum256([]byte(myaddr))
	node.ipaddr = myaddr
	me := new(Finger)
	me.id = node.id
	me.ipaddr = node.ipaddr
	node.fingerTable[0] = *me
	succ := new(Finger)
	node.successor = succ
	pred := new(Finger)
	node.predecessor = pred

	//set up channels for finger manager
	c := make(chan Finger)
	c2 := make(chan Request)
	node.finger = c
	node.request = c2

	//initialize listener and network manager threads
	node.listen(myaddr)
	node.connections = make(map[string]net.Conn)
	node.applications = make(map[byte]ChordApp)

	//initialize maintenance and finger manager threads
	go node.data()
	go node.maintain()
	fmt.Printf("Exiting create.\n")
	return node
}

//Join will add a ChordNode to the network from an existing node
//specified by addr.
func Join(myaddr string, addr string) *ChordNode {
	node := Create(myaddr)
	fmt.Printf("Finished creating node. Now to join...\n")

	fmt.Printf("looking up %x at %s.\n", node.id, addr)
	successor, err := Lookup(node.id, addr)
	checkError(err)
	if successor == "" {
		debug.PrintStack()
		panic("in JOIN AHH")
	}

	//find id of node
	msg := getidMsg()
	reply, err := send(msg, successor)
	checkError(err)

	//update node info to include successor
	succ := new(Finger)
	succ.id, _ = parseId(reply)
	//fmt.Printf("Found successor: %x.\n", succ.id)
	succ.ipaddr = successor
	node.query(true, false, 1, succ)

	return node
}

//data manages reads and writes to the node data structure
func (node *ChordNode) data() {
	//fmt.Printf("Node %s data manager is ready.\n", node.ipaddr)
	for {
		req := <-node.request
		//fmt.Printf("Node %s processing query.\n", node.ipaddr)
		if req.write {
			if req.succ {
				node.successorList[req.index] = <-node.finger
			} else {
				if req.index < 0 {
					*node.predecessor = <-node.finger
				} else if req.index == 1 {
					*node.successor = <-node.finger
					node.fingerTable[1] = *node.successor
					node.successorList[0] = *node.successor
				} else {
					node.fingerTable[req.index] = <-node.finger
				}
			}
		} else { //req.read
			if req.succ {
				node.finger <- node.successorList[req.index]
			} else {
				if req.index < 0 {
					node.finger <- *node.predecessor
				} else {
					node.finger <- node.fingerTable[req.index]
				}
			}
		}
	}
}

//query allows functions to read from or write to the node object
func (node *ChordNode) query(write bool, succ bool, index int, newf *Finger) Finger {
	f := new(Finger)
	req := Request{write, succ, index}
	node.request <- req
	//fmt.Printf("Node %s sent query.\n", node.ipaddr)
	if write {
		node.finger <- *newf
	} else {
		*f = <-node.finger
	}

	//fmt.Printf("Node %s has received query response.\n", node.ipaddr)
	return *f
}

//maintain will periodically perform maintenance operations
func (node *ChordNode) maintain() {
	fmt.Printf("Node %s maintaining.\n", node.ipaddr)
	ctr := 0
	for {
		time.Sleep(100 * time.Millisecond)
		//stabilize
		node.stabilize()
		//check predecessor
		node.checkPred()
		//update fingers
		node.fix(ctr)
		ctr = ctr % 256
		ctr += 1
	}
}

//stablize ensures that the node's successor's predecessor is itself
//If not, it updates its successor's predecessor.
func (node *ChordNode) stabilize() {
	//fmt.Printf("Node %s is stabilizing...\n", node.ipaddr)
	successor := node.query(false, false, 1, nil)

	if successor.zero() {
		return
	}

	//check to see if successor is still around
	msg := pingMsg()
	reply, err := node.send(msg, successor.ipaddr)
	if err != nil {
		//successor failed to respond
		successor = node.query(false, true, 1, nil)
		node.query(true, false, 1, &successor)
		if successor.ipaddr == node.ipaddr {
			successor.ipaddr = ""
			node.query(true, false, 1, &successor)
			return
		}
		return
	}

	//everything is OK, update successor list
	msg = getsuccessorsMsg()
	reply, err = node.send(msg, successor.ipaddr)
	if err != nil {
		return
	}
	ft, err := parseFingers(reply)
	if err != nil {
		return
	}
	for i := range ft {
		if i < sha256.Size*8-1 {
			node.query(true, true, i+1, &ft[i])
		}
	}

	//ask sucessor for predecessor
	msg = getpredMsg()
	reply, err = node.send(msg, successor.ipaddr)
	if err != nil {
		return
	}

	predOfSucc, err := parseFinger(reply)
	if err != nil { //node failed
		return
	}
	if predOfSucc.ipaddr != "" {
		if predOfSucc.id != node.id {
			if inRange(predOfSucc.id, node.id, successor.id) {
				node.query(true, false, 1, &predOfSucc)
			}
		} else { //everything is fine
			return
		}
	}

	//claim to be predecessor of succ
	me := new(Finger)
	me.id = node.id
	me.ipaddr = node.ipaddr
	msg = claimpredMsg(*me)
	node.send(msg, successor.ipaddr)

}

//Register allows chord applications to receive notifications
//and messages through Chord
func (node *ChordNode) Register(id byte, app ChordApp) bool {
	if _, ok := node.applications[id]; ok {
		fmt.Printf("Could not register application with id %d.\n", id)
		return false
	}
	node.applications[id] = app
	return true

}

func (node *ChordNode) notify(newPred Finger) {
	node.query(true, false, -1, &newPred)
	//fmt.Printf("Updating predecessor...\n")
	//update predecessor
	successor := node.query(false, false, 1, nil)
	if successor.zero() { //TODO: so if you get here, you were probably the first node.
		node.query(true, false, 1, &newPred)
	}
	//notify applications
	for _, app := range node.applications {
		app.Notify(newPred.id[:32], node.id[:32])
	}
}

func (node *ChordNode) checkPred() {
	//fmt.Printf("Checking predecessor.\n")
	predecessor := node.query(false, false, -1, nil)
	if predecessor.zero() {
		return
	}

	msg := pingMsg()
	reply, err := node.send(msg, predecessor.ipaddr)
	if err != nil {
		//fmt.Printf("Node %s setting pred back to nil.\n", node.ipaddr)
		predecessor.ipaddr = ""
		node.query(true, false, -1, &predecessor)
	}

	if success, err := parsePong(reply); !success || err != nil {
		//fmt.Printf("Node %s setting pred back to nil.\n", node.ipaddr)
		predecessor.ipaddr = ""
		node.query(true, false, -1, &predecessor)
	}

	return

}

func (node *ChordNode) fix(which int) {
	successor := node.query(false, false, 1, nil)
	if which == 0 || which == 1 || successor.zero() {
		return
	}
	var targetId [sha256.Size]byte
	copy(targetId[:sha256.Size], target(node.id, which)[:sha256.Size])
	//fmt.Printf("Node %s is looking for target %x.\n", node.ipaddr, targetId)
	newip, err := node.lookup(targetId, successor.ipaddr)
	if err != nil { //node failed: TODO make more robust
		successor = node.query(false, true, 1, nil)
		newip, err = node.lookup(targetId, successor.ipaddr)
	}
	if err != nil || newip == node.ipaddr {
		return
	}
	//fmt.Printf("Target %x belongs to %s.\n", targetId, newip)

	//find id of node
	msg := getidMsg()
	reply, err := node.send(msg, newip)
	if err != nil {
		return
	}

	newfinger := new(Finger)
	newfinger.ipaddr = newip
	newfinger.id, _ = parseId(reply)
	//fmt.Printf("Node %s updating finger %d: %s.\n", node.ipaddr, which, newfinger.ipaddr)
	node.query(true, false, which, newfinger)

}

func (node *ChordNode) Finalize() {
	//send message to all children to terminate

	fmt.Printf("Exiting...\n")
}

//inRange checks to see if the value x is in (min, max)
func inRange(x [sha256.Size]byte, min [sha256.Size]byte, max [sha256.Size]byte) bool {
	//There are 3 cases: min < x and x < max,
	//x < max and max < min, max < min and min < x
	xint := new(big.Int)
	maxint := new(big.Int)
	minint := new(big.Int)
	xint.SetBytes(x[:sha256.Size])
	minint.SetBytes(min[:sha256.Size])
	maxint.SetBytes(max[:sha256.Size])

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

//target returns the target id used by the fix function
func target(me [sha256.Size]byte, which int) []byte {
	meint := new(big.Int)
	meint.SetBytes(me[:sha256.Size])

	baseint := new(big.Int)
	baseint.SetUint64(2)

	powint := new(big.Int)
	powint.SetInt64(int64(which - 1))

	var biggest [sha256.Size + 1]byte
	for i := range biggest {
		biggest[i] = 255
	}

	tmp := new(big.Int)
	tmp.SetInt64(1)

	modint := new(big.Int)
	modint.SetBytes(biggest[:sha256.Size])
	modint.Add(modint, tmp)

	target := new(big.Int)
	target.Exp(baseint, powint, modint)
	target.Add(meint, target)
	target.Mod(target, modint)

	bytes := target.Bytes()
	diff := sha256.Size - len(bytes)
	if diff > 0 {
		tmp := make([]byte, sha256.Size)
		//pad with zeros
		for i := 0; i < diff; i++ {
			tmp[i] = 0
		}
		for i := diff; i < sha256.Size; i++ {
			tmp[i] = bytes[i-diff]
		}
		fmt.Printf("Padded %x to %x.\n", bytes, tmp)
		bytes = tmp
	}
	return bytes[:sha256.Size]
}

func (f Finger) String() string {
	return fmt.Sprintf("%s", f.ipaddr)
}

func (f Finger) zero() bool {
	if f.ipaddr == "" {
		return true
	} else {
		return false
	}
}

/** Printouts of information **/

func (node *ChordNode) Info() string {
	var succ, pred string
	successor := node.query(false, false, 1, nil)
	predecessor := node.query(false, false, -1, nil)
	if !successor.zero() {
		succ = successor.String()
	} else {
		succ = "Unknown"
	}
	if !predecessor.zero() {
		pred = predecessor.String()
	} else {
		pred = "Unknown"
	}
	return fmt.Sprintf("%x\t%s\t%s\n", node.id, succ, pred)
}

func (node *ChordNode) ShowFingers() string {
	retval := ""
	finger := new(Finger)
	prevfinger := new(Finger)
	ctr := 0
	for i := 0; i < sha256.Size*8+1; i++ {
		*finger = node.query(false, false, i, nil)
		if !finger.zero() {
			ctr += 1
			if i == 0 || finger.ipaddr != prevfinger.ipaddr {
				retval += fmt.Sprintf("%s\n", finger.String())
			}
		}
		*prevfinger = *finger
	}
	return retval + fmt.Sprintf("Total fingers: %d.\n", ctr)
}

func (node *ChordNode) ShowSucc() string {
	table := ""
	finger := new(Finger)
	prevfinger := new(Finger)
	for i := 0; i < sha256.Size*8; i++ {
		*finger = node.query(false, true, i, nil)
		if finger.ipaddr != "" {
			if i == 0 || finger.ipaddr != prevfinger.ipaddr {
				table += fmt.Sprintf("%s\n", finger.String())
			}
		}
		*prevfinger = *finger
	}
	return table
}

/** Chord application interface and methods **/
type ChordApp interface {
	Notify(id []byte, me []byte) string
	Message(addr string, data string) string
}
