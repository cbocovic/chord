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
	"crypto/sha256"
	"fmt"
	"math/big"
	"os"
	"time"
)

//Finger type denoting identifying information about a ChordNode
type Finger struct {
	id     [sha256.Size]byte
	ipaddr string
}

//ChordNode type denoting a Chord server. Each server has a predecessor, successor, fingertable
// containing information about log(N) other nodes in the network, identifier, and InternetAddress.
type ChordNode struct {
	predecessor   *Finger
	successor     *Finger
	successorList [sha256.Size * 8]Finger
	fingerTable   [sha256.Size*8 + 1]Finger

	id     [sha256.Size]byte
	ipaddr string
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
func Lookup(key [sha256.Size]byte, start string) (addr string, err error) {

	addr = start

	msg := getfingersMsg()
	reply, err := send(msg, start)
	if err != nil { //node failed
		return
	}

	ft, err := parseFingers(reply)
	checkError(err)

	//loop through finger table and see what the closest finger is
	for i := len(ft) - 1; i > 0; i-- {
		f := ft[i]
		if i == 0 {
			break
		}
		if inRange(f.id, ft[0].id, key) { //see if f.id is closer than I am.
			addr, err = Lookup(key, f.ipaddr)
			if err != nil { //node failed
				continue
			}
			return
		}
	}

	return
}

//Create will start a new Chord ring and return the original ChordNode
func Create(myaddr string) *ChordNode {
	node := new(ChordNode)
	//create id by hashing ipaddr
	node.id = sha256.Sum256([]byte(myaddr))
	node.ipaddr = myaddr
	me := new(Finger)
	me.id = node.id
	me.ipaddr = node.ipaddr
	node.fingerTable[0] = *me
	fmt.Printf("Created node with id: %x\n", node.id)
	node.listen(myaddr)
	go node.maintain()
	fmt.Printf("Exiting create.\n")
	return node
}

//Join will add a ChordNode to the network from an existing node
//specified by addr.
func Join(myaddr string, addr string) *ChordNode {
	node := Create(myaddr)
	fmt.Printf("Finished creating node. Now to join...\n")

	successor, err := Lookup(node.id, addr)
	checkError(err)

	//find id of node
	msg := getidMsg()
	reply, err := send(msg, successor)
	checkError(err)

	//update node info to include successor
	succ := new(Finger)
	succ.id, _ = parseId(reply)
	fmt.Printf("Found successor: %x.\n", succ.id)
	succ.ipaddr = successor
	node.successor = succ
	node.fingerTable[1] = *succ
	node.successorList[0] = *succ

	return node
}

//maintain will periodically perform maintenance operations
func (node *ChordNode) maintain() {
	fmt.Printf("Maintaining...\n")
	ctr := 0
	for {
		time.Sleep(5 * time.Second)
		//stabilize
		node.stabilize()
		//check predecessor
		node.checkPred()
		//update fingers
		node.fix(ctr)
		ctr = (ctr + 1) % 256
	}
}

//stablize ensures that the node's successor's predecessor is itself
//If not, it updates its successor's predecessor.
func (node *ChordNode) stabilize() {
	if node.successor == nil {
		return
	}

	//check to see if successor is still around
	msg := pingMsg()
	reply, err := send(msg, node.successor.ipaddr)
	if err != nil {
		//successor failed to respond
		*node.successor = node.successorList[1]
		node.successorList[0] = *node.successor
		node.fingerTable[1] = *node.successor
		fmt.Printf("Updated successor.\n")
		return
	}

	//everything is OK, update successor list
	fmt.Printf("Updating successor list...")
	msg = getsuccessorsMsg()
	reply, err = send(msg, node.successor.ipaddr)
	checkError(err)
	ft, err := parseFingers(reply)
	checkError(err)
	for i := range ft {
		if i < sha256.Size {
			node.successorList[i+1] = ft[i]
		}
	}

	//ask sucessor for predecessor
	msg = getpredMsg()
	reply, err = send(msg, node.successor.ipaddr)
	checkError(err)

	predOfSucc, err := parseFinger(reply)
	checkError(err)
	if predOfSucc.ipaddr != "" {
		if predOfSucc.id != node.id {
			if inRange(predOfSucc.id, node.id, node.successor.id) {
				*node.successor = predOfSucc
				node.fingerTable[1] = predOfSucc
				node.successorList[0] = predOfSucc
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
	send(msg, node.successor.ipaddr)

}

func (node *ChordNode) notify(newPred Finger) {
	//update predecessor
	node.predecessor = new(Finger)
	*node.predecessor = newPred
	if node.successor == nil { //TODO: so if you get here, you were probably the first node.
		node.successor = new(Finger)
		*node.successor = newPred
	}
	//notify applications
}

func (node *ChordNode) checkPred() {
	fmt.Printf("Checking predecessor.\n")
	if node.predecessor == nil {
		return
	}

	msg := pingMsg()
	reply, err := send(msg, node.predecessor.ipaddr)
	if err != nil {
		node.predecessor = nil
	}

	if success, err := parsePong(reply); !success || err != nil {
		node.predecessor = nil
	}

	return

}

func (node *ChordNode) fix(which int) {
	fmt.Printf("Fixing finger %d\n.", which)
	if which == 0 || which == 1 || node.successor == nil {
		return
	}
	var targetId [sha256.Size]byte
	copy(targetId[:sha256.Size], target(node.id, which)[:sha256.Size])
	newip, err := Lookup(targetId, node.successor.ipaddr)
	if err != nil { //node failed: TODO make more robust
		newip, err = Lookup(targetId, node.successorList[1].ipaddr)
	}
	checkError(err)

	//find id of node
	msg := getidMsg()
	reply, err := send(msg, newip)
	checkError(err)

	newfinger := new(Finger)
	newfinger.ipaddr = newip
	newfinger.id, _ = parseId(reply)
	node.fingerTable[which] = *newfinger

}

func (node *ChordNode) Finalize() {
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

	var biggest [sha256.Size]byte
	for i := range biggest {
		biggest[i] = 255
	}

	modint := new(big.Int)
	modint.SetBytes(biggest[:sha256.Size])

	target := new(big.Int)
	target.Exp(baseint, powint, modint)
	target.Add(meint, target)
	return target.Bytes()[:sha256.Size]
}

func (f Finger) String() string {
	return fmt.Sprintf("%x \t %s", f.id, f.ipaddr)
}

func (node *ChordNode) Info() string {
	var succ, pred string
	if node.successor != nil {
		succ = node.successor.String()
	} else {
		succ = "Unknown"
	}
	if node.predecessor != nil {
		pred = node.predecessor.String()
	} else {
		pred = "Unknown"
	}
	return fmt.Sprintf("Successor: %s\nPredecessor: %s\n", succ, pred)
}

func (node *ChordNode) ShowFingers() string {
	table := ""
	for i, finger := range node.fingerTable {
		if finger.ipaddr != "" {
			if i == 0 || finger.ipaddr != node.fingerTable[i-1].ipaddr {
				table += fmt.Sprintf("%s\n", finger.String())
			}
		}
	}
	return table
}

func (node *ChordNode) ShowSucc() string {
	table := ""
	for i, finger := range node.successorList {
		if finger.ipaddr != "" {
			if i == 0 || finger.ipaddr != node.successorList[i-1].ipaddr {
				table += fmt.Sprintf("%s\n", finger.String())
			}
		}
	}
	return table
}
