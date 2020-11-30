package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"


// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]


	done_map	[]int
	seq_DB		map[int]*SeqData
	min			int
	max			int
}

type SeqData struct {
	accepted_val 		interface{}
	HighestPromPropID	int64
	HighestAccPropID	int64
	Decided				bool
}

type PrepareArgs struct {
	From 	int
	Done	int
	Seq		int
	PropID	int64
}

type PrepareReply struct {
	Promise				bool
	AcceptedVal			interface{}
	HighestAccPropID	int64
	HighestPromPropID	int64
}

type AcceptArgs struct {
	From	int
	Done 	int
	Seq		int
	PropID	int64
	Value	interface{}
}

type AcceptReply struct {
	Accepted	bool
}

type DecidedArgs struct {
	From 	int
	Done	int
	Seq		int
	Value	interface{}
}

type DecidedReply struct {
	Unused	bool
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//makes a propID duh
func makePropID(myID int, offset int64) int64 {
	return ((offset << 32) | int64(myID))
}

//recieves Prepare RPCs
func (px *Paxos) PrepareHandler(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	//only listen if seq is not forgotten
	if args.Seq >= px.Min() {
		//update px.max if needed
		if args.Seq > px.max {
			px.max = args.Seq
		}
		
		//create a SeqData struct for this seq if it's new
		if _, exists := px.seq_DB[args.Seq]; !exists {
			px.seq_DB[args.Seq] = &SeqData{nil, -1, -1, false}
		}
		//if n > n_p : n_p = n; reply prepare_ok(n, n_a, v_a);
		if args.PropID > px.seq_DB[args.Seq].HighestPromPropID { 
			px.seq_DB[args.Seq].HighestPromPropID = args.PropID
			reply.Promise = true
		} else { //else reply prepare_reject
			reply.Promise = false
		}
		reply.HighestPromPropID = px.seq_DB[args.Seq].HighestPromPropID
		
		if px.seq_DB[args.Seq].HighestAccPropID > -1 {
			reply.AcceptedVal = px.seq_DB[args.Seq].accepted_val
		}
		reply.HighestAccPropID = px.seq_DB[args.Seq].HighestAccPropID
	}
	px.Done2(args.Done, args.From)
	px.mu.Unlock()
	return nil
}

//receives Accept RPCs
func (px *Paxos) AcceptHandler(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	//only listen if seq is not forgotten
	if args.Seq >= px.Min() {
		//update px.max if needed
		if args.Seq > px.max {
			px.max = args.Seq
		}
		
		//create a SeqData struct for this seq if it's new
		if _, exists := px.seq_DB[args.Seq]; !exists {
			px.seq_DB[args.Seq] = &SeqData{nil, -1, -1, false}
		}
		
		//if n >= n_p: n_p = n; n_a = n; v_a = v; reply accept_ok(n);
		if args.PropID >= px.seq_DB[args.Seq].HighestPromPropID {
			px.seq_DB[args.Seq].HighestPromPropID = args.PropID
			px.seq_DB[args.Seq].HighestAccPropID = args.PropID
			px.seq_DB[args.Seq].accepted_val = args.Value
			reply.Accepted = true
		} else { //else: reply accept_reject;
			reply.Accepted = false
		}
	}
	px.Done2(args.Done, args.From)
	px.mu.Unlock()
	return nil
}

//receives Decided RPCs
func (px *Paxos) DecidedHandler(args *DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	//only listen if seq is not forgotten
	if args.Seq >= px.Min() {
		//update px.max if needed
		if args.Seq > px.max {
			px.max = args.Seq
		}
		
		//create a SeqData struct for this seq if it's new
		if _, exists := px.seq_DB[args.Seq]; !exists {
			px.seq_DB[args.Seq] = &SeqData{nil, -1, -1, false}
		}
		
		px.seq_DB[args.Seq].Decided = true
		px.seq_DB[args.Seq].accepted_val = args.Value
		
		//propagate from here to get around bad network regions
		//px.spamDecided(args.Seq, args.Value)
	}
	px.Done2(args.Done, args.From)
	px.mu.Unlock()
	return nil
}

//sends a prepare message to everyone. EXPECTS ITS CALLER TO HOLD THE LOCK ALREADY
func (px *Paxos) spamPrepare(seq int, propID int64, c chan PrepareReply) {
	for i := 0; i < len(px.peers); i++ {
		go func(peerIndex int) {
			args := &PrepareArgs{px.me, px.done_map[px.me], seq, propID}
			reply := PrepareReply{}
			if peerIndex == px.me {
				px.PrepareHandler(args, &reply)
			} else {
				call(px.peers[peerIndex], "Paxos.PrepareHandler", args, &reply)
			}
			c <- reply
		}(i)
	}
}

//sends a accept message to everyone. EXPECTS ITS CALLER TO HOLD THE LOCK ALREADY
func (px *Paxos) spamAccept(seq int, propID int64, value interface{}, c chan AcceptReply) {
	for i := 0; i < len(px.peers); i++ {
		go func(peerIndex int) {
			args := &AcceptArgs{px.me, px.done_map[px.me], seq, propID, value}
			reply := AcceptReply{}
			if peerIndex == px.me {
				px.AcceptHandler(args, &reply)
			} else {
				call(px.peers[peerIndex], "Paxos.AcceptHandler", args, &reply)
			}
			c <- reply
		}(i)
	}
}

//sends a decided message to everyone. EXPECTS ITS CALLER TO HOLD THE LOCK ALREADY
func (px *Paxos) spamDecided(seq int, value interface{}) {
	for i := 0; i < len(px.peers); i++ {
		go func(peerIndex int) {
			args := &DecidedArgs{px.me, px.done_map[px.me], seq, value}
			reply := DecidedReply{}
			if peerIndex == px.me {
				px.DecidedHandler(args, &reply)
			} else {
				call(px.peers[peerIndex], "Paxos.DecidedHandler", args, &reply)
			}
		}(i)
	}
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	if seq >= px.Min() {	
		go func(seq int, v interface{}) {
			//update px.max if needed
			if seq > px.max {
				px.max = seq
			}
			px.mu.Lock()
			//create a SeqData struct for this seq if it's new
			if _, exists := px.seq_DB[seq]; !exists {
				px.seq_DB[seq] = &SeqData{nil, -1, -1, false}
			}
			px.mu.Unlock()
			
			currentVal := v
			decided := false
			
			//while not decided:
			//choose n, unique and higher than any n seen so far
			for n := makePropID(px.me, 0); !decided && !px.isdead();{
				//send prepare(n) to all servers including self
				Preparechan := make(chan PrepareReply, len(px.peers))
				promises := 0
				highestAcceptedPropID := int64(-1)
				var highestAcceptedVal interface {}
				px.spamPrepare(seq, n, Preparechan)
				Temp_n := n
				
				//receive results from channel
				for i := 0; i < len(px.peers); i++ {
					reply := <-Preparechan
					if reply.Promise{
						promises++
						if reply.HighestAccPropID > highestAcceptedPropID {
							highestAcceptedPropID = reply.HighestAccPropID
							highestAcceptedVal = reply.AcceptedVal
						}
					} else if reply.HighestPromPropID > Temp_n{
						Temp_n = reply.HighestPromPropID
					}
				}
				
				//if prepare_ok(n, n_a, v_a) from majority:
				if promises > len(px.peers)/2 {
					//v' = v_a with highest n_a; choose own v otherwise
					if highestAcceptedPropID > -1 {
						currentVal = highestAcceptedVal
					}
					
					//send accept(n, v') to all
					Acceptchan := make(chan AcceptReply, len(px.peers))
					accepts := 0
					px.spamAccept(seq, n, currentVal, Acceptchan)
					
					//read results from channel
					for i := 0; i < len(px.peers); i++ {
						reply := <-Acceptchan
						if reply.Accepted{
							accepts++
						}
					}
					//if accept_ok(n) from majority:
					if accepts > len(px.peers)/2 {
						//send decided(v') to all
						px.spamDecided(seq, currentVal)
						decided = true
					} else {
						n = makePropID(px.me, (Temp_n >> 32) + rand.Int63n(int64(32)) + 1)
					}
				} else {
					n = makePropID(px.me, (Temp_n >> 32) + rand.Int63n(int64(32)) + 1)
				}
			}
		}(seq, v)
	}
}

//handles the logic for updating done vectors. EXPECTS ITS CALLER TO HOLD THE LOCK ALREADY
func (px *Paxos) Done2(seq int, peer int) {
	//update done_map
	if seq > px.done_map[peer]{
		OldDone := px.done_map[peer]
		px.done_map[peer] = seq;
		
		//Try to update min if this peer could have been the lowest one
		if OldDone == px.min {
			//iterate done_map to get new min
			new_min := seq
			for _, element := range px.done_map {
				if element < new_min {
					new_min = element
				}
			}
			//if new min > old min, update min and purge db
			if new_min > px.min {
				px.min = new_min
				for key, _ := range px.seq_DB {
					if key <= px.min {
						delete(px.seq_DB, key)
					}
				}
			}
		}
	}
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	px.Done2(seq, px.me)
	px.mu.Unlock()
	return
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	return px.max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	return px.min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	var val interface{}
	fate := Forgotten 
	px.mu.Lock()
	if seq >= px.min {
		if element, ok := px.seq_DB[seq]; ok && element.Decided {
			fate = Decided
			val = element.accepted_val
		} else {
			fate = Pending
		}
	}
	px.mu.Unlock()
	return fate, val
}



//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me


	// Your initialization code here.
	px.done_map = make([]int, len(px.peers))
	px.seq_DB = make(map[int]*SeqData)
	px.min	= -1
	px.max	= -1
	for i := 0; i < len(px.peers); i++{
		px.done_map[i] = -1
	}
	
	rand.Seed(int64(px.me*64))
	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}
	return px
}
