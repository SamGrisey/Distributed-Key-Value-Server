package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// You'll have to add definitions here.
	Key   		string
	Value 		string
	Operation   string 
	Identifier	int64
	Sender		int64
}

type Req struct {
	Value 		string
	Seq			int
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	db    				map[string]string
	lastAppliedSeq		int
	requestCache		map[int64]map[int64]*Req
}

//waits for seq to become decided and returns the decided operation
func (kv *KVPaxos) ye_ye_boi_wait_this_out(seq int) Op {
	wait_time := 10 * time.Millisecond
	//while true:
	for true {
		//wait wait_time milliseconds
		time.Sleep(wait_time)
		//if seq's status is decided:
		if status, decidedOperation := kv.px.Status(seq); status == paxos.Decided {
			//return the decided operation
			return decidedOperation.(Op)
		} else { //else: increase wait_time
			if wait_time < 10 * time.Second {
				wait_time *= 2
			}
		}
	}
	//never actually used but the compiler wants it
	return Op{}
}

//gets this operation scheduled via paxos and applies all elected operations between the last locally applied operation and this one
func (kv *KVPaxos) ye_ye_boi_run_this_back(request Op) string {
	retval := ""
	decidedOperation := Op{}
	//while true:
	for true{
		//increment lastAppliedSeq
		kv.lastAppliedSeq++
		//check the status of lastAppliedSeq. If decided:
		if status, decidedOp := kv.px.Status(kv.lastAppliedSeq); status == paxos.Decided {
			//get the decided operation
			decidedOperation = decidedOp.(Op)
		} else {
			//try to elect this operation for lastAppliedSeq
			kv.px.Start(kv.lastAppliedSeq, request)
			//get the decided operation
			decidedOperation = kv.ye_ye_boi_wait_this_out(kv.lastAppliedSeq)
		}

		//execute the decided operation
		switch decidedOperation.Operation {
		case "Put":
			kv.db[decidedOperation.Key] = decidedOperation.Value
			retval = ""
		case "Append":
			kv.db[decidedOperation.Key] = kv.db[decidedOperation.Key] + decidedOperation.Value
			retval = ""
		case "Get":
			if _,exists := kv.db[decidedOperation.Key]; exists {
				retval = kv.db[decidedOperation.Key]
			} else {
				retval = ""
			}
		}
		
		//cache operation
		//this strategy is utterly fucking retarded
		if _, exists := kv.requestCache[decidedOperation.Sender]; !exists {
			kv.requestCache[decidedOperation.Sender] = make(map[int64]*Req)
		}
		kv.requestCache[decidedOperation.Sender][decidedOperation.Identifier] = &Req{retval, kv.lastAppliedSeq}
		
		//if the decided operation was this operation: break
		if decidedOperation.Identifier == request.Identifier {
			kv.px.Done(kv.lastAppliedSeq)
			break
		}
	}
	return retval
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	//this strategy is utterly fucking retarded
	if _, exists := kv.requestCache[args.Sender]; !exists {
		kv.requestCache[args.Sender] = make(map[int64]*Req)
	}
	//Don't do paxos on duplicate Get because it could return something different
	if request, exists := kv.requestCache[args.Sender][args.Identifier]; !exists {
		// build op struct
		op := Op{args.Key, "", "Get", args.Identifier, args.Sender}
		result := kv.ye_ye_boi_run_this_back(op)
		
		//build reply
		reply.Value = result
		reply.Err = OK
	} else {
		//build reply
		reply.Value = request.Value
		reply.Err = OK
	}
	
	//clear everything from the shit pile up to but not including this seq
	for key, element := range kv.requestCache[args.Sender] {
		if element.Seq < kv.requestCache[args.Sender][args.Identifier].Seq {
			delete(kv.requestCache[args.Sender], key)
		}
	}

	kv.mu.Unlock()
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	//this strategy is utterly fucking retarded
	if _, exists := kv.requestCache[args.Sender]; !exists {
		kv.requestCache[args.Sender] = make(map[int64]*Req)
	}
	//Don't serve a duplicate request
	if _, exists := kv.requestCache[args.Sender][args.Identifier]; !exists {
		// build op struct
		op := Op{args.Key, args.Value, args.Op, args.Identifier, args.Sender}
		kv.ye_ye_boi_run_this_back(op)
	}
	
	//clear everything from the shit pile up to but not including this seq
	for key, element := range kv.requestCache[args.Sender] {
		if element.Seq < kv.requestCache[args.Sender][args.Identifier].Seq {
			delete(kv.requestCache[args.Sender], key)
		}
	}
	
	//build reply
	reply.Err = OK
	kv.mu.Unlock()
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.db = make(map[string]string)
	kv.lastAppliedSeq = -1
	kv.requestCache = make(map[int64]map[int64]*Req)
	
	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l


	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
