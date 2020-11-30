package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"
import "container/list"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "time"
import "syscall"
import "encoding/gob"
import "math/rand"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num
	currentconfig int
	opnumber int64
	lastAppliedSeq int
}


type Op struct {
	OpID	int64
	Operation string
	GID     int64
	Servers []string
	Shard int
	Num int
}

type serverLoad struct {
	GID int64
	Discrepancy int
}

//balances the shard load across all server groups in the current config
func (sm *ShardMaster) balanceConfig(leaver int64) {
	targetload := NShards/len(sm.configs[sm.currentconfig].Groups)
	load := make(map[int64]int)
	shardmap := make(map[int64]*list.List)
	orphans := list.New()
	
	for key,_ := range sm.configs[sm.currentconfig].Groups {
		shardmap[key] = list.New()
		load[key] = 0
	}
	
	for shardnum, gid := range sm.configs[sm.currentconfig].Shards {
		if gid != leaver && gid != 0 {
			load[gid]++
			shardmap[gid].PushBack(shardnum)
		} else {
			orphans.PushBack(shardnum)
		}
	}
	
	DPrintf("Peer %v starting balancing algorithm. Orphans found: %v \n", sm.me, orphans.Len())
	//inefficient as shit
	for balanced := false; !balanced;  {
		balanced = true
		donorFound, doneeFound := false, false
		donor, donee := int64(0), int64(0)
		
		//find the group with the least shards (potential donee) and most shards (potential donor)
		for gid, numshards := range load {
			if numshards > targetload {
				if !donorFound {
					donorFound = true
					donor = gid
				} else if numshards > load[donor] {
					donor = gid
				}
			} else {
				if !doneeFound {
					doneeFound = true
					donee = gid
				} else if numshards < load[donee] {
					donee = gid
				}
			}
		}
		DPrintf("Peer %v in balancing algorithm. Donee found: %v with %v load. Donor found: %v with %v load \n", sm.me, doneeFound, load[donee], donorFound, load[donor])
		//transfer shards if it makes sense (orphans take priority over donor)
		if doneeFound {
			if numOrphans := orphans.Len(); numOrphans > 0 {
				balanced = false
				needed := (targetload + 1) - load[donee]
				
				for i := 0; i < needed && i < numOrphans; i++ {
					load[donee]++
					transferredShard := orphans.Front().Value.(int)
					shardmap[donee].PushFront(transferredShard)
					sm.configs[sm.currentconfig].Shards[transferredShard] = donee
					orphans.Remove(orphans.Front())
				}
			} else if donorFound && !(load[donee] == targetload && load[donor] == targetload + 1){
				balanced = false
				needed := targetload - load[donee]
				extra := load[donor] - targetload
				
				for i := 0; i < needed && i < extra; i++ {
					load[donee]++
					load[donor]--
					transferredShard := shardmap[donor].Front().Value.(int)
					shardmap[donee].PushBack(transferredShard)
					sm.configs[sm.currentconfig].Shards[transferredShard] = donee
					shardmap[donor].Remove(shardmap[donor].Front())
				}
			}
		}
	}
}

//creates a new config on the slice that is a clone of the current config and increments the current configuration number
func (sm *ShardMaster) cloneCurrentConfigToNext() {
	sm.configs = append(sm.configs, Config{})
	sm.configs[sm.currentconfig + 1].Num = sm.currentconfig + 1
	for shard, gid := range sm.configs[sm.currentconfig].Shards {
		sm.configs[sm.currentconfig + 1].Shards[shard] = gid
	}
	sm.configs[sm.currentconfig + 1].Groups = make(map[int64][]string)
	for gid, servers := range sm.configs[sm.currentconfig].Groups {
		sm.configs[sm.currentconfig + 1].Groups[gid] = servers
	}
	sm.currentconfig++
}

//waits for seq to become decided and returns the decided operation
func (sm *ShardMaster) ye_ye_boi_wait_this_out(seq int) Op {
	wait_time := 10 * time.Millisecond
	//while true:
	for true {
		//wait wait_time milliseconds
		time.Sleep(wait_time)
		//if seq's status is decided:
		if status, decidedOperation := sm.px.Status(seq); status == paxos.Decided {
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
func (sm *ShardMaster) ye_ye_boi_run_this_back(request Op) Config {
	retval := Config{}
	decidedOperation := Op{}
	//while true:
	for true{
		//increment lastAppliedSeq
		sm.lastAppliedSeq++
		//check the status of lastAppliedSeq. If decided:
		if status, decidedOp := sm.px.Status(sm.lastAppliedSeq); status == paxos.Decided {
			//get the decided operation
			decidedOperation = decidedOp.(Op)
		} else {
			//try to elect this operation for lastAppliedSeq
			sm.px.Start(sm.lastAppliedSeq, request)
			//get the decided operation
			decidedOperation = sm.ye_ye_boi_wait_this_out(sm.lastAppliedSeq)
		}

		//execute the decided operation
		switch decidedOperation.Operation {
		case "Join":
			DPrintf("Peer %v doing Join on old cfg: %v ... \n", sm.me, sm.configs[sm.currentconfig])
			sm.cloneCurrentConfigToNext()
			sm.configs[sm.currentconfig].Groups[decidedOperation.GID] = decidedOperation.Servers
			sm.balanceConfig(-1)
			DPrintf("Peer %v finishing Join with new cfg: %v ... \n", sm.me, sm.configs[sm.currentconfig])
		case "Leave":
			sm.cloneCurrentConfigToNext()
			delete(sm.configs[sm.currentconfig].Groups, decidedOperation.GID)
			sm.balanceConfig(decidedOperation.GID)
		case "Move":
			sm.cloneCurrentConfigToNext()
			sm.configs[sm.currentconfig].Shards[decidedOperation.Shard] = decidedOperation.GID
		case "Query":
			if decidedOperation.Num == -1 || decidedOperation.Num > sm.currentconfig {
				retval = sm.configs[sm.currentconfig]
			} else {
				retval = sm.configs[decidedOperation.Num]
			}
		}
		
		//if the decided operation was this operation: break
		if decidedOperation.OpID == request.OpID {
			sm.px.Done(sm.lastAppliedSeq)
			sm.opnumber++
			break
		}
	}
	return retval
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	sm.mu.Lock()
	op := Op{(int64(sm.me) << 32 | sm.opnumber), "Join", args.GID, args.Servers, 0, 0}
	sm.ye_ye_boi_run_this_back(op)
	sm.mu.Unlock()
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.mu.Lock()
	op := Op{(int64(sm.me) << 32 | sm.opnumber), "Leave", args.GID, nil, 0, 0}
	sm.ye_ye_boi_run_this_back(op)
	sm.mu.Unlock()
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.mu.Lock()
	op := Op{(int64(sm.me) << 32 | sm.opnumber), "Move", args.GID, nil, args.Shard, 0}
	sm.ye_ye_boi_run_this_back(op)
	sm.mu.Unlock()
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()
	op := Op{(int64(sm.me) << 32 | sm.opnumber), "Query", 0, nil, 0, args.Num}
	retval := sm.ye_ye_boi_run_this_back(op)
	reply.Config = retval
	sm.mu.Unlock()
	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	sm.opnumber = 0
	sm.currentconfig = 0
	sm.lastAppliedSeq = -1

	
	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
