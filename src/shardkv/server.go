package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Req struct {
	Value 		string
	Seq			int
}

type Op struct {
	Operation   string
	Identifier	int64
	ConfigNum	int
	Sender		int64
	ShardNum	int
	Key   		string
	Value 		string 
	Shard		ShardData
	NewConfig	shardmaster.Config
}

type ShardData struct {
	LoadedForConfig		int
	Db    				map[string]string
	RequestCache		map[int64]map[int64]*Req
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	deliveredShards		chan LoadArgs
	responsible			[shardmaster.NShards]int
	shards				map[int]*ShardData
	lastAppliedSeq		int
	currentConfig		shardmaster.Config
}

//waits for seq to become decided and returns the decided operation
func (kv *ShardKV) ye_ye_boi_wait_this_out(seq int) Op {
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
func (kv *ShardKV) ye_ye_boi_run_this_back(request Op) (int, string) {
	retval := ""
	decidedOperation := Op{}

	for true {
		kv.lastAppliedSeq++
		//check the status of lastAppliedSeq. apply if decided else schedule
		if status, decidedOp := kv.px.Status(kv.lastAppliedSeq); status == paxos.Decided {
			//get the decided operation
			decidedOperation = decidedOp.(Op)
		} else {
			//always run bookkeeping Ops
			if request.Operation == "Update" || request.Operation == "Load" {
				if sharddata, exists := kv.shards[request.ShardNum]; (request.Operation == "Update" && request.NewConfig.Num == kv.currentConfig.Num+1) || (request.Operation == "Load" && (!exists || request.ConfigNum > sharddata.LoadedForConfig)) {
					kv.px.Start(kv.lastAppliedSeq, request)
					//get the decided operation
					decidedOperation = kv.ye_ye_boi_wait_this_out(kv.lastAppliedSeq)
				} else {
					kv.lastAppliedSeq--
					kv.px.Done(kv.lastAppliedSeq)
					return 0, ""
				}
			} else if kv.responsible[request.ShardNum] == 1 { //run db Ops if we're responsible
				//this strategy is utterly fucking retarded
				if _, exists := kv.shards[request.ShardNum].RequestCache[request.Sender]; !exists {
					kv.shards[request.ShardNum].RequestCache[request.Sender] = make(map[int64]*Req)
				}
				
				//PROBLEM MAY BE HERE
				//if not duplicate, run paxos, else return cached result
				if cachedReq, exists := kv.shards[request.ShardNum].RequestCache[request.Sender][request.Identifier]; !exists {
					kv.px.Start(kv.lastAppliedSeq, request)
					//get the decided operation
					decidedOperation = kv.ye_ye_boi_wait_this_out(kv.lastAppliedSeq)
				} else {
					DPrintf("GID %v Peer %v Seq %v Op %v ID %v from %v returning cached value: %v \n", kv.gid, kv.me, cachedReq.Seq, request.Operation, request.Identifier, request.Sender, cachedReq.Value)
					kv.lastAppliedSeq--
					kv.px.Done(kv.lastAppliedSeq)
					return 0, cachedReq.Value 
				}
			} else { //not responsible for this db Op so rollback kv.lastAppliedSeq and return error
				kv.lastAppliedSeq--
				kv.px.Done(kv.lastAppliedSeq)
				return -2, ""
			}
		}
		
		//execute the decided operation
		DPrintf("GID %v Peer %v Seq %v Op %v \n", kv.gid, kv.me, kv.lastAppliedSeq, decidedOperation.Operation)
		switch decidedOperation.Operation {
		case "Put":
			DPrintf("GID %v Peer %v Seq %v Shard %v executing PUT ID %v from %v; isOwnRequest: %v Key: %v Value: %v responsible: %v \n", kv.gid, kv.me, kv.lastAppliedSeq, decidedOperation.ShardNum, decidedOperation.Identifier, decidedOperation.Sender, decidedOperation.Identifier == request.Identifier, decidedOperation.Key, decidedOperation.Value, kv.responsible[decidedOperation.ShardNum])
			kv.shards[decidedOperation.ShardNum].Db[decidedOperation.Key] = decidedOperation.Value
			retval = ""
		case "Append":
			DPrintf("GID %v Peer %v Seq %v Shard %v executing APPEND ID %v from %v; isOwnRequest: %v Key: %v Value: %v responsible: %v \n", kv.gid, kv.me, kv.lastAppliedSeq, decidedOperation.ShardNum, decidedOperation.Identifier, decidedOperation.Sender, decidedOperation.Identifier == request.Identifier, decidedOperation.Key, decidedOperation.Value, kv.responsible[decidedOperation.ShardNum])
			kv.shards[decidedOperation.ShardNum].Db[decidedOperation.Key] = kv.shards[decidedOperation.ShardNum].Db[decidedOperation.Key] + decidedOperation.Value
			retval = ""
		case "Get":
			if _,exists := kv.shards[decidedOperation.ShardNum].Db[decidedOperation.Key]; exists {
				retval = kv.shards[decidedOperation.ShardNum].Db[decidedOperation.Key]
			} else {
				retval = ""
			}
			DPrintf("GID %v Peer %v Seq %v Shard %v executing GET ID %v from %v; isOwnRequest: %v Key: %v Result: %v responsible: %v \n", kv.gid, kv.me, kv.lastAppliedSeq, decidedOperation.ShardNum, decidedOperation.Identifier, decidedOperation.Sender, decidedOperation.Identifier == request.Identifier, decidedOperation.Key, retval, kv.responsible[decidedOperation.ShardNum])
		case "Load":
			DPrintf("GID %v Peer %v Seq %v executing LOAD; isOwnRequest: %v ShardNum: %v DB: %v \n", kv.gid, kv.me, kv.lastAppliedSeq, decidedOperation.Identifier == request.Identifier, decidedOperation.ShardNum, decidedOperation.Shard.Db)
			//load in shard and activate responsibility if we have already updated to a config where we must service it
			if sharddata, exists := kv.shards[decidedOperation.ShardNum]; !exists || decidedOperation.ConfigNum > sharddata.LoadedForConfig {
				kv.shards[decidedOperation.ShardNum] = &ShardData{}
				kv.shards[decidedOperation.ShardNum].Db = make(map[string]string)
				kv.shards[decidedOperation.ShardNum].RequestCache = make(map[int64]map[int64]*Req)
				kv.shards[decidedOperation.ShardNum].LoadedForConfig = decidedOperation.ConfigNum
				for key, val := range decidedOperation.Shard.Db {
					kv.shards[decidedOperation.ShardNum].Db[key] = val
				}
				for key, _ := range decidedOperation.Shard.RequestCache {
					kv.shards[decidedOperation.ShardNum].RequestCache[key] = make(map[int64]*Req)
					for key2, val2 := range decidedOperation.Shard.RequestCache[key] {
						kv.shards[decidedOperation.ShardNum].RequestCache[key][key2] = val2
					}
				}
				if kv.currentConfig.Shards[decidedOperation.ShardNum] == kv.gid {
					kv.responsible[decidedOperation.ShardNum] = 1
				}
			}
		case "Update":
			DPrintf("GID %v Peer %v Seq %v executing UPDATE; isOwnRequest: %v ConfigNum: %v Shards: %v \n", kv.gid, kv.me, kv.lastAppliedSeq, decidedOperation.Identifier == request.Identifier, decidedOperation.NewConfig.Num, decidedOperation.NewConfig.Shards)
			if decidedOperation.NewConfig.Num > kv.currentConfig.Num {
				//attempt to hand off all applicable shards and roll back kv.lastAppliedSeq to retry later if failed
				failed := false
				for i := 0; i < shardmaster.NShards; i++ {
					_, exists := kv.shards[i]
					if kv.currentConfig.Shards[i] == kv.gid && decidedOperation.NewConfig.Shards[i] != kv.gid {
						kv.responsible[i] = 0
						if !(exists && kv.handoff(i, decidedOperation.NewConfig.Groups[decidedOperation.NewConfig.Shards[i]], decidedOperation.NewConfig.Num) > len(decidedOperation.NewConfig.Groups[decidedOperation.NewConfig.Shards[i]])/2) {
							failed = true
						} 
					}
				}
				
				if failed {
					kv.lastAppliedSeq--
					kv.px.Done(kv.lastAppliedSeq)
					return -1, ""
				}
				//after a successful shard handoff, update config and delete handed off shards
				for i := 0; i < shardmaster.NShards; i++ {
					if kv.currentConfig.Shards[i] == kv.gid && decidedOperation.NewConfig.Shards[i] != kv.gid {
						delete(kv.shards, i) 
					}
				}
				kv.currentConfig.Num = decidedOperation.NewConfig.Num
				for i := 0; i < shardmaster.NShards; i++ {
					kv.currentConfig.Shards[i] = decidedOperation.NewConfig.Shards[i]
				}
				kv.currentConfig.Groups = make(map[int64][]string)
				for key, _ := range decidedOperation.NewConfig.Groups {
					kv.currentConfig.Groups[key] = make([]string, len(decidedOperation.NewConfig.Groups[key]))
					for i := 0; i < len(decidedOperation.NewConfig.Groups[key]); i++ {
						kv.currentConfig.Groups[key][i] = decidedOperation.NewConfig.Groups[key][i]
					}
				}
				
				//update new responsibilities
				for i := 0; i < shardmaster.NShards; i++ {
					_, exists := kv.shards[i]
					if exists && kv.currentConfig.Shards[i] == kv.gid {
						kv.responsible[i] = 1
					}
				}
			}
		}
		
		//cache operation only if get, put, append
		//this strategy is utterly fucking retarded
		if decidedOperation.Operation != "Update" && decidedOperation.Operation != "Load" {
			if _, exists := kv.shards[decidedOperation.ShardNum].RequestCache[decidedOperation.Sender]; !exists {
				//DPrintf("GID %v Peer %v %v \n", kv.gid, kv.me, kv.shards[decidedOperation.ShardNum].RequestCache)
				kv.shards[decidedOperation.ShardNum].RequestCache[decidedOperation.Sender] = make(map[int64]*Req)
			}
			kv.shards[decidedOperation.ShardNum].RequestCache[decidedOperation.Sender][decidedOperation.Identifier] = &Req{retval, kv.lastAppliedSeq}
		}
		
		//if the decided operation was this operation: break
		if decidedOperation.Identifier == request.Identifier {
			kv.px.Done(kv.lastAppliedSeq)
			break
		}
	}
	return 0, retval
}

func (kv *ShardKV) handoff(shardnum int, servers []string, confignum int) int {
	args := &LoadArgs{confignum, shardnum, *kv.shards[shardnum]}
	reply := LoadReply{}
	successes := 0
	for i := 0; i < len(servers); i++ {
		//call Load handler
		if ok := call(servers[i], "ShardKV.Load", args, &reply); ok && reply.Err == OK {
			successes++
		}
	}
	return successes
}

func (kv *ShardKV) Load(args *LoadArgs, reply *LoadReply) error {
	//kv.mu.Lock()
	if oldshard, exists := kv.shards[args.ShardNum]; !exists || args.LoadedForConfig > oldshard.LoadedForConfig {
		kv.deliveredShards <- *args
	}
	reply.Err = OK
	//kv.mu.Unlock()
	return nil
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	//DPrintf("GID %v Peer %v beginning Get RPC for %v ... \n", kv.gid, kv.me, *args)
	// build op struct
	op := Op{"Get", args.Identifier, args.ConfigNum, args.Sender, args.ShardNum, args.Key, "", ShardData{}, shardmaster.Config{}}
	retcode, result := kv.ye_ye_boi_run_this_back(op)
	//DPrintf("GID %v Peer %v Get(%v) retcode = %v ... \n", kv.gid, kv.me, *args, retcode)
	
	//build reply
	if retcode == 0{
		reply.Value = result
		reply.Err = OK
	} else if retcode == -1 {
		reply.Value = ""
		reply.Err = ErrAwaitingUpdate
	} else if retcode == -2 {
		reply.Value = ""
		reply.Err = ErrWrongGroup
	}
	
	/*/clear everything from the shit pile up to but not including this seq
	if retcode == 0 {
		for key, element := range kv.shards[args.ShardNum].RequestCache[args.Sender] {
			if element.Seq < kv.shards[args.ShardNum].RequestCache[args.Sender][args.Identifier].Seq {
				delete(kv.shards[args.ShardNum].RequestCache[args.Sender], key)
			}
		}
	}*/

	kv.mu.Unlock()
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	//DPrintf("GID %v Peer %v beginning PutAppend RPC for %v ... \n", kv.gid, kv.me, *args)
	// build op struct
	op := Op{args.Op, args.Identifier, args.ConfigNum, args.Sender, args.ShardNum, args.Key, args.Value, ShardData{}, shardmaster.Config{}}
	retcode, _ := kv.ye_ye_boi_run_this_back(op)
	//DPrintf("GID %v Peer %v PutAppend(%v) retcode = %v ... \n", kv.gid, kv.me, *args, retcode)
	
	/*/clear everything from the shit pile up to but not including this seq
	if retcode == 0{
		for key, element := range kv.shards[args.ShardNum].RequestCache[args.Sender] {
			if element.Seq < kv.shards[args.ShardNum].RequestCache[args.Sender][args.Identifier].Seq {
				delete(kv.shards[args.ShardNum].RequestCache[args.Sender], key)
			}
		}
	}*/
	
	//build reply
	if retcode == 0{
		reply.Err = OK
	} else if retcode == -1 {
		reply.Err = ErrAwaitingUpdate
	} else if retcode == -2 {
		reply.Err = ErrWrongGroup
	}
	kv.mu.Unlock()
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	//load all delivered shards since last tick (avoids deadlock from loading immediately via RPC)
	for empty := false; !empty; {
		select {
		case s := <- kv.deliveredShards:
			oldshard, exists := kv.shards[s.ShardNum]
			if !exists || s.LoadedForConfig > oldshard.LoadedForConfig {
				op := Op{"Load", nrand(), s.LoadedForConfig, (kv.gid << 32 | int64(kv.me)), s.ShardNum, "", "", s.Shard, shardmaster.Config{}}
				retcode, _ := kv.ye_ye_boi_run_this_back(op)
				if retcode < 0 {
					empty = true
				}
			}
		default:
			empty = true
		}
	}
	
	//For each newer configuration:
	for response := kv.sm.Query(kv.currentConfig.Num+1); response.Num > kv.currentConfig.Num; response = kv.sm.Query(kv.currentConfig.Num+1) {
		if response.Num == 1{
			//if the new num is 1 (first real config), then we have to create new ShardDatas for each shard assigned to us (probably all of them)
			for i := 0; i < shardmaster.NShards; i++ {
				if response.Shards[i] == kv.gid {
					kv.shards[i] = &ShardData{}
					kv.shards[i].LoadedForConfig = 1
					kv.shards[i].Db = make(map[string]string)
					kv.shards[i].RequestCache = make(map[int64]map[int64]*Req)
					kv.responsible[i] = 1
				}
			}
			kv.currentConfig = response
		} else {
			breakflag := false
			//Check if we currently possess all the shards we're expected to hand off
			for i := 0; i < shardmaster.NShards; i++ {
				_, exists := kv.shards[i]
				if kv.currentConfig.Shards[i] == kv.gid && response.Shards[i] != kv.gid && !exists {
					breakflag = true
					break
				}
			}
			if breakflag {
				break
			}
			
			//schedule the configuration update
			op := Op{"Update", nrand(), kv.currentConfig.Num, (kv.gid << 32 | int64(kv.me)), 0, "", "", ShardData{}, response}
			kv.ye_ye_boi_run_this_back(op)
		}
	}
	kv.mu.Unlock()
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})
	gob.Register(Req{})
	gob.Register(ShardData{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	kv.lastAppliedSeq = -1
	kv.shards = make(map[int]*ShardData)
	kv.currentConfig = shardmaster.Config{}
	kv.deliveredShards = make(chan LoadArgs, shardmaster.NShards * 5)
	
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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
