package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"



type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	thisView	viewservice.View
	db    		map[string]string
	lastServedAppend	map[string]int64
}

func (pb *PBServer) SockPuppetGet(args *SockPuppetGetArgs, reply *SockPuppetGetReply) error {
	//Just double check if the sender is still the primary since Get is idempotent anyways
	pb.mu.Lock()
	if pb.thisView.Backup == pb.me {
		if pb.thisView.Primary == args.Primary {
			reply.Err = OK
		} else {
			reply.Err = ErrNotPrimary
		}
	} else {
		reply.Err = ErrWrongServer
	}
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) SockPuppetPutAppend(args *SockPuppetPutAppendArgs, reply *SockPuppetPutAppendReply) error {
	pb.mu.Lock()
	if pb.thisView.Backup == pb.me {
		if pb.thisView.Primary == args.Primary {
			if args.Cmd == "Put" {
				pb.db[args.Key] = args.Value
			} else if args.Cmd == "Append" && args.Nonce != pb.lastServedAppend[args.Sender]  {
				pb.db[args.Key] = pb.db[args.Key] + args.Value
				pb.lastServedAppend[args.Sender] = args.Nonce
			}
			reply.Err = OK
		} else {
			reply.Err = ErrNotPrimary
		}
	} else {
		reply.Err = ErrWrongServer
	}
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	if pb.thisView.Primary == pb.me {
		value, exists := pb.db[args.Key]
		if exists {
			reply.Value = value
		} else {
			reply.Value = ""
		}
		reply.Err = OK
		if pb.thisView.Backup != "" {
			bkup_args := &SockPuppetGetArgs{args.Key, pb.me}
			bkup_reply := SockPuppetGetReply{}
			ok := call(pb.thisView.Backup, "PBServer.SockPuppetGet", bkup_args, &bkup_reply)
			if !ok || bkup_reply.Err != OK {
				reply.Err = ErrOnBackup + bkup_reply.Err
			}
		}
	} else {
		reply.Err = ErrNotPrimary
	}
	pb.mu.Unlock()
	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	if pb.thisView.Primary == pb.me {
		//apply the change locally, then roll back if the Backup encounters an error and return the error to the client
		tempVal, tempNonce := pb.db[args.Key], (pb.lastServedAppend[args.Sender] | 0)
		if args.Cmd == "Put" {
			pb.db[args.Key] = args.Value
		} else if args.Cmd == "Append" && args.Nonce != pb.lastServedAppend[args.Sender] {
			pb.db[args.Key] = pb.db[args.Key] + args.Value
			pb.lastServedAppend[args.Sender] = args.Nonce
		}
		reply.Err = OK
		if pb.thisView.Backup != "" {
			bkup_args := &SockPuppetPutAppendArgs{args.Key, args.Value, args.Cmd, args.Sender, args.Nonce, pb.me}
			bkup_reply := SockPuppetPutAppendReply{}
			ok := call(pb.thisView.Backup, "PBServer.SockPuppetPutAppend", bkup_args, &bkup_reply)
			if !ok || bkup_reply.Err != OK {
				pb.db[args.Key] = tempVal
				pb.lastServedAppend[args.Sender] = tempNonce
				reply.Err = ErrOnBackup + bkup_reply.Err
			}
		}
	} else {
		reply.Err = ErrNotPrimary
	}
	pb.mu.Unlock()
	return nil
}

//Replicates the db and dup tracking state onto the backup server
func (pb *PBServer) ReplicateDB(args *ReplicateArgs, reply *ReplicateReply) error {
	pb.mu.Lock()
	//Get get the most recent view and update if this is the Backup and the sender is the Primary
	vsResponse, _ := pb.vs.Get()
	if vsResponse.Backup == pb.me && vsResponse.Primary == args.Primary{
		pb.db = args.DB
		pb.lastServedAppend = args.LastServedAppend
		reply.Err = OK
	} else {
		reply.Err = ErrWrongServer
	}
	pb.mu.Unlock()
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	pb.mu.Lock()
	vsResponse, _ := pb.vs.Ping(pb.thisView.Viewnum)
	lastView := pb.thisView
	pb.thisView = vsResponse
	//sync up new backup server (or same server if it restarted)
	if pb.thisView.Primary == pb.me && pb.thisView.Backup != "" && pb.thisView.Viewnum != lastView.Viewnum {
		for synced := false; !synced; {
			args := &ReplicateArgs{pb.db, pb.lastServedAppend, pb.me}
			reply := ReplicateReply{}
			ok := call(pb.thisView.Backup, "PBServer.ReplicateDB", args, &reply)
			if ok && reply.Err == OK {
				synced = true
			}
		}
	}
	pb.mu.Unlock()
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.thisView = viewservice.View{0, "", ""}
	pb.db = make(map[string]string)
	pb.lastServedAppend = make(map[string]int64)
	
	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
