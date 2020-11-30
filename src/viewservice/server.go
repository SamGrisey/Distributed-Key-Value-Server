package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
	thisView				View
	mustReplacePrimary 		bool
	mustReplaceBackup 		bool
	canChangeView			bool
	backupNotified			bool
	servers 				map[string]time.Time
}

//culls the dead servers from vs.servers and returns the most-recently pinged idle server
func (vs *ViewServer) cullDead() string {
	mostRecentPing := ""
	for key, element := range vs.servers {
		if age := time.Since(element); age > (DeadPings * PingInterval){
			if key == vs.thisView.Primary{
				vs.mustReplacePrimary = true
			} else if key == vs.thisView.Backup {
				vs.mustReplaceBackup = true
			}
			delete(vs.servers, key)
		} else {
			//server is considered idle if it's actually idle, or it's the server about to be replaced (due to explicit restart rather than timeout)
			if age < time.Since(vs.servers[mostRecentPing]) && ((key != vs.thisView.Primary && key != vs.thisView.Backup) || (key == vs.thisView.Primary && vs.mustReplacePrimary) || (key == vs.thisView.Backup && vs.mustReplaceBackup)){
				mostRecentPing = key
			}
		}
    }
	return mostRecentPing
}

//replaces the servers if needed (and possible) and advances the view number
func (vs *ViewServer) replaceServers(replacement_canidate string) {
	if vs.canChangeView {
		//double replacement only viable if it's the first view, otherwise it's irrecoverable
		if vs.mustReplacePrimary && vs.mustReplaceBackup {
			if vs.thisView.Viewnum == 0 {
				if replacement_canidate != "" {
					vs.thisView.Primary = replacement_canidate
					vs.thisView.Viewnum++
					vs.mustReplacePrimary = false
					vs.canChangeView = false
					if backup := vs.cullDead(); backup != "" {
						vs.thisView.Backup = backup
						vs.backupNotified = false
						vs.mustReplaceBackup = false
					}
				}
			} else {
				vs.canChangeView = false
			}
		} else if vs.mustReplacePrimary {
			if vs.thisView.Backup != "" {
				vs.thisView.Primary = vs.thisView.Backup
				vs.thisView.Backup = replacement_canidate
				vs.thisView.Viewnum++
				vs.canChangeView = false
				vs.mustReplacePrimary = false
				vs.backupNotified = false
				if replacement_canidate == "" {
					vs.mustReplaceBackup = true
				}
			} else { //irrecoverable
				vs.canChangeView = false
			}
		} else if vs.mustReplaceBackup && !(replacement_canidate == "" && vs.thisView.Backup == ""){
			vs.thisView.Backup = replacement_canidate
			vs.thisView.Viewnum++
			vs.canChangeView = false
			vs.backupNotified = false
			if replacement_canidate != "" {
				vs.mustReplaceBackup = false
			}
		}
	}
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	vs.servers[args.Me] = time.Now()
	//check if I require a view update (servers might report themselves as restarted, or come back from the dead intact well after the timeout period if primary lost connection before it acked)
	if args.Me == vs.thisView.Primary {
		if args.Viewnum == vs.thisView.Viewnum {
			vs.canChangeView = true
			vs.mustReplacePrimary = false
		} else if args.Viewnum == 0 {
			vs.mustReplacePrimary = true
		}
	} else if args.Me == vs.thisView.Backup{
		if args.Viewnum == vs.thisView.Viewnum {
			vs.backupNotified = true
		} else if args.Viewnum == 0 && vs.backupNotified {
			vs.mustReplaceBackup = true
		}
	}
	//carry out any required view updates
	if vs.mustReplaceBackup || vs.mustReplacePrimary{
		vs.replaceServers(vs.cullDead())
	}
	reply.View = vs.thisView
	vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	reply.View = vs.thisView
	vs.mu.Unlock()
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	//want to cull regardless of replacement needs every PingInterval
	replacementCanidate := vs.cullDead()
	if vs.mustReplaceBackup || vs.mustReplacePrimary{
		vs.replaceServers(replacementCanidate)
	}
	vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.canChangeView = true
	vs.mustReplacePrimary = true
	vs.mustReplaceBackup = true
	vs.thisView = View{0, "", ""}
	vs.servers = make(map[string]time.Time)
	vs.backupNotified = false
	
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
