package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
	ErrNotPrimary = "ErrNotPrimary"
	ErrOnBackup = "ErrOnBackup: "
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Nonce	int64
	Sender 	string
	Cmd		string
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
type ReplicateArgs struct {
	DB 					map[string]string
	LastServedAppend	map[string]int64
	Primary				string
}

type ReplicateReply struct {
	Err   Err
}

type SockPuppetPutAppendArgs struct {
	Key   	string
	Value 	string
	Cmd		string
	Sender	string
	Nonce	int64
	Primary	string
}

type SockPuppetPutAppendReply struct {
	Err   Err
}

type SockPuppetGetArgs struct {
	Key 	string
	Primary	string
}

type SockPuppetGetReply struct {
	Err   Err
}
