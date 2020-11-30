package kvpaxos

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Op    string // "Put" or "Append"
	Key   string
	Value string
	Identifier	int64
	Sender		int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	Identifier	int64
	Sender		int64
}

type GetReply struct {
	Err   Err
	Value string
}
