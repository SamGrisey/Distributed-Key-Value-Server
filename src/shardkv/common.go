package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrAwaitingUpdate = "ErrAwaitingUpdate"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	ShardNum	int
	Identifier	int64
	Sender		int64
	ConfigNum	int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	ShardNum	int
	Identifier	int64
	Sender		int64
	ConfigNum	int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type LoadArgs struct {
	LoadedForConfig	int
	ShardNum		int
	Shard			ShardData
}

type LoadReply struct {
	Err		Err
}