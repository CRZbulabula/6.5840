package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	ClerkId   int64
	ClerkOpId int

	OpType OpType // "Put" or "Append"
	Key    string
	Value  string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ClerkId   int64
	ClerkOpId int

	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
