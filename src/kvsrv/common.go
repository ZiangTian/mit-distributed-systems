package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	IsResend bool
	ReqId    int64
}

type NotifyDoneArgs struct {
	ReqId int64
}

type NotifyDoneReply struct {
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	IsResend bool
	ReqId    int64
}

type GetReply struct {
	Value string
}
