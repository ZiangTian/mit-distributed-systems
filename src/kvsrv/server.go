package kvsrv

import (
	"log"
	"sync"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kvpairs map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.kvpairs[args.Key]
	if ok {
		reply.Value = value
		reply.Exists = true
	} else {
		reply.Value = ""
		reply.Exists = false
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("Got Put request: %s %s", args.Key, args.Value)
	kv.kvpairs[args.Key] = args.Value
	reply.Value = ""
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	oldValue, ok := kv.kvpairs[args.Key]
	if !ok { // key not exist
		kv.kvpairs[args.Key] = args.Value
		reply.Value = ""
	} else {
		kv.kvpairs[args.Key] = oldValue + args.Value
		reply.Value = oldValue
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvpairs = make(map[string]string)
	kv.mu = sync.Mutex{}
	return kv
}
