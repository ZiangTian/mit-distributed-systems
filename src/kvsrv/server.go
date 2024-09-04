package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kvpairs   map[string]string
	reqStatus map[int64]bool   // whether a request has been fulfilled
	reqReply  map[int64]string // records the correct reply for a request
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// if is not resend, assert the request has not been fulfilled
	if !args.IsResend {
		// search in reqStatus and reqReply, should not find the request
		_, ok1 := kv.reqStatus[args.ReqId]
		_, ok2 := kv.reqReply[args.ReqId]
		if ok1 || ok2 {
			panic("A fresh request HAD been fulfilled")
		}
		kv.reqStatus[args.ReqId] = true

		value, ok := kv.kvpairs[args.Key]
		if ok {
			reply.Value = value
		} else {
			reply.Value = ""
		}
		kv.reqReply[args.ReqId] = reply.Value
	} else {
		// if is resend, check if the request has been fulfilled
		fulfilled := kv.reqStatus[args.ReqId]
		if !fulfilled { // if request has not been fulfilled
			// fulfill the request
			value, ok := kv.kvpairs[args.Key]
			if ok {
				reply.Value = value
			} else {
				reply.Value = ""
			}
			kv.reqReply[args.ReqId] = reply.Value
			kv.reqStatus[args.ReqId] = true
		} else {
			// if request has been fulfilled, return the correct reply
			reply.Value = kv.reqReply[args.ReqId]
		}
	}

	/*
		But i do have another concern. what if the initial request was send but lost and in the interim,
		Another request from another client was fulfilled. In this case the resend request would get the wrong reply.
	*/

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("Got Put request: %s %s", args.Key, args.Value)
	if !args.IsResend { // if is fresh
		_, ok1 := kv.reqStatus[args.ReqId]
		_, ok2 := kv.reqReply[args.ReqId]
		if ok1 || ok2 {
			panic("A fresh request HAD been fulfilled")
		}

		kv.reqStatus[args.ReqId] = true
		kv.reqReply[args.ReqId] = ""
		kv.kvpairs[args.Key] = args.Value

		reply.Value = ""
	} else {
		// if is resend, check if the request has been fulfilled
		fulfilled := kv.reqStatus[args.ReqId]
		if !fulfilled { // if request has not been fulfilled
			kv.kvpairs[args.Key] = args.Value
			kv.reqReply[args.ReqId] = ""
			kv.reqStatus[args.ReqId] = true
			reply.Value = ""
		} else {
			// if request has been fulfilled, return the correct reply
			reply.Value = kv.reqReply[args.ReqId]
		}
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !args.IsResend {
		_, ok1 := kv.reqStatus[args.ReqId]
		_, ok2 := kv.reqReply[args.ReqId]
		if ok1 || ok2 {
			panic("A fresh request HAD been fulfilled")
		}

		oldValue, ok := kv.kvpairs[args.Key]
		if !ok { // key not exist
			kv.kvpairs[args.Key] = args.Value
			reply.Value = ""
		} else {
			kv.kvpairs[args.Key] = oldValue + args.Value
			reply.Value = oldValue
		}

		kv.reqStatus[args.ReqId] = true
		kv.reqReply[args.ReqId] = reply.Value
	} else {
		// if is resend, check if the request has been fulfilled
		fulfilled := kv.reqStatus[args.ReqId]
		if !fulfilled { // if request has not been fulfilled
			oldValue, ok := kv.kvpairs[args.Key]
			if !ok { // key not exist
				kv.kvpairs[args.Key] = args.Value
				reply.Value = ""
			} else {
				kv.kvpairs[args.Key] = oldValue + args.Value
				reply.Value = oldValue
			}

			kv.reqStatus[args.ReqId] = true
			kv.reqReply[args.ReqId] = reply.Value
		} else {
			// if request has been fulfilled, return the correct reply
			reply.Value = kv.reqReply[args.ReqId]
		}
	}

}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvpairs = make(map[string]string)
	kv.mu = sync.Mutex{}
	kv.reqStatus = make(map[int64]bool)
	kv.reqReply = make(map[int64]string)
	return kv
}
