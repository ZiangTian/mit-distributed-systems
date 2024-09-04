package kvsrv

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	DPrintf("Clerk.Get(%s)", key)

	reqId := nrand()
	args := GetArgs{Key: key, IsResend: false, ReqId: reqId}
	reply := GetReply{}

	success := ck.server.Call("KVServer.Get", &args, &reply)
	for !success {
		time.Sleep(100 * time.Millisecond)
		args.IsResend = true
		success = ck.server.Call("KVServer.Get", &args, &reply)
	}

	return reply.Value // if empty, handled by server
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	reqId := nrand()
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.ReqId = reqId
	args.IsResend = false
	reply := PutAppendReply{}
	DPrintf("Clerk.PutAppend(%s, %s, %s)", key, value, op)
	success := ck.server.Call("KVServer."+op, &args, &reply)
	for !success {
		time.Sleep(100 * time.Millisecond)
		args.IsResend = true
		success = ck.server.Call("KVServer."+op, &args, &reply)
	}
	return reply.Value // if empty, handled by server
}

func (ck *Clerk) Put(key string, value string) {
	DPrintf("Clerk.Put(%s, %s)", key, value)
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	DPrintf("Clerk.Append(%s, %s)", key, value)
	return ck.PutAppend(key, value, "Append")
}
