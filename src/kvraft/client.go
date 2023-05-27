package kvraft

import (
	"6.5840/labrpc"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

const rpcRetryInterval = 100 * time.Millisecond

type Clerk struct {
	clerkId  int64
	nextOpId atomic.Int32

	servers   []*labrpc.ClientEnd
	serverNum int32
	leaderId  atomic.Int32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)

	ck.clerkId = nrand()
	ck.nextOpId.Store(0)

	ck.servers = servers
	ck.serverNum = int32(len(servers))
	ck.leaderId.Store(0)

	return ck
}

func (ck *Clerk) generateOpId() int {
	return int(ck.nextOpId.Add(1))
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		ClerkId:   ck.clerkId,
		ClerkOpId: ck.generateOpId(),
		Key:       key,
	}

	for {
		for i := 0; i < len(ck.servers); i++ {
			var reply GetReply
			serverId := (ck.leaderId.Load() + int32(i)) % ck.serverNum
			ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)
			if ok {
				if reply.Err == OK {
					ck.leaderId.Store(serverId)
					DPrintf(
						"Clerk %d Get %s opId %d success, server %d reply %s",
						ck.clerkId, args.Key, args.ClerkOpId, serverId, reply.Value)
					return reply.Value
				} else {
					DPrintf(
						"Clerk %d Get %s opId %d failed, server %d reply %s",
						ck.clerkId, args.Key, args.ClerkOpId, serverId, reply.Err)
				}
			}
		}
		time.Sleep(rpcRetryInterval)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, opType OpType) {
	args := PutAppendArgs{
		ClerkId:   ck.clerkId,
		ClerkOpId: ck.generateOpId(),
		OpType:    opType,
		Key:       key,
		Value:     value,
	}

	for {
		for i := 0; i < len(ck.servers); i++ {
			var reply PutAppendReply
			serverId := (ck.leaderId.Load() + int32(i)) % ck.serverNum
			ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
			if ok {
				if reply.Err == OK {
					ck.leaderId.Store(serverId)
					DPrintf(
						"Clerk %d PutAppend key %s value %s opId %d success, server %d",
						ck.clerkId, args.Key, args.Value, args.ClerkOpId, serverId)
					return
				} else {
					DPrintf(
						"Clerk %d PutAppend key %s value %s opId %d failed, server %d reply %s",
						ck.clerkId, args.Key, args.Value, args.ClerkOpId, serverId, reply.Err)
				}
			}
		}
		time.Sleep(rpcRetryInterval)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
