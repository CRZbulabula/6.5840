package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.5840/labrpc"
	"sync"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"
import "6.5840/shardctrler"
import "time"

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	mu     sync.Mutex
	config shardctrler.Config

	sm       *shardctrler.Clerk
	make_end func(string) *labrpc.ClientEnd

	clerkId  int64
	nextOpId atomic.Int32
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end

	ck.clerkId = nrand()
	ck.nextOpId.Store(0)

	return ck
}

func (ck *Clerk) generateOpId() int {
	return int(ck.nextOpId.Add(1))
}

func (ck *Clerk) tryToUpdateConfig() {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.config = ck.sm.Query(ck.config.Num + 1)
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	shard := key2shard(key)
	args := GetArgs{
		ClerkId:   ck.clerkId,
		ClerkOpId: ck.generateOpId(),
		Key:       key,
	}

	for {
		ck.mu.Lock()
		gid := ck.config.Shards[shard]
		servers, ok := ck.config.Groups[gid]
		ck.mu.Unlock()

		if ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					DPrintf("Clerk: %d Get %s, %s from %s", ck.clerkId, key, reply.Value, servers[si])
					return reply.Value
				}
				if ok && (reply.Err == ErrTimeOut || reply.Err == ErrUnAvailable) {
					// retry this server when ErrTimeOut or ErrUnAvailable
					si -= 1
					time.Sleep(rpcRetryInterval)
				}
				if ok && (reply.Err == ErrWrongConfig) {
					ck.tryToUpdateConfig()
					break
				}
				// Switch server when not ok, or ErrWrongLeader
			}
		} else {
			ck.tryToUpdateConfig()
		}
		time.Sleep(rpcRetryInterval)
	}
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, opType OpType) {
	shard := key2shard(key)
	args := PutAppendArgs{
		ClerkId:   ck.clerkId,
		ClerkOpId: ck.generateOpId(),
		Key:       key,
		Value:     value,
		OpType:    opType,
	}

	for {
		ck.mu.Lock()
		gid := ck.config.Shards[shard]
		servers, ok := ck.config.Groups[gid]
		ck.mu.Unlock()

		if ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					DPrintf("Clerk: %d PutAppend %s, %s, %s to %s", ck.clerkId, key, value, opType, servers[si])
					return
				}
				if ok && (reply.Err == ErrTimeOut || reply.Err == ErrUnAvailable) {
					// retry this server when ErrTimeOut or ErrUnAvailable
					si -= 1
					time.Sleep(rpcRetryInterval)
				}
				if ok && reply.Err == ErrWrongConfig {
					ck.tryToUpdateConfig()
					break
				}
				// Switch server when not ok, or ErrWrongLeader
			}
		} else {
			ck.tryToUpdateConfig()
		}
		time.Sleep(rpcRetryInterval)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, Put)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, Append)
}
