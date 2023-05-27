package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	GET    = "GET"
	PUT    = "PUT"
	APPEND = "APPEND"
)

type OpType string

type Op struct {
	ClerkId   int64
	ClerkOpId int

	OpType OpType
	Key    string
	Value  string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	persister    *raft.Persister
	maxraftstate int // snapshot if log grows this big

	clerkLastApplied map[int64]int
	entryCond        map[int]*sync.Cond

	store map[string]string
}

const maxCommitTime = 200 * time.Millisecond

func (kv *KVServer) makeAlarm(index int) {
	go func() {
		<-time.After(maxCommitTime)
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.notifyCond(index)
	}()
}

func (kv *KVServer) makeCond(index int) {
	if _, ok := kv.entryCond[index]; !ok {
		kv.entryCond[index] = sync.NewCond(&kv.mu)
	}
	kv.makeAlarm(index)
}

func (kv *KVServer) waitCond(index int) {
	for !kv.killed() {
		if cond := kv.entryCond[index]; cond != nil {
			cond.Wait()
		} else {
			break
		}
	}
}

func (kv *KVServer) notifyCond(index int) {
	if cond := kv.entryCond[index]; cond != nil {
		cond.Broadcast()
		delete(kv.entryCond, index)
	}
}

func (kv *KVServer) submit(op Op) (int, bool) {
	index, _, isLeader := kv.rf.Start(op)
	return index, isLeader
}

func (kv *KVServer) waitUntilAppliedOrTimeout(op Op) (Err, string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.isApplied(op) {
		// Apply the op
		index, isLeader := kv.submit(op)
		if !isLeader {
			DPrintf("KVServer: %d reject op: %v because I'm not the leader", kv.me, op)
			return ErrWrongLeader, ""
		}
		kv.makeCond(index)
		kv.waitCond(index)
	}

	if kv.isApplied(op) {
		// Get the value
		value := ""
		if op.OpType == GET {
			value = kv.store[op.Key]
		}
		return OK, value
	}
	DPrintf("KVServer: %d timeout not apply op: %v", kv.me, op)
	return ErrNoKey, ""
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		ClerkId:   args.ClerkId,
		ClerkOpId: args.ClerkOpId,
		OpType:    GET,
		Key:       args.Key,
	}
	reply.Err, reply.Value = kv.waitUntilAppliedOrTimeout(op)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		ClerkId:   args.ClerkId,
		ClerkOpId: args.ClerkOpId,
		OpType:    args.OpType,
		Key:       args.Key,
		Value:     args.Value,
	}
	reply.Err, _ = kv.waitUntilAppliedOrTimeout(op)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	DPrintf("KVServer: %d killed", kv.me)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) updateClerkLastApplied(op Op) {
	kv.clerkLastApplied[op.ClerkId] = max(kv.clerkLastApplied[op.ClerkId], op.ClerkOpId)
}

func (kv *KVServer) isApplied(op Op) bool {
	return kv.clerkLastApplied[op.ClerkId] >= op.ClerkOpId
}

func (kv *KVServer) tryApplyClientOp(op Op, index int) {
	if !kv.isApplied(op) {
		kv.applyClientOp(op)
		kv.notifyCond(index)
	}
}

func (kv *KVServer) applyClientOp(op Op) {
	defer kv.updateClerkLastApplied(op)
	switch op.OpType {
	case GET:
		return
	case PUT:
		DPrintf(
			"KVServer: %d Apply Put Key: %s, Value: %s, OpId: %d",
			kv.me, op.Key, op.Value, op.ClerkOpId)
		kv.store[op.Key] = op.Value
		return
	case APPEND:
		DPrintf(
			"KVServer: %d Apply Append Key: %s, Value: %s, OpId: %d",
			kv.me, op.Key, op.Value, op.ClerkOpId)
		kv.store[op.Key] += op.Value
		return
	}
}

func (kv *KVServer) applyKV() {
	for entry := range kv.applyCh {
		if kv.killed() {
			break
		}

		kv.mu.Lock()
		if entry.SnapshotValid {
			// Install snapshot
			kv.installSnapshot(entry.Snapshot)

		} else {
			op := entry.Command.(Op)
			index := entry.CommandIndex
			kv.tryApplyClientOp(op, index)

			if kv.maxraftstate > 0 && kv.isNeedSnapshot() {
				// Snapshot
				kv.snapshot(index)
			}
		}
		kv.mu.Unlock()
	}
}

const snapshotRatio = 0.9

func (kv *KVServer) isNeedSnapshot() bool {
	return float32(kv.persister.RaftStateSize()) > snapshotRatio*float32(kv.maxraftstate)
}

func (kv *KVServer) installSnapshot(snapshot []byte) {
	buffer := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(buffer)
	if decoder.Decode(&kv.store) != nil || decoder.Decode(&kv.clerkLastApplied) != nil {
		DPrintf("KVServer: %d Failed to install snapshot", kv.me)
	}
	DPrintf("KVServer: %d Install snapshot", kv.me)
}

func (kv *KVServer) snapshot(index int) {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	if encoder.Encode(kv.store) != nil || encoder.Encode(kv.clerkLastApplied) != nil {
		DPrintf("KVServer: %d Failed to snapshot", kv.me)
	}

	snapshot := buffer.Bytes()
	kv.rf.Snapshot(index, snapshot)
	DPrintf("KVServer: %d Snapshot at %d", kv.me, index)
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me

	kv.persister = persister
	kv.maxraftstate = maxraftstate

	if kv.maxraftstate > 0 && kv.persister.SnapshotSize() > 0 {
		kv.installSnapshot(kv.persister.ReadSnapshot())
	} else {
		kv.store = make(map[string]string)
		kv.clerkLastApplied = make(map[int64]int)
	}

	kv.entryCond = make(map[int]*sync.Cond)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applyKV()

	return kv
}
