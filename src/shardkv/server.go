package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"bytes"
	"sync/atomic"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

type ShardStore struct {
	Store map[string]string
	State ShardState
}

type ShardKV struct {
	mu   sync.Mutex
	me   int
	gid  int
	dead int32 // set by Kill()

	make_end func(string) *labrpc.ClientEnd
	ctrlers  []*labrpc.ClientEnd
	mck      *shardctrler.Clerk

	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	persister    *raft.Persister
	maxraftstate int // snapshot if log grows this big

	migrateShardNum  int // The number of migrating shards
	clerkLastApplied map[int64]int
	config           shardctrler.Config
	store            [NShards]ShardStore

	entryCond map[int]*sync.Cond
}

func (kv *ShardKV) makeAlarm(index int) {
	go func() {
		<-time.After(maxCommitTime)
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.notifyCond(index)
	}()
}

func (kv *ShardKV) makeCond(index int) {
	if _, ok := kv.entryCond[index]; !ok {
		kv.entryCond[index] = sync.NewCond(&kv.mu)
	}
	kv.makeAlarm(index)
}

func (kv *ShardKV) waitCond(index int) {
	for !kv.killed() {
		if cond := kv.entryCond[index]; cond != nil {
			cond.Wait()
		} else {
			break
		}
	}
}

func (kv *ShardKV) notifyCond(index int) {
	if cond := kv.entryCond[index]; cond != nil {
		cond.Broadcast()
		delete(kv.entryCond, index)
	}
}

func (kv *ShardKV) submit(op Op) (int, bool) {
	index, _, isLeader := kv.rf.Start(op)
	return index, isLeader
}

func (kv *ShardKV) waitUntilClerkOpAppliedOrTimeout(op Op) (Err, string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := key2shard(op.Key)

	if kv.isClerkOpApplied(op) {
		value := ""
		if op.OpType == Get {
			value = kv.store[shard].Store[op.Key]
		}
		return OK, value
	}

	if kv.config.Shards[shard] != kv.gid {
		DPrintf(
			"ShardKV: %d.%d reject op: %v because shard %d belongs to %d, my gid is %d",
			kv.gid, kv.me, op, shard, kv.config.Shards[shard], kv.gid)
		return ErrWrongConfig, ""
	}
	if kv.store[shard].State != AVAILABLE {
		DPrintf(
			"ShardKV: %d.%d reject op: %v because shard %d is not available now",
			kv.gid, kv.me, op, shard)
		return ErrUnAvailable, ""
	}

	if !kv.isClerkOpApplied(op) {
		index, isLeader := kv.submit(op)
		if !isLeader {
			DPrintf("ShardKV: %d.%d reject op: %v because I'm not the leader", kv.gid, kv.me, op)
			return ErrWrongLeader, ""
		}
		kv.makeCond(index)
		kv.waitCond(index)
	}

	if kv.isClerkOpApplied(op) {
		// Get the value
		value := ""
		if op.OpType == Get {
			value = kv.store[shard].Store[op.Key]
		}
		return OK, value
	}
	DPrintf("ShardKV: %d.%d timeout not apply op: %v", kv.gid, kv.me, op)
	return ErrTimeOut, ""
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		ClerkId:   args.ClerkId,
		ClerkOpId: args.ClerkOpId,
		OpType:    Get,
		Key:       args.Key,
	}

	reply.Err, reply.Value = kv.waitUntilClerkOpAppliedOrTimeout(op)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		ClerkId:   args.ClerkId,
		ClerkOpId: args.ClerkOpId,
		OpType:    args.OpType,
		Key:       args.Key,
		Value:     args.Value,
	}

	reply.Err, _ = kv.waitUntilClerkOpAppliedOrTimeout(op)
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	DPrintf("ShardKV: %d.%d killed\n", kv.gid, kv.me)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) updateClerkLastApplied(op Op) {
	kv.clerkLastApplied[op.ClerkId] = max(kv.clerkLastApplied[op.ClerkId], op.ClerkOpId)
}

func (kv *ShardKV) isClerkOpApplied(op Op) bool {
	return kv.clerkLastApplied[op.ClerkId] >= op.ClerkOpId
}

func (kv *ShardKV) isClerkOpCanApply(op Op) bool {
	shard := key2shard(op.Key)
	return kv.config.Shards[shard] == kv.gid && kv.store[shard].State == AVAILABLE
}

func (kv *ShardKV) isNeedSnapshot() bool {
	return float32(kv.persister.RaftStateSize()) > snapshotRatio*float32(kv.maxraftstate)
}

func (kv *ShardKV) installSnapshot(snapshot []byte) {
	buffer := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(buffer)

	if decoder.Decode(&kv.migrateShardNum) != nil {
		DPrintf("ShardKV: %d.%d Failed to install snapshot migrateShardNum", kv.gid, kv.me)
		return
	}
	if decoder.Decode(&kv.clerkLastApplied) != nil {
		DPrintf("ShardKV: %d.%d Failed to install snapshot clerkLastApplied", kv.gid, kv.me)
		return
	}
	if decoder.Decode(&kv.config) != nil {
		DPrintf("ShardKV: %d.%d Failed to install snapshot config", kv.gid, kv.me)
		return
	}
	if decoder.Decode(&kv.store) != nil {
		DPrintf("ShardKV: %d.%d Failed to install snapshot store", kv.gid, kv.me)
		return
	}

	// DPrintf("ShardKV: %d.%d Install snapshot", kv.gid, kv.me)
}

func (kv *ShardKV) snapshot(index int) {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)

	if encoder.Encode(kv.migrateShardNum) != nil {
		DPrintf("ShardKV: %d.%d Failed to snapshot migrateShardNum", kv.gid, kv.me)
		return
	}
	if encoder.Encode(kv.clerkLastApplied) != nil {
		DPrintf("ShardKV: %d.%d Failed to snapshot clerkLastApplied", kv.gid, kv.me)
		return
	}
	if encoder.Encode(kv.config) != nil {
		DPrintf("ShardKV: %d.%d Failed to snapshot config", kv.gid, kv.me)
		return
	}
	if encoder.Encode(kv.store) != nil {
		DPrintf("ShardKV: %d.%d Failed to snapshot store", kv.gid, kv.me)
		return
	}

	snapshot := buffer.Bytes()
	kv.rf.Snapshot(index, snapshot)
	// DPrintf("ShardKV: %d.%d Snapshot at %d", kv.gid, kv.me, index)
}

func (kv *ShardKV) applyClientOp(op Op) {
	defer kv.updateClerkLastApplied(op)
	switch op.OpType {
	case Get:
		return
	case Put:
		DPrintf(
			"ShardKV: %d.%d Apply Put Key: %s, Value: %s, OpId: %d",
			kv.gid, kv.me, op.Key, op.Value, op.ClerkOpId)
		shard := key2shard(op.Key)
		kv.store[shard].Store[op.Key] = op.Value
		return
	case Append:
		DPrintf(
			"ShardKV: %d.%d Apply Append Key: %s, Value: %s, OpId: %d",
			kv.gid, kv.me, op.Key, op.Value, op.ClerkOpId)
		shard := key2shard(op.Key)
		kv.store[shard].Store[op.Key] += op.Value
		return
	}
}

func (kv *ShardKV) tryApplyClerkOp(op Op, index int) {
	if !kv.isClerkOpApplied(op) && kv.isClerkOpCanApply(op) {
		kv.applyClientOp(op)
		kv.notifyCond(index)
	}
}

func (kv *ShardKV) InstallShard(args *MigrateShardArgs, reply *MigrateShardReply) {
	kv.mu.Lock()
	if kv.config.Num != args.Num {
		kv.mu.Unlock()
		if kv.config.Num > args.Num {
			// Already installed
			reply.Err = ErrTimeOut
		} else {
			// Waiting for update myself
			reply.Err = ErrWrongConfig
		}
		DPrintf(
			"ShardKV: %d.%d reject InstallShard because config num are different, my Num %d arg Num %d",
			kv.gid, kv.me, kv.config.Num, args.Num)
		return
	}
	kv.mu.Unlock()

	op := Op{
		OpType: InstallShard,
		Num:    args.Num,
		Shard:  args.Shard,
		Store:  args.Store,
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("ShardKV: %d.%d receive InstallShard shard %d store %v", kv.gid, kv.me, args.Shard, args.Store)
	reply.Err = kv.waitUntilServerOpAppliedOrLeaderChanged(op)
}

func (kv *ShardKV) isServerOpApplied(op Op) bool {
	switch op.OpType {
	case UpdateConfig:
		return kv.config.Num >= op.Config.Num
	case InstallShard:
		return kv.config.Num > op.Num || (kv.config.Num == op.Num && kv.store[op.Shard].State == AVAILABLE)
	case DeleteShard:
		return kv.config.Num > op.Num || (kv.config.Num == op.Num && kv.store[op.Shard].State == UNAVAILABLE)
	}
	return false
}

func (kv *ShardKV) waitUntilServerOpAppliedOrLeaderChanged(op Op) Err {
	for !kv.killed() {
		// Try to apply the op
		index, isLeader := kv.submit(op)
		if !isLeader {
			DPrintf("ShardKV: %d.%d reject op: %v because I'm not the leader", kv.gid, kv.me, op)
			return ErrWrongLeader
		}
		kv.makeCond(index)
		kv.waitCond(index)

		if kv.isServerOpApplied(op) {
			return OK
		}

		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
		DPrintf("ShardKV: %d.%d retry op: %v", kv.gid, kv.me, op)
		kv.mu.Lock()
	}
	return ErrWrongLeader
}

func (kv *ShardKV) configMonitor() {
	for !kv.killed() {
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); isLeader && kv.migrateShardNum == 0 {
			newConfig := kv.mck.Query(kv.config.Num + 1)
			if kv.config.Num < newConfig.Num {
				DPrintf("ShardKV: %d.%d Start to update config %v", kv.gid, kv.me, newConfig)
				op := Op{
					OpType: UpdateConfig,
					Config: newConfig,
				}
				kv.waitUntilServerOpAppliedOrLeaderChanged(op)
			}
		}
		kv.mu.Unlock()
		time.Sleep(configMonitorInterval)
	}
}

func (kv *ShardKV) sendMigrateShardRequest(newGid int, args MigrateShardArgs, wg *sync.WaitGroup) {
	defer wg.Done()

	servers := kv.config.Groups[newGid]
	for _, isLeader := kv.rf.GetState(); isLeader && !kv.killed(); {
		for _, server := range servers {
			srv := kv.make_end(server)
			var reply MigrateShardReply
			ok := srv.Call("ShardKV.InstallShard", &args, &reply)

			if ok && (reply.Err == OK || reply.Err == ErrTimeOut) {
				kv.mu.Lock()
				DPrintf(
					"ShardKV: %d.%d Successfully migrate shard %d to %d, start delete",
					kv.gid, kv.me, args.Shard, newGid)
				op := Op{
					OpType: DeleteShard,
					Num:    args.Num,
					Shard:  args.Shard,
				}
				kv.waitUntilServerOpAppliedOrLeaderChanged(op)
				kv.mu.Unlock()
				return
			}
			if ok && reply.Err == ErrWrongConfig {
				DPrintf(
					"ShardKV: %d.%d Fail to migrate shard %d to %d because of wrong config",
					kv.gid, kv.me, args.Shard, newGid)
				return
			}
		}
		time.Sleep(rpcRetryInterval)
	}
}

/** Migrate REMOVING shards to other groups */
func (kv *ShardKV) shardMigrator() {
	for !kv.killed() {
		wg := sync.WaitGroup{}
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); isLeader && kv.migrateShardNum > 0 {
			for shard, shardStore := range kv.store {
				if shardStore.State == REMOVING {
					newGid := kv.config.Shards[shard]
					DPrintf(
						"ShardKV: %d.%d Start to migrate shard %d from %d to %d",
						kv.gid, kv.me, shard, kv.gid, newGid)
					wg.Add(1)
					args := MigrateShardArgs{
						Num:   kv.config.Num,
						Shard: shard,
						Store: shardStore.Store,
					}
					go kv.sendMigrateShardRequest(newGid, args, &wg)
				}
			}
		}
		kv.mu.Unlock()
		wg.Wait()
		time.Sleep(shardMigratorInterval)
	}
}

func (kv *ShardKV) applyServerOp(op Op) {
	switch op.OpType {
	case UpdateConfig:
		newConfig := op.Config
		for shard := 0; shard < NShards; shard++ {
			currentGid := kv.config.Shards[shard]
			newGid := newConfig.Shards[shard]

			if currentGid == kv.gid && newGid != kv.gid {
				kv.migrateShardNum += 1
				kv.store[shard].State = REMOVING

			} else if currentGid != kv.gid && newGid == kv.gid {
				if currentGid == 0 {
					kv.store[shard].State = AVAILABLE
				} else {
					kv.migrateShardNum += 1
					kv.store[shard].State = ADDING
				}
			}
		}
		kv.config = newConfig
		DPrintf("ShardKV: %d.%d apply update config to %v", kv.gid, kv.me, kv.config)
		break

	case InstallShard:
		kv.store[op.Shard] = ShardStore{
			Store: copyMap(op.Store),
			State: AVAILABLE,
		}
		kv.migrateShardNum -= 1
		DPrintf("ShardKV: %d.%d apply install shard %d, Store %v", kv.gid, kv.me, op.Shard, op.Store)
		if kv.migrateShardNum == 0 {
			DPrintf(
				"ShardKV: %d.%d successfully migrate all shards of config %v",
				kv.gid, kv.me, kv.config)
		}
		break

	case DeleteShard:
		kv.store[op.Shard] = ShardStore{
			Store: make(map[string]string),
			State: UNAVAILABLE,
		}
		kv.migrateShardNum -= 1
		DPrintf("ShardKV: %d.%d apply delete shard %d", kv.gid, kv.me, op.Shard)
		if kv.migrateShardNum == 0 {
			DPrintf(
				"ShardKV: %d.%d successfully migrate all shards of config %v",
				kv.gid, kv.me, kv.config)
		}
		break
	}
}

func (kv *ShardKV) tryApplyServerOp(op Op, index int) {
	if !kv.isServerOpApplied(op) {
		kv.applyServerOp(op)
		kv.notifyCond(index)
	}
}

func (kv *ShardKV) kvApplier() {
	for entry := range kv.applyCh {
		if kv.killed() {
			return
		}

		kv.mu.Lock()
		if entry.SnapshotValid {
			// Install snapshot
			kv.installSnapshot(entry.Snapshot)

		} else {
			op := entry.Command.(Op)
			index := entry.CommandIndex

			switch op.OpType {
			case Get:
				kv.tryApplyClerkOp(op, index)
				break
			case Put:
				kv.tryApplyClerkOp(op, index)
				break
			case Append:
				kv.tryApplyClerkOp(op, index)
				break
			case UpdateConfig:
				kv.tryApplyServerOp(op, index)
				break
			case InstallShard:
				kv.tryApplyServerOp(op, index)
				break
			case DeleteShard:
				kv.tryApplyServerOp(op, index)
				break
			}

			if kv.maxraftstate > 0 && kv.isNeedSnapshot() {
				// Snapshot
				kv.snapshot(index)
			}
		}
		kv.mu.Unlock()
	}
}

// StartServer servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should Store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft State along with the snapshot.
//
// the k/v server should snapshot when Raft's saved State exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid

	kv.make_end = make_end
	kv.ctrlers = ctrlers
	kv.mck = shardctrler.MakeClerk(ctrlers)

	kv.maxraftstate = maxraftstate
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	if kv.maxraftstate > 0 && kv.persister.SnapshotSize() > 0 {
		kv.installSnapshot(kv.persister.ReadSnapshot())
	} else {
		kv.config = shardctrler.Config{}
		kv.migrateShardNum = 0
		kv.clerkLastApplied = make(map[int64]int)
		for i := range kv.store {
			kv.store[i] = ShardStore{
				Store: make(map[string]string),
				State: UNAVAILABLE,
			}
		}
	}

	kv.entryCond = make(map[int]*sync.Cond)

	go kv.kvApplier()
	go kv.configMonitor()
	go kv.shardMigrator()

	return kv
}
