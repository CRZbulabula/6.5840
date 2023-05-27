package shardctrler

import (
	"6.5840/raft"
	"sort"
	"sync/atomic"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	dead int32 // set by Kill()

	clerkLastApplied map[int64]int
	entryCond        map[int]*sync.Cond

	configs []Config // indexed by config num
}

type OpType string

const (
	JOIN  = "JOIN"
	LEAVE = "LEAVE"
	MOVE  = "MOVE"
	QUERY = "QUERY"
)

type Op struct {
	ClerkId   int64
	ClerkOpId int

	OpType  OpType
	Servers map[int][]string // For JOIN
	GIDs    []int            // For LEAVE
	Shard   int              // For MOVE
	GID     int              // For MOVE
	Num     int              // For QUERY

}

const maxCommitTime = 200 * time.Millisecond

func (sc *ShardCtrler) makeAlarm(index int) {
	go func() {
		<-time.After(maxCommitTime)
		sc.mu.Lock()
		defer sc.mu.Unlock()
		sc.notifyCond(index)
	}()
}

func (sc *ShardCtrler) makeCond(index int) {
	if _, ok := sc.entryCond[index]; !ok {
		sc.entryCond[index] = sync.NewCond(&sc.mu)
	}
	sc.makeAlarm(index)
}

func (sc *ShardCtrler) waitCond(index int) {
	for !sc.killed() {
		if cond := sc.entryCond[index]; cond != nil {
			cond.Wait()
		} else {
			break
		}
	}
}

func (sc *ShardCtrler) notifyCond(index int) {
	if cond := sc.entryCond[index]; cond != nil {
		cond.Broadcast()
		delete(sc.entryCond, index)
	}
}

func (sc *ShardCtrler) submit(op Op) (int, bool) {
	index, _, isLeader := sc.rf.Start(op)
	return index, isLeader
}

func (sc *ShardCtrler) waitUntilAppliedOrTimeout(op Op) (Err, *Config) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if !sc.isApplied(op) {
		// Apply the op
		index, isLeader := sc.submit(op)
		if !isLeader {
			DPrintf("ShardCtrler: %d reject op: %v because I'm not the leader", sc.me, op)
			return ErrWrongLeader, nil
		}
		sc.makeCond(index)
		sc.waitCond(index)
	}

	if sc.isApplied(op) {
		// Do QUERY
		var value *Config
		if op.OpType == QUERY {
			if op.Num == -1 || op.Num >= len(sc.configs) {
				value = &sc.configs[len(sc.configs)-1]
			} else {
				value = &sc.configs[op.Num]
			}
		}
		return OK, value
	}

	DPrintf("ShardCtrler: %d timeout not apply op: %v", sc.me, op)
	return ErrNoApplied, nil
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	op := &Op{
		ClerkId:   args.ClerkId,
		ClerkOpId: args.ClerkOpId,
		OpType:    JOIN,
		Servers:   args.Servers,
	}

	err, _ := sc.waitUntilAppliedOrTimeout(*op)
	reply.WrongLeader = err == ErrWrongLeader
	reply.Err = err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := &Op{
		ClerkId:   args.ClerkId,
		ClerkOpId: args.ClerkOpId,
		OpType:    LEAVE,
		GIDs:      args.GIDs,
	}

	err, _ := sc.waitUntilAppliedOrTimeout(*op)
	reply.WrongLeader = err == ErrWrongLeader
	reply.Err = err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := &Op{
		ClerkId:   args.ClerkId,
		ClerkOpId: args.ClerkOpId,
		OpType:    MOVE,
		Shard:     args.Shard,
		GID:       args.GID,
	}

	err, _ := sc.waitUntilAppliedOrTimeout(*op)
	reply.WrongLeader = err == ErrWrongLeader
	reply.Err = err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := &Op{
		ClerkId:   args.ClerkId,
		ClerkOpId: args.ClerkOpId,
		OpType:    QUERY,
		Num:       args.Num,
	}

	err, replyConfig := sc.waitUntilAppliedOrTimeout(*op)
	reply.WrongLeader = err == ErrWrongLeader
	reply.Err = err
	if replyConfig != nil {
		reply.Config = *replyConfig
		DPrintf("ShardCtrler: %d reply query: %v", sc.me, reply.Config)
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	DPrintf("ShardCtrler: %d killed", sc.me)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) isApplied(op Op) bool {
	return sc.clerkLastApplied[op.ClerkId] >= op.ClerkOpId
}

func (sc *ShardCtrler) updateClerkLastApplied(op Op) {
	sc.clerkLastApplied[op.ClerkId] = max(sc.clerkLastApplied[op.ClerkId], op.ClerkOpId)
}

func (sc *ShardCtrler) applyClerkOp(op Op) {
	defer sc.updateClerkLastApplied(op)
	switch op.OpType {
	case JOIN:
		currentConfig := sc.configs[len(sc.configs)-1]
		newConfig := currentConfig.newCopy()
		isFirstJoin := len(currentConfig.Groups) == 0

		gids := getSortedKeys(op.Servers)
		for _, gid := range gids {
			newConfig.Groups[gid] = op.Servers[gid]
			DPrintf("ShardCtrler: %d apply join gid: %d, servers: %v", sc.me, gid, newConfig.Groups[gid])
		}

		if isFirstJoin {
			newConfig.printShards("ShardCtrler: %d start balance shards after FIRST join:", sc.me)
			balanceShardsAfterFirstJoin(&newConfig, getSortedKeys(op.Servers))
			newConfig.printShards("ShardCtrler: %d finish balance shards after FIRST join:", sc.me)
		} else {
			newConfig.printShards("ShardCtrler: %d start balance shards after join:", sc.me)
			balanceShardsAfterJoin(&newConfig, getSortedKeys(op.Servers))
			newConfig.printShards("ShardCtrler: %d finish balance shards after join:", sc.me)
		}
		sc.configs = append(sc.configs, newConfig)
		return
	case LEAVE:
		currentConfig := sc.configs[len(sc.configs)-1]
		newConfig := currentConfig.newCopy()

		for _, gid := range op.GIDs {
			delete(newConfig.Groups, gid)
			DPrintf("ShardCtrler: %d apply leave gid: %d", sc.me, gid)
		}

		newConfig.printShards("ShardCtrler: %d start balance shards after leave:", sc.me)
		balanceShardsAfterLeave(&newConfig, op.GIDs)
		newConfig.printShards("ShardCtrler: %d finish balance shards after leave:", sc.me)
		sc.configs = append(sc.configs, newConfig)
		return
	case MOVE:
		currentConfig := sc.configs[len(sc.configs)-1]
		newConfig := currentConfig.newCopy()
		newConfig.Shards[op.Shard] = op.GID
		DPrintf(
			"ShardCtrler: %d apply move shard %d, %d -> %d",
			sc.me, op.Shard, currentConfig.Shards[op.Shard], op.GID)
		sc.configs = append(sc.configs, newConfig)
		return
	case QUERY:
		return
	}
}

func balanceShardsAfterFirstJoin(config *Config, gids []int) {
	pointer := 0
	groupNum := len(gids)
	for i := 0; i < NShards; i++ {
		config.Shards[i] = gids[pointer]
		pointer = (pointer + 1) % groupNum
		DPrintf("\tbalance shard %d, %d -> %d", i, 0, config.Shards[i])
	}
}

type GidShards struct {
	Gid       int
	Shards    []int
	ShardsNum int
}

func getSortedGidShardsList(config *Config) []GidShards {
	gidShardsMap := make(map[int][]int)
	for gid := range config.Groups {
		gidShardsMap[gid] = make([]int, 0)
	}
	for shard, gid := range config.Shards {
		gidShardsMap[gid] = append(gidShardsMap[gid], shard)
	}
	gidShardsList := make([]GidShards, 0)
	for gid, shards := range gidShardsMap {
		gidShardsList = append(
			gidShardsList,
			GidShards{gid, shards, len(shards)})
	}
	sort.Slice(gidShardsList, func(i, j int) bool {
		if gidShardsList[i].ShardsNum == gidShardsList[j].ShardsNum {
			return gidShardsList[i].Gid < gidShardsList[j].Gid
		}
		return gidShardsList[i].ShardsNum < gidShardsList[j].ShardsNum
	})
	return gidShardsList
}

func balanceShardsAfterJoin(config *Config, joinGids []int) {
	groupNum := len(config.Groups)
	if groupNum-len(joinGids) >= NShards {
		// No need to balance
		return
	}
	groupNum = min(groupNum, NShards) // Notice: groupNum <= NShards
	base := NShards / groupNum
	remain := NShards % groupNum

	gidShardsList := getSortedGidShardsList(config)
	for _, gid := range joinGids {
		gidShardsList = append(
			[]GidShards{{gid, make([]int, 0), 0}},
			gidShardsList...)
	}

	l, r := 0, len(gidShardsList)-1
	for l < r {
		inL := base - gidShardsList[l].ShardsNum
		outR := gidShardsList[r].ShardsNum - base
		if remain > 0 {
			outR -= 1
		}

		moveNum := min(inL, outR)
		for i := 1; i <= moveNum; i++ {
			shard := gidShardsList[r].Shards[gidShardsList[r].ShardsNum-i]
			config.Shards[shard] = gidShardsList[l].Gid
			DPrintf(
				"\tbalance shard %d, %d -> %d",
				shard, gidShardsList[r].Gid, gidShardsList[l].Gid)
		}
		gidShardsList[l].ShardsNum += moveNum
		gidShardsList[r].ShardsNum -= moveNum

		if inL < outR {
			l += 1
		} else if inL > outR {
			r -= 1
			remain -= 1
		} else {
			l += 1
			r -= 1
			remain -= 1
		}
	}
}

func balanceShardsAfterLeave(config *Config, leaveGids []int) {
	groupNum := len(config.Groups)
	if groupNum == 0 {
		for i := 0; i < NShards; i++ {
			config.Shards[i] = 0
		}
		return
	}

	var leaveShards []int
	leaveGidsMap := listToMap(leaveGids)
	if groupNum >= NShards {
		// Quick balance
		moveNum := 0
		occupiedGidsMap := make(map[int]bool)
		for shard, gid := range config.Shards {
			if _, leave := leaveGidsMap[gid]; leave {
				moveNum += 1
				leaveShards = append(leaveShards, shard)
			} else {
				occupiedGidsMap[gid] = true
			}
		}
		if moveNum == 0 {
			return
		}

		pointer := 0
		gids := getSortedKeys(config.Groups)
		for _, gid := range gids {
			if _, occupied := occupiedGidsMap[gid]; !occupied {
				shard := leaveShards[pointer]
				DPrintf(
					"\tbalance shard %d, %d -> %d",
					shard, config.Shards[shard], gid)
				config.Shards[shard] = gid

				pointer += 1
				if pointer == moveNum {
					return
				}
			}
		}
	}

	base := NShards / groupNum
	remain := NShards % groupNum

	gidShardsList := getSortedGidShardsList(config)
	for _, gs := range gidShardsList {
		if _, ok := leaveGidsMap[gs.Gid]; ok {
			leaveShards = append(leaveShards, gs.Shards...)
		}
	}
	if len(leaveShards) == 0 {
		return
	}

	pointer := 0
	for _, gs := range gidShardsList {
		if _, ok := leaveGidsMap[gs.Gid]; !ok {
			moveNum := base - gs.ShardsNum
			for i := 0; i < moveNum; i++ {
				shard := leaveShards[pointer]
				DPrintf(
					"\tbalance shard %d, %d -> %d",
					shard, config.Shards[shard], gs.Gid)
				config.Shards[shard] = gs.Gid
				pointer += 1
			}
		}
	}

	for _, gs := range gidShardsList {
		if remain > 0 {
			if _, ok := leaveGidsMap[gs.Gid]; !ok {
				shard := leaveShards[pointer]
				DPrintf(
					"\tbalance shard %d, %d -> %d",
					shard, config.Shards[shard], gs.Gid)
				config.Shards[shard] = gs.Gid
				pointer += 1
				remain -= 1
			}
		} else {
			break
		}
	}
}

func (sc *ShardCtrler) tryApplyClerkOp(op Op, index int) {
	if !sc.isApplied(op) {
		sc.applyClerkOp(op)
		sc.notifyCond(index)
	}
}

func (sc *ShardCtrler) applyClerkOps() {
	for entry := range sc.applyCh {
		if sc.killed() {
			break
		}

		sc.mu.Lock()
		op := entry.Command.(Op)
		index := entry.CommandIndex
		sc.tryApplyClerkOp(op, index)
		sc.mu.Unlock()
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.clerkLastApplied = make(map[int64]int)
	sc.entryCond = make(map[int]*sync.Cond)

	go sc.applyClerkOps()

	return sc
}
