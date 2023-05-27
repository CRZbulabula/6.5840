package shardkv

import (
	"6.5840/shardctrler"
	"log"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const snapshotRatio = 0.9
const NShards = shardctrler.NShards
const maxCommitTime = 500 * time.Millisecond
const configMonitorInterval = 100 * time.Millisecond
const shardMigratorInterval = configMonitorInterval / 2
const rpcRetryInterval = 50 * time.Millisecond

const (
	AVAILABLE   = "AVAILABLE"
	UNAVAILABLE = "UNAVAILABLE"
	ADDING      = "ADDING"
	REMOVING    = "REMOVING"
)

type ShardState string

const (
	// Clerk Ops
	Get    = "Get"
	Put    = "Put"
	Append = "Append"

	// Server Ops
	UpdateConfig = "UpdateConfig"
	InstallShard = "InstallShard"
	DeleteShard  = "DeleteShard"
)

type OpType string

type Op struct {
	OpType OpType

	// For Get, Put, Append
	ClerkId   int64
	ClerkOpId int
	Key       string
	Value     string

	// For UpdateConfig
	Config shardctrler.Config

	// For InstallShard, DeleteShard
	Num   int
	Shard int
	Store map[string]string
}

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongConfig = "ErrWrongConfig"
	ErrUnAvailable = "ErrUnAvailable"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	ClerkId   int64
	ClerkOpId int

	Key    string
	Value  string
	OpType OpType // Put or Append
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

type MigrateShardArgs struct {
	Num   int
	Shard int
	Store map[string]string
}

type MigrateShardReply struct {
	Err Err
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func copyMap(m map[string]string) map[string]string {
	newMap := make(map[string]string)
	for k, v := range m {
		newMap[k] = v
	}
	return newMap
}
