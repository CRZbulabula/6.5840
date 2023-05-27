package shardctrler

import (
	"log"
	"sort"
)

const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (config *Config) newCopy() Config {
	newConfig := Config{}
	newConfig.Num = config.Num + 1
	for i := 0; i < NShards; i++ {
		newConfig.Shards[i] = config.Shards[i]
	}
	newConfig.Groups = make(map[int][]string)
	for gid, servers := range config.Groups {
		serversCopy := make([]string, len(servers))
		copy(serversCopy, servers)
		newConfig.Groups[gid] = serversCopy
	}
	return newConfig
}

const (
	OK             = "OK"
	ErrNoApplied   = "ErrNoApplied"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type JoinArgs struct {
	ClerkId   int64
	ClerkOpId int

	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	ClerkId   int64
	ClerkOpId int

	GIDs []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	ClerkId   int64
	ClerkOpId int

	Shard int
	GID   int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	ClerkId   int64
	ClerkOpId int

	Num int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

func (config *Config) printShards(format string, a ...interface{}) {
	if Debug {
		DPrintf(format, a...)
		DPrintf("\tshards: %v", config.Shards)
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func getSortedKeys(m map[int][]string) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func listToMap(l []int) map[int]bool {
	m := make(map[int]bool)
	for _, v := range l {
		m[v] = true
	}
	return m
}
