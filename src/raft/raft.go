package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

type PeerState struct {
	CurrentState ServerState
	CurrentTerm  int
	VotedFor     int

	// For snapshot
	LastIncludedIndex int // the index of the last included log entry in the snapshot
	LastIncludedTerm  int // the term of the last included log entry in the snapshot
}

// A Go object implementing a single Raft peer.
type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Persistent state on all servers
	stateMutex sync.Mutex
	peerState  PeerState
	log        []LogEntry

	// Volatile state on all servers
	receivedHeartbeat atomic.Bool
	applyCh           chan ApplyMsg
	commitIndex       int
	lastApplied       int
	applyCond         *sync.Cond

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

func (rf *Raft) getLastLogIndex() int {
	if len(rf.log) == 0 {
		return rf.peerState.LastIncludedIndex
	}
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.peerState.LastIncludedTerm
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getLog(index int) LogEntry {
	if index == 0 {
		return LogEntry{0, 0, nil}
	}
	logLength := len(rf.log)
	for i := 0; i < logLength; i++ {
		if rf.log[i].Index == index {
			return rf.log[i]
		}
	}
	return LogEntry{0, 0, nil}
}

func (rf *Raft) getLogLoc(index int) int {
	logLength := len(rf.log)
	for i := 0; i < logLength; i++ {
		if rf.log[i].Index == index {
			return i
		} else if rf.log[i].Index > index {
			return -1
		}
	}
	return -1
}

// GetState return CurrentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.stateMutex.Lock()
	defer rf.stateMutex.Unlock()
	return rf.peerState.CurrentTerm, rf.peerState.CurrentState == Leader
}

func (rf *Raft) getStateInBytes() []byte {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	if encoder.Encode(rf.peerState) != nil || encoder.Encode(rf.log) != nil {
		DPrintf("Error encoding peer state or log")
	}
	return buffer.Bytes()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	rf.persister.Save(rf.getStateInBytes(), rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var peerState PeerState
	var log []LogEntry
	if decoder.Decode(&peerState) != nil || decoder.Decode(&log) != nil {
		DPrintf("Error decoding peer state or log")
	} else {
		peerState.CurrentState = Follower
		rf.peerState = peerState
		rf.log = log
		rf.commitIndex = peerState.LastIncludedIndex
		rf.lastApplied = peerState.LastIncludedIndex
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.stateMutex.Lock()
	defer rf.stateMutex.Unlock()

	if rf.peerState.LastIncludedIndex >= index {
		DPrintf(
			"Server %d: snapshot has been took %d, last included index: %d",
			rf.me, index, rf.peerState.LastIncludedIndex)
		return
	}

	// Trim logs and persist
	DPrintf("Server %d: snapshotting at index %d", rf.me, index)
	snapshotLoc := rf.getLogLoc(index)
	rf.peerState.LastIncludedIndex = index
	rf.peerState.LastIncludedTerm = rf.log[snapshotLoc].Term
	rf.log = rf.log[snapshotLoc+1:]
	rf.persister.Save(rf.getStateInBytes(), snapshot)
}

type InstallSnapshotArgs struct {
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.stateMutex.Lock()
	defer rf.stateMutex.Unlock()

	if args.Term < rf.peerState.CurrentTerm {
		// Reject if my term is newer
		reply.Term = rf.peerState.CurrentTerm
		DPrintf(
			"Server %d: reject snapshot because my term %d is newer than the given term %d",
			rf.me, rf.peerState.CurrentTerm, args.Term)
		return
	}

	if args.Term > rf.peerState.CurrentTerm {
		// Convert to follower if my term is obsoleted
		DPrintf(
			"Server %d: convert to Follower due to term %d -> %d",
			rf.me, rf.peerState.CurrentTerm, args.Term)
		rf.peerState.CurrentTerm = args.Term
		rf.peerState.CurrentState = Follower
		rf.peerState.VotedFor = -1
		rf.persist()
	}

	rf.receivedHeartbeat.Store(true)
	reply.Term = rf.peerState.CurrentTerm

	if args.LastIncludedIndex <= rf.peerState.LastIncludedIndex {
		// Reject if my snapshot is newer
		DPrintf(
			"Server %d: reject snapshot because my snapshot lastIndex %d is newer than the given lastIndex %d",
			rf.me, rf.peerState.LastIncludedIndex, args.LastIncludedIndex)
		return
	}

	// Trim logs
	lastLogLoc := rf.getLogLoc(args.LastIncludedIndex)
	if lastLogLoc != -1 && rf.log[lastLogLoc].Term == args.LastIncludedTerm {
		rf.log = rf.log[lastLogLoc+1:]
	} else {
		// Discard all if log is inconsistent
		rf.log = []LogEntry{}
	}

	// Install, persist and apply snapshot
	rf.commitIndex = args.LastIncludedIndex
	rf.peerState.LastIncludedIndex = args.LastIncludedIndex
	rf.peerState.LastIncludedTerm = args.LastIncludedTerm
	rf.persister.Save(rf.getStateInBytes(), args.Snapshot)

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Snapshot,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}

		rf.lastApplied = args.LastIncludedIndex
		DPrintf(
			"Server %d: snapshot installed at index %d, term %d",
			rf.me, args.LastIncludedIndex, args.LastIncludedTerm)
	}()
	time.Sleep(10 * time.Millisecond)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.stateMutex.Lock()
	defer rf.stateMutex.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.peerState.CurrentTerm

	if args.Term > rf.peerState.CurrentTerm {
		// Convert to follower if my term is obsoleted
		DPrintf(
			"Server %d: convert to Follower due to term %d -> %d",
			rf.me, rf.peerState.CurrentTerm, args.Term)
		rf.receivedHeartbeat.Store(true)
		rf.peerState.CurrentState = Follower
		rf.peerState.CurrentTerm = args.Term
		rf.peerState.VotedFor = -1
		rf.persist()
	}

	if args.Term < rf.peerState.CurrentTerm {
		// Reject if my term is newer
		DPrintf("Server %d: reject vote from %d because my term is newer", rf.me, args.CandidateId)
		return
	}

	if rf.peerState.CurrentState != Follower {
		// Reject if I'm not a follower
		DPrintf("Server %d: reject vote from %d because I'm not a Follower", rf.me, args.CandidateId)
		return
	}

	if rf.peerState.VotedFor == -1 || rf.peerState.VotedFor == args.CandidateId {
		lastLogIndex := rf.getLastLogIndex()
		lastLogTerm := rf.getLastLogTerm()

		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			// Vote for the first-come valid candidate
			reply.VoteGranted = true
			rf.peerState.VotedFor = args.CandidateId
			rf.receivedHeartbeat.Store(true)
			rf.persist()
			DPrintf(
				"Server %d: term %d voted for %d",
				rf.me, rf.peerState.CurrentTerm, args.CandidateId)
		} else {
			DPrintf(
				"Server %d: reject vote from %d because my log is newer. My last log index: %d term: %d. Candidate last log index: %d, term: %d",
				rf.me, args.CandidateId, lastLogIndex, lastLogTerm, args.LastLogIndex, args.LastLogTerm)
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

type AppendEntriesArgs struct {
	Term         int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// The next log index that leader should append, for optimization
	NextLogIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.stateMutex.Lock()
	defer rf.stateMutex.Unlock()

	reply.Term = rf.peerState.CurrentTerm
	reply.Success = false
	reply.NextLogIndex = 0

	if args.Term > rf.peerState.CurrentTerm {
		// Convert to follower if my term is obsoleted
		rf.receivedHeartbeat.Store(true)
		rf.peerState.CurrentState = Follower
		rf.peerState.CurrentTerm = args.Term
		rf.peerState.VotedFor = -1
		rf.persist()
		DPrintf(
			"Server %d: converted to follower due to term %d -> %d",
			rf.me, rf.peerState.CurrentTerm, args.Term)
	}

	if args.Term < rf.peerState.CurrentTerm {
		// Reject the request if my term is newer
		DPrintf(
			"Server %d: reject append entries because my term is newer, args term: %d my term: %d",
			rf.me, args.Term, rf.peerState.CurrentTerm)
		return
	}

	rf.receivedHeartbeat.Store(true)
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()
	if lastLogIndex < args.PrevLogIndex {
		// Reject the request if the previous log doesn't exist
		reply.NextLogIndex = lastLogIndex + 1
		DPrintf(
			"Server %d: reject append entries because my log is shorter, prevIndex: %d, my last index: %d",
			rf.me, args.PrevLogIndex, lastLogIndex)
		return
	}

	prevLogLoc := rf.getLogLoc(args.PrevLogIndex)
	if prevLogLoc == -1 {
		if rf.peerState.LastIncludedIndex == args.PrevLogIndex && rf.peerState.LastIncludedTerm == args.PrevLogTerm {
			// The previous log is the tail of snapshot

		} else {
			// The previous log is included in snapshot
			reply.NextLogIndex = rf.peerState.LastIncludedIndex + 1
			DPrintf(
				"Server %d: reject append entries because prevLog is included in snapshot, prevIndex: %d, last index in snapshot: %d",
				rf.me, args.PrevLogIndex, rf.peerState.LastIncludedIndex)
			return
		}
	} else if rf.log[prevLogLoc].Term != args.PrevLogTerm {
		// Reject the request if the previous log doesn't match
		prevLogTerm := rf.log[prevLogLoc].Term
		for i := prevLogLoc - 1; i >= 0; i-- {
			if rf.log[i].Term != prevLogTerm {
				reply.NextLogIndex = rf.log[i].Index + 1
				break
			}
		}
		if reply.NextLogIndex == 0 {
			reply.NextLogIndex = 1
		}

		return
	}

	reply.Success = true
	if len(args.Entries) > 0 {
		argLastLog := args.Entries[len(args.Entries)-1]
		if argLastLog.Term > lastLogTerm || (argLastLog.Term == lastLogTerm && argLastLog.Index >= lastLogIndex) {
			// Append entries if arg logs are newer
			rf.log = rf.log[:prevLogLoc+1]
			rf.appendLogEntries(args.Entries)
			rf.persist()
		}
	}
	if min(args.LeaderCommit, rf.getLastLogIndex()) > rf.commitIndex {
		// Update commit index
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		rf.applyCond.Broadcast()
		DPrintf("Server %d: follower update commitIndex to %d", rf.me, rf.commitIndex)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) appendLogEntries(entries []LogEntry) {
	entryLength := len(entries)
	DPrintf(
		"Server %d: append entries index: %d-%d, term: %d-%d",
		rf.me, entries[0].Index, entries[entryLength-1].Index, entries[0].Term, entries[entryLength-1].Term)
	rf.log = append(rf.log, entries...)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.stateMutex.Lock()

	peerState := rf.peerState
	index := -1
	term := -1
	isLeader := peerState.CurrentState == Leader

	if isLeader {
		// Only leader can apply new log to state machine
		term = peerState.CurrentTerm
		index = rf.getLastLogIndex() + 1
		logEntry := LogEntry{
			Term:    term,
			Command: command,
			Index:   index,
		}
		rf.appendLogEntries([]LogEntry{logEntry})
		rf.persist()

		rf.stateMutex.Unlock()
		// Broadcast to append log entries in Followers immediately
		rf.leaderBroadcast()
	} else {
		rf.stateMutex.Unlock()
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	DPrintf("Server %d: killed", rf.me)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

const rpcTimeout = 20 * time.Millisecond
const heartbeatInterval = 100 * time.Millisecond

func randomElectionTimeInMs() time.Duration {
	// Wait for [300, 500) ms
	return time.Duration(300+(rand.Int()%200)) * time.Millisecond
}

func (rf *Raft) followerProcess(peerState PeerState) {
	rf.stateMutex.Lock()

	/** Check if new election should be started */
	if !rf.receivedHeartbeat.Load() {
		DPrintf(
			"Server %d: Term %d -> %d Follower -> Candidate because election timeout",
			rf.me, peerState.CurrentTerm, peerState.CurrentTerm+1)
		// Convert to Candidate if no heartbeat from leader
		rf.peerState.CurrentState = Candidate
		rf.peerState.CurrentTerm = peerState.CurrentTerm + 1
		rf.peerState.VotedFor = rf.me
		rf.persist()
		rf.stateMutex.Unlock()
	} else {
		// Reset heartbeat flag
		rf.receivedHeartbeat.Store(false)
		rf.stateMutex.Unlock()
		time.Sleep(randomElectionTimeInMs())
	}
}

func (rf *Raft) candidateProcess(peerState PeerState) {
	/** Start new election */
	rf.stateMutex.Lock()
	voteLoop := int(2*randomElectionTimeInMs()/heartbeatInterval + 1)
	args := RequestVoteArgs{
		Term:         peerState.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.stateMutex.Unlock()

	for i := 0; !rf.killed() && i < voteLoop; i++ {
		voteCount := int32(1)
		var maxRemoteTerm atomic.Int32
		maxRemoteTerm.Store(0)
		var isMyTermObsoleted atomic.Bool
		isMyTermObsoleted.Store(false)

		// Send RequestVote to all other servers
		for server := 0; server < len(rf.peers); server++ {
			if server != rf.me {
				go func(server int) {
					reply := RequestVoteReply{}
					if rf.sendRequestVote(server, &args, &reply) {
						if reply.Term > peerState.CurrentTerm && reply.Term > int(maxRemoteTerm.Load()) {
							isMyTermObsoleted.Store(true)
							maxRemoteTerm.Store(int32(reply.Term))
						}
						if reply.VoteGranted {
							atomic.AddInt32(&voteCount, 1)
						}
					}
				}(server)
			}
		}
		time.Sleep(rpcTimeout)

		rf.stateMutex.Lock()
		if rf.peerState.CurrentState != Candidate {
			// Return immediately if I'm not Candidate
			rf.stateMutex.Unlock()
			return
		} else if isMyTermObsoleted.Load() {
			// Convert to Follower if term obsolete
			DPrintf(
				"Server %d: Term %d -> %d Candidate -> Follower because term obsolete",
				rf.me, peerState.CurrentTerm, maxRemoteTerm.Load())
			rf.peerState.CurrentState = Follower
			rf.peerState.CurrentTerm = int(maxRemoteTerm.Load())
			rf.peerState.VotedFor = -1
			rf.persist()
			rf.stateMutex.Unlock()
			return

		} else if int(voteCount) > len(rf.peers)/2 {
			// Convert to Leader if majority votes received
			DPrintf(
				"Server %d: Term %d Candidate -> Leader because receive majority votes",
				rf.me, peerState.CurrentTerm)
			rf.peerState.CurrentState = Leader
			rf.peerState.CurrentTerm = peerState.CurrentTerm
			rf.peerState.VotedFor = -1

			lastLogIndex := rf.getLastLogIndex()
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for j := 0; j < len(rf.peers); j++ {
				rf.nextIndex[j] = lastLogIndex + 1
				rf.matchIndex[j] = 0
			}

			rf.persist()
			rf.stateMutex.Unlock()
			return

		} else if i < voteLoop-1 {
			// Wait for next voteRequest
			DPrintf(
				"Server %d: Term %d Candidate -> Candidate because no majority votes received: %d",
				rf.me, peerState.CurrentTerm, int(voteCount))
			rf.stateMutex.Unlock()
			time.Sleep(heartbeatInterval)
		} else {
			rf.stateMutex.Unlock()
		}
	}

	rf.stateMutex.Lock()
	if rf.peerState.CurrentState != Candidate {
		// Return immediately if I'm not Candidate
		rf.stateMutex.Unlock()
		return
	}
	// Remains Candidate and start a new election if no leader elected
	DPrintf(
		"Server %d: Term %d -> %d Candidate -> Candidate because election timeout and no leader elected",
		rf.me, peerState.CurrentTerm, peerState.CurrentTerm+1)
	rf.peerState.CurrentState = Candidate
	rf.peerState.CurrentTerm = peerState.CurrentTerm + 1
	rf.peerState.VotedFor = rf.me
	rf.persist()
	rf.stateMutex.Unlock()
}

func (rf *Raft) leaderBroadcast() {
	var maxRemoteTerm atomic.Int32
	maxRemoteTerm.Store(0)
	var isMyTermObsoleted atomic.Bool
	isMyTermObsoleted.Store(false)

	for server := 0; server < len(rf.peers); server++ {
		if server != rf.me {
			go func(server int) {
				rf.stateMutex.Lock()
				if rf.matchIndex[server] < rf.peerState.LastIncludedIndex {
					// Send InstallSnapshot
					args := InstallSnapshotArgs{
						Term:              rf.peerState.CurrentTerm,
						LastIncludedIndex: rf.peerState.LastIncludedIndex,
						LastIncludedTerm:  rf.peerState.LastIncludedTerm,
						Snapshot:          rf.persister.ReadSnapshot(),
					}
					rf.stateMutex.Unlock()

					reply := InstallSnapshotReply{}
					if rf.sendInstallSnapshot(server, &args, &reply) {
						rf.stateMutex.Lock()
						if reply.Term > rf.peerState.CurrentTerm && reply.Term > int(maxRemoteTerm.Load()) {
							isMyTermObsoleted.Store(true)
							maxRemoteTerm.Store(int32(reply.Term))
						} else {
							rf.matchIndex[server] = args.LastIncludedIndex
							rf.nextIndex[server] = args.LastIncludedIndex + 1
						}
						rf.stateMutex.Unlock()
					}

				} else {
					// Send AppendEntries
					args := AppendEntriesArgs{
						Term:         rf.peerState.CurrentTerm,
						PrevLogIndex: rf.nextIndex[server] - 1,
						LeaderCommit: rf.commitIndex,
					}
					prevLog := rf.getLog(args.PrevLogIndex)
					if prevLog.Index == args.PrevLogIndex {
						args.PrevLogTerm = prevLog.Term
					} else if args.PrevLogIndex == rf.peerState.LastIncludedIndex {
						args.PrevLogTerm = rf.peerState.LastIncludedTerm
					} else {
						args.PrevLogTerm = 0
					}
					logLength := len(rf.log)
					nextLogLoc := rf.getLogLoc(rf.nextIndex[server])
					if logLength > 0 && nextLogLoc >= 0 {
						args.Entries = rf.log[nextLogLoc:]
					} else {
						args.Entries = make([]LogEntry, 0)
					}
					rf.stateMutex.Unlock()

					reply := AppendEntriesReply{}
					if rf.sendAppendEntries(server, &args, &reply) {
						rf.stateMutex.Lock()
						if reply.Term > rf.peerState.CurrentTerm && reply.Term > int(maxRemoteTerm.Load()) {
							isMyTermObsoleted.Store(true)
							maxRemoteTerm.Store(int32(reply.Term))
						} else if reply.Success {
							appendCount := len(args.Entries)
							rf.matchIndex[server] = max(args.PrevLogIndex+appendCount, rf.matchIndex[server])
							rf.nextIndex[server] = rf.matchIndex[server] + 1
						} else {
							if reply.NextLogIndex > 0 && reply.NextLogIndex < rf.getLastLogIndex() {
								rf.nextIndex[server] = reply.NextLogIndex
							} else if args.PrevLogIndex > 0 {
								rf.nextIndex[server] = args.PrevLogIndex
							}
						}
						rf.stateMutex.Unlock()
					}
				}
			}(server)
		}
	}
	time.Sleep(rpcTimeout)

	rf.stateMutex.Lock()
	if rf.peerState.CurrentState != Leader {
		// Return immediately if I'm not Leader
		rf.stateMutex.Unlock()
		return

	} else if isMyTermObsoleted.Load() {
		// Convert to Follower if term obsolete
		DPrintf(
			"Server %d: Term %d -> %d Leader -> Follower because term obsolete",
			rf.me, rf.peerState.CurrentTerm, maxRemoteTerm.Load())
		rf.peerState.CurrentState = Follower
		rf.peerState.CurrentTerm = int(maxRemoteTerm.Load())
		rf.peerState.VotedFor = -1
		rf.persist()
		rf.stateMutex.Unlock()

	} else {
		// Update commitIndex
		logLength := len(rf.log)
		currentTerm := rf.peerState.CurrentTerm
		for commitLoc := logLength - 1; commitLoc >= 0 && rf.log[commitLoc].Index > rf.commitIndex && rf.log[commitLoc].Term == currentTerm; commitLoc-- {
			appliedCount := 1
			for server := 0; server < len(rf.peers); server++ {
				if server != rf.me && rf.matchIndex[server] >= rf.log[commitLoc].Index {
					appliedCount++
				}
			}

			if appliedCount > len(rf.peers)/2 {
				rf.commitIndex = rf.log[commitLoc].Index
				DPrintf("Server %d: leader update commitIndex to %d", rf.me, rf.commitIndex)
				rf.applyCond.Broadcast()
				rf.stateMutex.Unlock()
				return
			}
		}

		rf.stateMutex.Unlock()
	}
}

func (rf *Raft) leaderProcess() {
	rf.leaderBroadcast()
	time.Sleep(heartbeatInterval)
}

func (rf *Raft) getUnCommittedEntries() []LogEntry {
	startIndex := rf.getLogLoc(rf.lastApplied + 1)
	endIndex := rf.getLogLoc(rf.commitIndex) + 1
	var entries []LogEntry
	if startIndex >= 0 && endIndex >= 0 && startIndex < endIndex {
		entries = make([]LogEntry, endIndex-startIndex)
		copy(entries, rf.log[startIndex:endIndex])
	} else {
		entries = make([]LogEntry, 0)
	}
	return entries
}

func (rf *Raft) applyLogEntries() {
	rf.stateMutex.Lock()
	defer rf.stateMutex.Unlock()

	for rf.killed() == false {
		if entries := rf.getUnCommittedEntries(); len(entries) > 0 {
			rf.stateMutex.Unlock()

			for _, logEntry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      logEntry.Command,
					CommandIndex: logEntry.Index,
				}
				DPrintf(
					"Server %d: apply entry: %d of term: %d",
					rf.me, logEntry.Index, logEntry.Term)
			}

			rf.stateMutex.Lock()
			rf.lastApplied = entries[len(entries)-1].Index

		} else {
			rf.applyCond.Wait()
		}
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		peerState := rf.peerState
		switch peerState.CurrentState {
		case Follower:
			rf.followerProcess(peerState)
			break
		case Candidate:
			rf.candidateProcess(peerState)
			break
		case Leader:
			rf.leaderProcess()
			break
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.stateMutex = sync.Mutex{}

	// Initialize persistent state on all servers
	rf.peerState = PeerState{
		CurrentState:      Follower,
		CurrentTerm:       0,
		VotedFor:          -1,
		LastIncludedIndex: 0,
		LastIncludedTerm:  0,
	}
	rf.log = make([]LogEntry, 0)

	// Initialize volatile state on all servers
	rf.receivedHeartbeat.Store(true)
	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCond = sync.NewCond(&rf.stateMutex)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLogEntries()

	return rf
}
