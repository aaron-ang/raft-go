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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aaronang/raft-go/pkg/labgob"
	"github.com/aaronang/raft-go/pkg/labrpc"
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
	Term    int
	Index   int
	Command interface{}
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what state a Raft server must maintain.

	state         State     // current state of the server
	currentTerm   int       // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor      int       // candidateId that received vote in current term (or null if none)
	lastHeartbeat time.Time // time of last heartbeat received

	log         []LogEntry    // log entries; each entry contains command for state machine, and term when entry was received by leader
	commitIndex int           // index of highest log entry known to be committed
	lastApplied int           // index of highest log entry applied to state machine
	nextIndex   []int         // index of the next log entry to send to each server
	matchIndex  []int         // index of highest log entry known to be replicated on each server
	applyCh     chan ApplyMsg // channel to send ApplyMsg
	applyCond   *sync.Cond    // condition variable to signal when to apply logs

	snapshot []byte // snapshot of the server state
}

const (
	HeartbeatInterval  = 100 * time.Millisecond
	MinElectionTimeout = 360
	MaxElectionTimeout = 600
)

func getElectionTimeout() time.Duration {
	randTimeout := MinElectionTimeout + rand.Intn(MaxElectionTimeout-MinElectionTimeout)
	return time.Duration(randTimeout) * time.Millisecond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) GetIndex(index int) int {
	return index - rf.log[0].Index
}

func (rf *Raft) GetLogEntry(index int) LogEntry {
	if index < 0 { // last included index
		index += rf.log[0].Index + len(rf.log)
	}
	return rf.log[rf.GetIndex(index)]
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.Save(data, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry

	if d.Decode(&currentTerm) == nil {
		rf.currentTerm = currentTerm
	} else {
		return
	}

	if d.Decode(&votedFor) == nil {
		rf.votedFor = votedFor
	} else {
		return
	}

	if d.Decode(&log) == nil {
		rf.log = log
		rf.lastApplied = rf.log[0].Index
		rf.commitIndex = rf.log[0].Index
	} else {
		return
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if index < rf.log[0].Index {
		return
	}

	log := make([]LogEntry, 0)
	log = append(log, LogEntry{
		Term:  rf.GetLogEntry(index).Term,
		Index: rf.GetLogEntry(index).Index,
	})
	rf.log = append(log, rf.log[rf.GetIndex(index+1):]...)
	rf.snapshot = snapshot
}

type InstallSnapshotArgs struct {
	Term              int    // leader’s term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Offset            int    // byte offset where chunk is positioned in the snapshot file
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
	Done              bool   // true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

// InstallSnapshot RPC handler.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	rf.lastHeartbeat = time.Now()

	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	if args.Done {
		applyMsg := ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		rf.mu.Unlock()
		rf.applyCh <- applyMsg
		rf.mu.Lock()

		rf.snapshot = args.Data
		rf.lastApplied = args.LastIncludedIndex
		rf.commitIndex = args.LastIncludedIndex

		// if existing log entry has same index and term as snapshot’s last included entry,
		// retain log entries following it and reply
		for i, logEntry := range rf.log {
			if logEntry.Index == args.LastIncludedIndex && logEntry.Term == args.LastIncludedTerm {
				rf.log = rf.log[i+1:]
				return
			}
		}
		// discard the entire log
		rf.log = []LogEntry{{
			Term:  args.LastIncludedTerm,
			Index: args.LastIncludedIndex,
		}}
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) startInstallSnapshot(peer int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log[0].Index,
		LastIncludedTerm:  rf.log[0].Term,
		Offset:            0,
		Data:              rf.snapshot,
		Done:              true,
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()

	go func(server int) {
		ok := rf.sendInstallSnapshot(server, &args, &reply)
		if !ok {
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			rf.convertToFollower(reply.Term)
			rf.persist()
		}

		if rf.state != Leader || rf.currentTerm != reply.Term {
			return
		}

		rf.matchIndex[server] = rf.log[0].Index
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	}(peer)
}

func (rf *Raft) convertToFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.nextIndex = nil
	rf.matchIndex = nil
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).

	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm

	// rule 1
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	var isUpToDate = func() bool {
		myLastEntry := rf.GetLogEntry(-1)
		myLastIndex, myLastTerm := myLastEntry.Index, myLastEntry.Term
		return args.LastLogTerm > myLastTerm ||
			(args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex)
	}

	// rule 2
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isUpToDate() {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.state = Follower
		reply.VoteGranted = true
		rf.lastHeartbeat = time.Now()
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect entries
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex int
	ConflictTerm  int
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	// rule 1
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	rf.lastHeartbeat = time.Now()

	// rule 2
	prevLogIndex := rf.GetLogEntry(-1).Index
	if prevLogIndex < args.PrevLogIndex {
		reply.ConflictIndex = prevLogIndex + 1
		return
	}

	var entryTerm = rf.GetLogEntry(args.PrevLogIndex).Term
	if entryTerm != args.PrevLogTerm {
		reply.ConflictTerm = entryTerm
		for i := args.PrevLogIndex; i >= rf.log[0].Index && rf.GetLogEntry(i).Term == entryTerm; i-- {
			reply.ConflictIndex = i
		}
		return
	}

	reply.Success = true

	for i := 0; i < len(args.Entries); i++ {
		logIndex := args.PrevLogIndex + i + 1
		// rule 3
		if logIndex <= rf.GetLogEntry(-1).Index && rf.GetLogEntry(-1).Term != args.Entries[i].Term {
			rf.log = rf.log[:rf.GetIndex(logIndex)]
		}
		// rule 4
		if logIndex > rf.GetLogEntry(-1).Index {
			rf.log = append(rf.log, args.Entries[i])
		}
	}
	// rule 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.GetLogEntry(-1).Index)
		rf.applyCond.Signal()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log.
// if this server isn't the leader, returns false,
// otherwise start the agreement and return immediately.
// there is no guarantee that this command will ever be committed to the Raft log,
// since the leader may fail or lose an election.
// even if the Raft instance has been killed, this function should return gracefully.
//
// the first return value is the index that the command will appear at if it's ever committed.
// the second return value is the current term.
// the third return value is true if this server believes it is the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index = rf.GetLogEntry(-1).Index + 1
	term = rf.currentTerm
	isLeader = rf.state == Leader
	if !isLeader {
		return index, term, false
	}

	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	rf.log = append(rf.log, LogEntry{Term: term, Index: index, Command: command})
	rf.persist()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method.
// your code can use killed() to check whether Kill() has been called.
// the use of atomic avoids the need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine handles the state transitions and timeouts.
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case Follower:
			rf.handleFollower()
		case Candidate:
			rf.handleElection()
		case Leader:
			rf.handleLeader()
		}
	}
}

func (rf *Raft) handleFollower() {
	electionTimeout := getElectionTimeout()
	time.Sleep(electionTimeout)

	rf.mu.Lock()

	if time.Since(rf.lastHeartbeat) >= electionTimeout {
		rf.state = Candidate
	}
	rf.mu.Unlock()
}

func (rf *Raft) handleElection() {
	electionTimeout := getElectionTimeout()
	start := time.Now()

	rf.mu.Lock()
	rf.currentTerm++
	me := rf.me
	rf.votedFor = me
	rf.persist()
	peers := rf.peers
	term := rf.currentTerm
	lastLogIndex, lastLogTerm := rf.GetLogEntry(-1).Index, rf.GetLogEntry(-1).Term
	rf.mu.Unlock()

	numVotes := 1
	numVoted := 1
	majority := (len(peers) / 2) + 1

	for peer := range peers {
		if peer == me {
			continue
		}

		args := RequestVoteArgs{
			Term:         term,
			CandidateId:  me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		reply := RequestVoteReply{}

		go func(server int) {
			ok := rf.sendRequestVote(server, &args, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			numVoted++
			if !ok {
				return
			}

			if reply.VoteGranted {
				numVotes++
			} else if reply.Term > args.Term {
				rf.convertToFollower(reply.Term)
				rf.persist()
			}
		}(peer)
	}

	// wait for votes
	for {
		rf.mu.Lock()
		if numVotes >= majority ||
			numVoted == len(peers) ||
			time.Since(start) >= electionTimeout {
			break
		}
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}

	// resolve election
	if time.Since(start) >= electionTimeout {
		rf.state = Follower
		rf.mu.Unlock()
		return
	}

	if rf.state == Candidate && numVotes >= majority {
		rf.state = Leader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for peer := range peers {
			rf.nextIndex[peer] = lastLogIndex + 1
		}
	} else {
		rf.state = Follower
	}
	rf.mu.Unlock()
}

func (rf *Raft) handleLeader() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		rf.mu.Lock()
		nextIndex, firstIndex := rf.nextIndex[peer], rf.log[0].Index
		rf.mu.Unlock()

		if nextIndex <= firstIndex {
			// leader has already discarded the next log entry
			// that it needs to send to this peer
			rf.startInstallSnapshot(peer)
		} else {
			rf.startAppendEntries(peer)
		}
	}
	time.Sleep(HeartbeatInterval)
}

func (rf *Raft) startAppendEntries(peer int) {
	rf.mu.Lock()
	startIndex := rf.nextIndex[peer]
	prevLogIndex := startIndex - 1
	prevLogTerm := rf.GetLogEntry(prevLogIndex).Term
	entries := make(
		[]LogEntry,
		len(rf.log[rf.GetIndex(startIndex):]),
	)
	copy(entries, rf.log[rf.GetIndex(startIndex):])
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	rf.mu.Unlock()

	go func(server int) {
		ok := rf.sendAppendEntries(server, &args, &reply)
		if !ok {
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()
		defer rf.persist()

		if reply.Term > rf.currentTerm {
			rf.convertToFollower(reply.Term)
			return
		}

		if rf.state != Leader || rf.currentTerm != args.Term {
			return
		}

		if reply.Success {
			// update nextIndex and matchIndex for follower
			rf.matchIndex[server] = prevLogIndex + len(entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		} else if reply.ConflictTerm == -1 {
			// log inconsistency; decrement nextIndex and retry
			rf.nextIndex[server] = reply.ConflictIndex
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		} else {
			// find index at conflict term
			newNextIndex := rf.GetLogEntry(-1).Index
			for ; newNextIndex >= rf.log[0].Index; newNextIndex-- {
				if rf.GetLogEntry(newNextIndex).Term == reply.ConflictTerm {
					break
				}
			}
			if newNextIndex < rf.log[0].Index {
				rf.nextIndex[server] = reply.ConflictIndex
			} else {
				rf.nextIndex[server] = newNextIndex
			}
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		}

		// if there exists an N such that N > commitIndex, a majority
		// of matchIndex[i] >= N, and log[N].term == currentTerm:
		// set commitIndex = N
		majority := (len(rf.peers) / 2) + 1
		for n := rf.GetLogEntry(-1).Index; n > rf.commitIndex; n-- {
			if rf.GetLogEntry(n).Term != rf.currentTerm {
				continue
			}

			count := 0
			for _, i := range rf.matchIndex {
				if i >= n {
					count++
				}
			}
			if count >= majority {
				rf.commitIndex = n
				rf.applyCond.Signal()
				break
			}
		}
	}(peer)
}

func (rf *Raft) applyLogs() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		commitIndex := rf.commitIndex
		queue := make([]ApplyMsg, 0)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			queue = append(queue, ApplyMsg{
				CommandValid: true,
				Command:      rf.GetLogEntry(i).Command,
				CommandIndex: rf.GetLogEntry(i).Index,
			})
		}
		rf.mu.Unlock()

		for _, applyMsg := range queue {
			rf.applyCh <- applyMsg
		}

		rf.mu.Lock()
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server.
// the ports of all the Raft servers (including this one) are in peers[].
// this server's port is peers[me].
// all the servers' peers[] arrays have the same order.
// persister is a place for this server to save its persistent state,
// and also initially holds the most recent saved state, if any.
// applyCh is a channel on which the tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.log = append(rf.log, LogEntry{Term: 0, Index: 0}) // dummy log entry
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLogs()

	return rf
}
