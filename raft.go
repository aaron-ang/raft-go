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

	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	lastIncludedIndex int // index of last included entry in snapshot
	lastIncludedTerm  int // term of last included entry in snapshot

	state         State
	lastHeartbeat time.Time
	applyCh       chan ApplyMsg
	bufferCh      chan ApplyMsg
}

const (
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
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
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
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.log) != nil ||
		d.Decode(&rf.lastIncludedIndex) != nil ||
		d.Decode(&rf.lastIncludedTerm) != nil {
		panic("decode error")
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}

	rf.lastIncludedTerm = rf.log[index-rf.lastIncludedIndex-1].Term
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Index == index {
			rf.log = rf.log[i+1:]
			break
		}
	}
	rf.lastIncludedIndex = index

	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
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

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	rf.lastHeartbeat = time.Now()

	if args.LastIncludedIndex <= rf.lastIncludedIndex || args.LastIncludedIndex <= rf.lastApplied {
		return
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.commitIndex = Max(args.LastIncludedIndex, rf.commitIndex)
	rf.lastApplied = args.LastIncludedIndex

	defer func() {
		rf.persist()
		rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.Data)
		rf.bufferCh <- ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()

	// if existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
	for i, logEntry := range rf.log {
		if logEntry.Index == args.LastIncludedIndex && logEntry.Term == args.LastIncludedTerm {
			rf.log = rf.log[i+1:]
			return
		}
	}

	// discard the entire log
	rf.log = make([]LogEntry, 0)
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
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Offset:            0,
		Data:              rf.persister.ReadSnapshot(),
		Done:              true,
	}
	rf.mu.Unlock()

	go func(server int) {
		reply := InstallSnapshotReply{}
		ok := rf.sendInstallSnapshot(server, &args, &reply)
		if !ok {
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()
		defer rf.persist()

		if rf.state != Leader {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.convertToFollower(reply.Term)
			return
		}

		if rf.currentTerm != args.Term {
			return
		}

		rf.nextIndex[server] = Max(args.LastIncludedIndex+1, rf.nextIndex[server])
		rf.matchIndex[server] = Max(args.LastIncludedIndex, rf.matchIndex[server])
	}(peer)
}

func (rf *Raft) convertToFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.nextIndex = nil
	rf.matchIndex = nil
}

// Returns the index of the last log entry.
func (rf *Raft) getLastLogIndex() (index int) {
	index = rf.lastIncludedIndex
	if l := len(rf.log); l > 0 {
		index = rf.log[l-1].Index
	}
	return
}

// Returns the term of the last log entry.
func (rf *Raft) getLastLogTerm() (term int) {
	term = rf.lastIncludedTerm
	if l := len(rf.log); l > 0 {
		term = rf.log[l-1].Term
	}
	return
}

func (rf *Raft) indexOffset() int {
	if rf.lastIncludedIndex == 0 {
		return 0
	}
	return rf.lastIncludedIndex + 1
}

func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		logEntry := rf.log[rf.lastApplied-rf.indexOffset()]
		rf.bufferCh <- ApplyMsg{
			CommandValid: true,
			Command:      logEntry.Command,
			CommandIndex: logEntry.Index,
		}
	}
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
	reply.VoteGranted = false

	// rule 1
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	var isUpToDate = func() bool {
		myLastIndex, myLastTerm := rf.getLastLogIndex(), rf.getLastLogTerm()
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
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1
	offset := rf.indexOffset()

	// rule 1
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	rf.lastHeartbeat = time.Now()

	// rule 2
	prevLogIndex := rf.getLastLogIndex()
	if prevLogIndex < args.PrevLogIndex {
		reply.ConflictIndex = prevLogIndex + 1
		return
	}

	var entryTerm int
	if args.PrevLogIndex <= rf.lastIncludedIndex {
		entryTerm = rf.lastIncludedTerm
	} else {
		entryTerm = rf.log[args.PrevLogIndex-offset].Term
	}
	if entryTerm != args.PrevLogTerm {
		reply.ConflictTerm = entryTerm
		for i := args.PrevLogIndex - offset; i >= 0 && rf.log[i].Term == entryTerm; i-- {
			reply.ConflictIndex = rf.log[i].Index
		}
		return
	}

	reply.Success = true

	i, j := args.PrevLogIndex+1-offset, 0
	if i >= 0 {
		for ; i < len(rf.log) && j < len(args.Entries); i, j = i+1, j+1 {
			if rf.log[i].Term != args.Entries[j].Term {
				break
			}
		}
		// rule 3
		rf.log = rf.log[:i]

		// rule 4
		if len(args.Entries) > 0 {
			rf.log = append(rf.log, args.Entries[j:]...)
		}
	}

	// rule 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.getLastLogIndex())
		go rf.applyLogs()
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

	index = rf.getLastLogIndex() + 1
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
			rf.handleCandidate()
		case Leader:
			rf.handleLeader()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) handleFollower() {
	electionTimeout := getElectionTimeout()
	time.Sleep(electionTimeout)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastHeartbeat := rf.lastHeartbeat
	if time.Since(lastHeartbeat) >= electionTimeout {
		rf.state = Candidate
	}
}

func (rf *Raft) handleCandidate() {
	electionTimeout := getElectionTimeout()
	start := time.Now()

	rf.mu.Lock()
	rf.currentTerm++
	me := rf.me
	rf.votedFor = me
	rf.persist()
	peers := rf.peers
	term := rf.currentTerm
	lastLogIndex, lastLogTerm := rf.getLastLogIndex(), rf.getLastLogTerm()
	rf.mu.Unlock()

	numVotes := 1
	numVoted := 1
	majority := (len(peers) / 2) + 1

	for peer := range peers {
		if peer == me {
			continue
		}
		go func(server int) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
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
		if rf.nextIndex[peer] <= rf.lastIncludedIndex {
			rf.startInstallSnapshot(peer)
		} else {
			rf.startAppendEntries(peer)
		}
	}
}

func (rf *Raft) startAppendEntries(peer int) {
	rf.mu.Lock()
	indexOffset := rf.indexOffset()
	prevLogIndex := rf.nextIndex[peer] - 1
	var prevLogTerm int
	if prevLogIndex < indexOffset {
		prevLogTerm = rf.lastIncludedTerm
	} else {
		prevLogTerm = rf.log[prevLogIndex-indexOffset].Term
	}
	entries := rf.log[rf.nextIndex[peer]-indexOffset:]
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      make([]LogEntry, len(entries)),
		LeaderCommit: rf.commitIndex,
	}
	copy(args.Entries, entries)
	rf.mu.Unlock()

	go func(server int) {
		reply := AppendEntriesReply{}
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
			rf.matchIndex[server] = Max(rf.matchIndex[server], prevLogIndex+len(args.Entries))
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		} else if reply.ConflictTerm == -1 {
			// log inconsistency; decrement nextIndex and retry
			rf.nextIndex[server] = reply.ConflictIndex
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		} else {
			// find index at conflict term
			newNextIndex := rf.getLastLogIndex()
			for ; newNextIndex >= rf.lastIncludedIndex; newNextIndex-- {
				if rf.log[newNextIndex-indexOffset].Term == reply.ConflictTerm {
					break
				}
			}
			if newNextIndex < rf.lastIncludedIndex {
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
		for n := rf.getLastLogIndex(); n > rf.commitIndex; n-- {
			count := 1
			if rf.log[n-indexOffset].Term == rf.currentTerm {
				for i := range rf.peers {
					if i != rf.me && rf.matchIndex[i] >= n {
						count++
					}
				}
			}
			if count >= majority {
				rf.commitIndex = n
				go rf.applyLogs()
				break
			}
		}
	}(peer)
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term: 0, Index: 0}) // dummy log entry

	rf.state = Follower
	rf.applyCh = applyCh
	rf.bufferCh = make(chan ApplyMsg, 1000)
	rf.bufferCh <- ApplyMsg{} // dummy apply message

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex

	// start ticker goroutine to start elections
	go rf.ticker()

	go func() {
		for !rf.killed() {
			rf.applyCh <- <-rf.bufferCh
		}
	}()

	return rf
}
