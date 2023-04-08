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
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"cs350/labrpc"
)

// import "bytes"
// import "cs350/labgob"

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

	state         State
	lastHeartbeat time.Time
	applyCh       chan ApplyMsg
}

const MinElectionTimeout = 360
const MaxElectionTimeout = 600

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
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

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

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// rule 1
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	myLastIndex := len(rf.log) - 1
	myLastTerm := rf.log[myLastIndex].Term

	var isUpToDate = func() bool {
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
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	// rule 1
	if args.Term < rf.currentTerm {
		return
	}

	// rule 2
	prevLogIndex := len(rf.log) - 1
	if prevLogIndex < args.PrevLogIndex {
		return
	} else if entryTerm := rf.log[args.PrevLogIndex].Term; entryTerm != args.PrevLogTerm {
		return
	}

	reply.Success = true
	rf.lastHeartbeat = time.Now()

	index := 0
	for ; index < len(args.Entries); index++ {
		if args.PrevLogIndex+index+1 >= len(rf.log) {
			break
		}
		if rf.log[args.PrevLogIndex+index+1].Term != args.Entries[index].Term {
			rf.log = rf.log[:args.PrevLogIndex+index+1]
			break
		}
	}

	// rule 4
	if len(args.Entries) > 0 {
		rf.log = append(rf.log, args.Entries[index:]...)
	}

	// rule 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
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

	term = rf.currentTerm
	isLeader = rf.state == Leader
	if !isLeader {
		return index, term, false
	}

	rf.log = append(rf.log, LogEntry{term, command})
	index = len(rf.log) - 1

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

		if state == Follower {
			rf.handleFollower()
		} else if state == Candidate {
			rf.handleCandidate()
		} else {
			rf.handleLeader()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) handleFollower() {
	electionTimeout := getElectionTimeout()
	time.Sleep(electionTimeout)

	rf.mu.Lock()
	lastHeartbeat := rf.lastHeartbeat
	rf.mu.Unlock()

	if time.Since(lastHeartbeat) >= electionTimeout {
		rf.mu.Lock()
		defer rf.mu.Unlock()
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
	peers := rf.peers
	term := rf.currentTerm
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	rf.mu.Unlock()

	numVotes := 1
	numVoted := 1
	majority := (len(peers) / 2) + 1

	for peer := range peers {
		if peer == me {
			continue
		}
		go func(peer int) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peer, &args, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			numVoted++
			if !ok {
				return
			}
			if reply.VoteGranted {
				numVotes++
			} else if args.Term < reply.Term {
				rf.state = Follower
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
			rf.nextIndex[peer] = len(rf.log)
		}
	} else {
		rf.state = Follower
	}
	rf.mu.Unlock()
}

func (rf *Raft) handleLeader() {
	rf.mu.Lock()
	peers := rf.peers
	me := rf.me
	term := rf.currentTerm
	log := rf.log
	lastLogIndex := len(rf.log) - 1
	commitIndex := rf.commitIndex

	nextIndex := rf.nextIndex
	matchIndex := rf.matchIndex
	nextIndex[me] = lastLogIndex + 1
	matchIndex[me] = lastLogIndex
	rf.mu.Unlock()

	// commit log entries
	majority := (len(peers) / 2) + 1
	for i := commitIndex + 1; i <= lastLogIndex; i++ {
		count := 0
		for peer := range peers {
			if matchIndex[peer] >= i && log[i].Term == term {
				count++
			}
		}

		if count >= majority {
			rf.mu.Lock()
			for j := rf.commitIndex + 1; j <= i; j++ {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[j].Command,
					CommandIndex: j,
				}
				rf.commitIndex++
			}
			rf.mu.Unlock()
		}
	}

	// send heartbeat
	for peer := range peers {
		if peer == me {
			continue
		}

		rf.mu.Lock()
		args := AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: me,
		}
		rf.mu.Unlock()

		reply := AppendEntriesReply{}
		go func(peer int) {
			ok := rf.sendAppendEntries(peer, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > args.Term {
				rf.state = Follower
				return
			}
		}(peer)
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.state = Follower
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
