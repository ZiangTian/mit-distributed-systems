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
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int         // term in which the entry was created
	Command interface{} // command for state machine
}

// Raft A Go object implementing a single Raft *peer*.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	state     int                 // 0: follower, 1: candidate, 2: leader

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 3A: states
	currentTerm int
	votedFor    int
	log         []LogEntry

	// 3A: volatile states
	commitIndex int
	lastApplied int

	// 3A: leader states
	nextIndex   []int
	matchIndex  []int
	numberVotes int

	lastHeartbeat time.Time

	// 3B: applyCh
	applyCh         chan ApplyMsg
	cond            *sync.Cond
	commitIDUpdated bool

	// 3D: snapshot
	lastIncludedIndex int
	lastIncludedTerm  int
	snapShotTemp      []byte // for sending snapshots, cleared after sending
}

type PersistedState struct {
	CurrentTerm       int
	VotedFor          int
	Log               []LogEntry
	LastIncludedIndex int
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	rf.mu.Unlock()

	return term, isleader
}

func (rf *Raft) getLogLength() int {
	return rf.lastIncludedIndex + len(rf.log)
}

func (rf *Raft) getLogEntry(index int) LogEntry {
	if index <= rf.lastIncludedIndex {
		log.Fatalf("S%d: getLogEntry: index %d is less than lastIncludedIndex %d", rf.me, index, rf.lastIncludedIndex)
	} else if index > rf.lastIncludedIndex+len(rf.log)-1 {
		log.Fatalf("S%d: getLogEntry: index %d is greater than the last log index %d", rf.me, index, rf.lastIncludedIndex+len(rf.log)-1)
	}
	return rf.log[index-rf.lastIncludedIndex]
}

func (rf *Raft) getRealIndex(index int) int {
	// get the real index in the log
	return index - rf.lastIncludedIndex
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
// this func does not lock the mutex, so it should be locked before calling.
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	//labgob.LabEncoder{}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	Ps := PersistedState{
		CurrentTerm:       rf.currentTerm,
		VotedFor:          rf.votedFor,
		Log:               rf.log,
		LastIncludedIndex: rf.lastIncludedIndex,
	}
	err := e.Encode(Ps)
	if err != nil {
		panic(err)
		return
	}
	rf.persister.Save(w.Bytes(), nil)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	Ps := PersistedState{}
	if d.Decode(&Ps) != nil {
		panic("Error decoding persisted state")
		return
	} else {
		rf.currentTerm = Ps.CurrentTerm
		rf.votedFor = Ps.VotedFor
		rf.log = Ps.Log
	}
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	// taking snapshots for the committed entries in log

	if rf.killed() {
		return
	}

	rf.mu.Lock()

	if index <= rf.lastIncludedIndex {
		Debug(dSnap, "S%d: Snapshot: index %d is less than or equal to lastIncludedIndex %d", rf.me, index, rf.lastIncludedIndex)
		rf.mu.Unlock()
		return
	} else if index > rf.commitIndex {
		Debug(dSnap, "S%d: Snapshot: index %d is greater than commitIndex %d", rf.me, index, rf.commitIndex)
		rf.mu.Unlock()
		return
	}

	// truncate the log
	newLog := []LogEntry{}
	for i := index + 1; i < rf.getLogLength(); i++ {
		newLog = append(newLog, rf.getLogEntry(i))
	}

	// update lastIncludedIndex and lastIncludedTerm
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.getLogEntry(index).Term

	// update the log
	rf.log = newLog

	// persist the snapshot
	rf.persister.Save(rf.persister.ReadRaftState(), snapshot)

	// TODO do we update the commitIndex here??
	if index > rf.commitIndex {
		rf.commitIndex = index
		rf.commitIDUpdated = true
	}

	// apply the snapshot
	rf.snapShotTemp = snapshot // this automatically triggers the applyMsg
	rf.mu.Unlock()

	rf.cond.Broadcast()

}

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk
	// Offset            int    // we don't do offset here
	//Done bool // true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

// InstallSnapshot RPC handler.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		Debug(dSnap, "S%d: InstallSnapshot RPC from S%d, outdated term, returning immediately", rf.me, args.LeaderId)
		rf.mu.Unlock()
		return
	}

	makeFollower(rf, args.Term, true) // WARNING: we reset the timer here

	if args.LastIncludedIndex < rf.lastIncludedIndex {
		Debug(dSnap, "S%d: InstallSnapshot: index %d is less than to lastIncludedIndex %d", rf.me, args.LastIncludedIndex, rf.lastIncludedIndex)
		rf.mu.Unlock()
		return
	}

	// update the log, depending on whether the snapshot is a prefix of the log
	// decode the snapshot
	r := bytes.NewBuffer(args.Data)
	d := labgob.NewDecoder(r)
	le := []LogEntry{}
	if d.Decode(&le) != nil {
		panic("Error decoding snapshot")
	}

	isAPrefix := false

	// check if the snapshot is a prefix of the log
	if len(le) < rf.getLogLength() {
		for i := 0; i < len(le); i++ {
			if le[i].Term != rf.getLogEntry(i).Term {
				break
			}
			if i == len(le)-1 {
				isAPrefix = true
				// keep the remaining log entries
				if len(le) < rf.lastIncludedIndex {
					// nothing to change
				} else {
					newLogLen := rf.getLogLength() - len(le)
					rf.log = rf.log[len(rf.log)-newLogLen:]

					rf.lastIncludedIndex = args.LastIncludedIndex
					rf.lastIncludedTerm = args.LastIncludedTerm
					rf.snapShotTemp = args.Data

					if args.LastIncludedIndex > rf.commitIndex {
						rf.commitIndex = args.LastIncludedIndex
						rf.commitIDUpdated = true
					}

					rf.mu.Unlock()
					rf.cond.Broadcast()

					return
				}
			}
		}
	}
	if !isAPrefix { // if not a prefix, discard the log
		// discard the entire log, and replace it with the snapshot
		// update lastIncludedIndex and lastIncludedTerm
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.snapShotTemp = args.Data

		// discard the log
		rf.log = []LogEntry{}

		// since this is forced by the leader, we need to proactively update the commitIndex
		if args.LastIncludedIndex > rf.commitIndex {
			rf.commitIndex = args.LastIncludedIndex
			rf.commitIDUpdated = true
		}

		rf.mu.Unlock()
		rf.cond.Broadcast()
	} else {
		rf.mu.Unlock()
	}

}

// RequestVoteArgs RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).

	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// RequestVoteReply RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).

	CurrentTerm int  // current term, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.CurrentTerm = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.CurrentTerm = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		makeFollower(rf, args.Term, false)
		//rf.votedFor = -1
	}

	// voting
	//serverLastLogTerm := rf.log[len(rf.log)-1].Term
	serverLastLogTerm := rf.getLogEntry(rf.getLogLength() - 1).Term
	//serverLastLogIndex := len(rf.log) - 1
	serverLastLogIndex := rf.getLogLength() - 1
	candidateLastLogTerm := args.LastLogTerm
	candidateLastLogIndex := args.LastLogIndex

	candidateIsMoreUpdated := !isMoreUpToDate(serverLastLogTerm, serverLastLogIndex, candidateLastLogTerm, candidateLastLogIndex)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && candidateIsMoreUpdated {
		rf.lastHeartbeat = time.Now() // only reset timer now.
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
	}

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex int  // index of the conflicting entry
	ConflictTerm  int  // term of the conflicting entry
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	// invoked by the leader to replicate log entries
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		Debug(dDrop, "S%d: AppendEntries RPC from S%d, outdated term, returning immediately", rf.me, args.LeaderId)

		rf.mu.Unlock()
		return
	}

	makeFollower(rf, args.Term, true)

	// // even if it's heartbeat, we still need to check the term

	// check for conflicting entries
	//if !coherencyCheck(args.PrevLogIndex, args.PrevLogTerm, rf.log) {
	if !rf.consistencyCheck(args.PrevLogIndex, args.PrevLogTerm) {
		Debug(dLog2, "S%d received PrevLogIndex: %d, PrevLogTerm: %d, log: %v", rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.log)
		Debug(dLog2, "S%d: log is not coherent, returning immediately", rf.me)
		reply.Success = false

		// find the conflicting entry
		var conflictingIndex int
		var conflictingTerm int
		if args.PrevLogIndex >= rf.getLogLength() {
			conflictingIndex = rf.getLogLength()
			conflictingTerm = -1
		} else {
			for i := rf.lastIncludedIndex + 1; i < rf.getLogLength(); i++ {
				if rf.getLogEntry(i).Term == args.PrevLogTerm { // the first consistent entry
					conflictingIndex = i + 1 // the first inconsistent entry
					conflictingTerm = rf.getLogEntry(i + 1).Term
					break
				}
			}
		}

		reply.ConflictTerm = conflictingTerm
		reply.ConflictIndex = conflictingIndex

		// TODO update the commitIndex??

		// rf.log = rf.log[:args.PrevLogIndex] // we can't do this!!
		rf.mu.Unlock()
		return
	} else {
		Debug(dLog2, "S%d received PrevLogIndex: %d, PrevLogTerm: %d, log: %v", rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.log)
		Debug(dLog2, "S%d: log is coherent", rf.me)
		reply.Success = true

		// WARNING we cannot append the entry by truncating the log!!
		// the RPC can be outdated, so truncation would mean losing entries

		// append the entries
		// if the follower has all the entries the leader sent, the log cannot be truncated.
		// Any elements following the entries sent by the leader MUST be kept
		curLog := append([]LogEntry{}, rf.log...)
		//idLastNewEntry := len(curLog)
		idLastNewEntry := rf.getLogLength()

		if args.PrevLogIndex+len(args.Entries) < rf.getLogLength() {
			// check for conflicts
			idLastNewEntry = args.PrevLogIndex + len(args.Entries)
			for i := 0; i < len(args.Entries); i++ {
				if args.Entries[i].Term != curLog[args.PrevLogIndex+1+i].Term {
					Debug(dWarn, "S%d: Coherent but conflicting entries, deleting from %d", rf.me, args.PrevLogIndex+1+i)
					rf.log = append(rf.log[:args.PrevLogIndex+1+i], args.Entries[i:]...)
					Debug(dLog2, "S%d: the new log now %v", rf.me, rf.log)
					idLastNewEntry = rf.getLogLength() - 1
					break
				}
			}
		} else {
			rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
			idLastNewEntry = rf.getLogLength() - 1
		}

		// update the commitIndex
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = smaller(args.LeaderCommit, idLastNewEntry)
			Debug(dCommit, "S%d: commitIndex updated to %d", rf.me, rf.commitIndex)
			rf.persist()
		}

		rf.commitIDUpdated = true
		rf.mu.Unlock()

		rf.cond.Broadcast()

		// apply the message
		//rf.tryApplyMsg() // this blocks, so perhaps we should do it in a goroutine?

	}

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// sendApplyMsg sends an ApplyMsg to the applyCh channel.
// this needs to acquire the lock first, so the lock should be released outside
func (rf *Raft) tryApplyMsg() {
	rf.mu.Lock()
	// check if the commitIndex has been updated
	for rf.commitIndex > rf.lastApplied { // rf.log[lastApplied] has been applied. last applied is inited to 0

		rf.lastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			//Command:      rf.log[rf.lastApplied].Command,
			Command:      rf.getLogEntry(rf.lastApplied).Command,
			CommandIndex: rf.lastApplied,
		}
		Debug(dCommit, "S%d Applying command %v at index %d", rf.me, rf.getLogEntry(rf.lastApplied).Command, rf.lastApplied)
		//rf.mu2.Lock()
		rf.mu.Unlock()
		rf.applyCh <- msg
		//rf.mu2.Unlock()
		rf.mu.Lock()
	}
	rf.mu.Unlock()
}

func (rf *Raft) keepApplyMsg() {
	for !rf.killed() {

		rf.cond.L.Lock()
		for !rf.commitIDUpdated || rf.snapShotTemp == nil {
			rf.cond.Wait()
		}

		// check if the commitIndex has been updated
		for rf.commitIndex > rf.lastApplied { // rf.log[lastApplied] has been applied. last applied is inited to 0
			rf.lastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.getLogEntry(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			}
			Debug(dClient, "S%d Applying command %v at index %d", rf.me, rf.getLogEntry(rf.lastApplied).Command, rf.lastApplied)
			//rf.mu2.Lock()
			rf.cond.L.Unlock()
			rf.applyCh <- msg
			//rf.mu2.Unlock()
			rf.cond.L.Lock()
		}
		if rf.snapShotTemp == nil { // if not caused by update, we don't need to reset the flag
			rf.commitIDUpdated = false
		}

		// or when there is a new snapshot
		if rf.snapShotTemp != nil {
			msg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapShotTemp,
				SnapshotIndex: rf.lastIncludedIndex, // TODO not sure if this is correct
				SnapshotTerm:  rf.lastIncludedTerm,
			}
			Debug(dSnap, "S%d Applying snapshot at index %d", rf.me, rf.lastIncludedIndex)
			rf.snapShotTemp = nil
			rf.cond.L.Unlock()
			rf.applyCh <- msg
			rf.cond.L.Lock()
		}

		rf.cond.L.Unlock()

	}
}

// Start the service using Raft (e.g. a k/v server) wants to start
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
	// Your code here (3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	if isLeader {
		// append the command to the log
		DPrintf("Starting command %v for server %d", command, rf.me)
		index = rf.getLogLength() // if it ever gets committed, it will be at the end of the log
		rf.log = append(rf.log, LogEntry{Term: term, Command: command})

		// go rf.syncLog()
	}

	return index, term, isLeader
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) checkTimeout() {
	rf.mu.Lock()
	//defer rf.mu.Unlock()

	if rf.state == LEADER {
		return
	}

	electionTimeout := 250 + rand.Intn(150)

	lastHeartbeat := rf.lastHeartbeat

	if time.Since(lastHeartbeat) > time.Duration(electionTimeout)*time.Millisecond {
		Debug(dTimer, "S%d Follower, election timeout", rf.me)
		rf.state = CANDIDATE

		rf.mu.Unlock()
		go rf.startElection()
	} else {
		rf.mu.Unlock()
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case FOLLOWER:
			rf.checkTimeout()
		case CANDIDATE:
			//	rf.startElection()	// relaunch the election even if it's already a candidate
			rf.checkTimeout()
		case LEADER:
			//rf.sendHeartbeats() // this does not block.
			rf.syncLog()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 100)
		//ms := 100
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// startElection checks if an election should be started and starts it if necessary; it
// is triggered when a follower hasn't received a heartbeat for a while
func (rf *Raft) startElection() {

	rf.mu.Lock()

	if rf.state != CANDIDATE {
		rf.mu.Unlock()
		return
	}
	Debug(dVote, "S%d starting election", rf.me)

	// increment current term
	rf.currentTerm++
	// vote for self
	rf.votedFor = rf.me
	rf.numberVotes = 1
	rf.lastHeartbeat = time.Now() // reset timer

	args := RequestVoteArgs{
		Term:         rf.currentTerm, // curTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLogLength() - 1,
		LastLogTerm:  rf.log[rf.getLogLength()-1].Term,
	}

	rf.mu.Unlock()

	// send RequestVote RPCs to all other servers
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			Debug(dVote, "S%d -> S%d, RequestVote with term %d, lastLogIndex %d, lastLogTerm %d", rf.me, server, args.Term, args.LastLogIndex, args.LastLogTerm)

			reply := RequestVoteReply{}
			success := rf.sendRequestVote(server, &args, &reply)

			// check if the election was successful
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if success {
				if reply.CurrentTerm > rf.currentTerm {
					makeFollower(rf, reply.CurrentTerm, false)
					//DPrintf("Raft server %d is no longer a candidate for term %d", rf.me, rf.currentTerm)
					Debug(dDrop, "S%d No longer candidate", rf.me)
					return
				}
				if rf.currentTerm != args.Term || rf.state != CANDIDATE {
					return
				}

				if reply.VoteGranted {
					rf.numberVotes++

					// check if the votes are enough
					if rf.numberVotes > len(rf.peers)/2 {
						makeLeader(rf)
						//rf.log = append(rf.log, []LogEntry{}...) // let's try this
						//DPrintf("Raft server %d is the leader for term %d", rf.me, rf.currentTerm)
						Debug(dVote, "S%d is the leader for term %d", rf.me, rf.currentTerm)
					}
				}
			}
		}(i)
	}
}

// syncLog syncs the log with the other servers;
// it acts both as a heartbeat and a log replication.
// This function takes place periodically in the leader in ticker.
// syncLog syncs the log with the other servers;
// it acts both as a heartbeat and a log replication.
// This function takes place periodically in the leader in ticker.
func (rf *Raft) syncLog() {

	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}

	Debug(dLeader, "S%d Leader, syncing log", rf.me)
	// TODO: How do we "INITIALIZE" nextIndex?

	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			for {
				// an infinite loop to retry until the log is synced or the leader is no longer the leader

				// prepare args to send to all the servers
				// PrevLogIndex: the log index previous to the one to be sent
				rf.mu.Lock()

				if rf.state != LEADER {
					rf.mu.Unlock()
					return
				}

				entriesToSend := []LogEntry{}
				if rf.getLogLength() > 1 {
					entriesToSend = append(entriesToSend, rf.log[rf.getRealIndex(rf.nextIndex[server]):]...) // on init, next is 1, so we send log[1:]
				}

				Debug(dLog, "S%d -> S%d, Sending entries: %v", rf.me, server, entriesToSend)
				Debug(dLog, "S%d -> S%d, S%d log now %v, S%d nextIndex %d", rf.me, server, rf.me, rf.log, server, rf.nextIndex[server])
				Debug(dLeader, "S%d -> S%d, Sending PLI: %d, PLT: %d", rf.me, server, rf.nextIndex[server]-1, rf.getLogEntry(rf.nextIndex[server]-1).Term)

				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[server] - 1, // use the fake index here bc it's been taken care of in appendEntries().
					PrevLogTerm:  rf.getLogEntry(rf.nextIndex[server] - 1).Term,
					Entries:      entriesToSend,
					LeaderCommit: rf.commitIndex,
				}

				rf.mu.Unlock()

				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, &args, &reply)

				// check if the leader term is still valid
				rf.mu.Lock()
				// rf.lastestTerm = rf.lastestTerm.lastestTerm && (reply.Term <= rf.currentTerm)
				if ok {
					if reply.Term > rf.currentTerm {
						makeFollower(rf, reply.Term, false)
						Debug(dDrop, "S%d No longer leader", rf.me)
						rf.mu.Unlock()
						return
					}
					if rf.currentTerm != args.Term || rf.state != LEADER { // avoid old RPCs reply
						rf.mu.Unlock()
						return
					}

					if reply.Success {
						// update nextIndex and matchIndex
						Debug(dLeader, "S%d -> S%d, Success, updating nextIndex and matchIndex", rf.me, server)

						rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
						rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)

						// check if the leader can commit the entry
						tempArr := make([]int, len(rf.peers))
						copy(tempArr, rf.matchIndex)
						tempArr[rf.me] = rf.getLogLength() - 1 // len(rf.log) - 1 // UPDATE!!!!
						sort.Ints(tempArr)
						N := tempArr[len(rf.peers)/2]
						Debug(dLeader, "The median commitID now is %d ", N)
						if N > rf.commitIndex && rf.getLogEntry(N).Term == rf.currentTerm {
							Debug(dCommit, "S%d commitIndex updated to %d", rf.me, N)
							rf.commitIndex = N
							rf.commitIDUpdated = true

							rf.persist()
							rf.mu.Unlock()

							rf.cond.Broadcast()
						} else {
							rf.mu.Unlock()
						}

						// try to apply the message
						//rf.tryApplyMsg()
						return
					} else {
						// decrement nextIndex and retry
						Debug(dLeader, "S%d -> S%d, Failure, decrementing nextIndex", rf.me, server)
						//rf.nextIndex[server] = bigger(1, rf.nextIndex[server]-1)

						// if leader has XTerm,
						leaderHasXterm := false
						for i := rf.getLogLength() - 1; i >= 0; i-- {
							if rf.getLogEntry(i).Term == reply.ConflictTerm {
								leaderHasXterm = true
								rf.nextIndex[server] = i + 1
								break
							}
						}

						if !leaderHasXterm {
							rf.nextIndex[server] = reply.ConflictIndex
						}

						rf.nextIndex[server] = bigger(1, rf.nextIndex[server])

					}
				} else {
					// retry
					Debug(dLeader, "retrying sending entries to S%d", server)
				}
				rf.mu.Unlock()
				time.Sleep(50 * time.Millisecond)
			}
		}(i)
	}
}

// Make the service or tester wants to create a Raft server. the ports
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
	rf.applyCh = applyCh
	rf.snapShotTemp = nil
	rf.lastIncludedIndex = -1
	rf.lastIncludedTerm = -1

	// print info
	Debug(dInfo, "Raft server %d created", rf.me)

	// Your initialization code here (3A, 3B, 3C).
	makeFollower(rf, 0, true)    // currentTerm = 0
	rf.log = make([]LogEntry, 1) // log[0] is a dummy entry

	rf.commitIndex = 0 // the index of the highest log entry known to be committed: 0
	rf.lastApplied = 0 // the index of the highest log entry applied to the state machine: 0

	// use a sync.cond variable to link the commitIndex with applyCh
	rf.cond = sync.NewCond(&rf.mu)
	rf.commitIDUpdated = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.keepApplyMsg()

	return rf
}
