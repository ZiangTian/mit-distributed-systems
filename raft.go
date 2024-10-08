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
	"sort"

	//	"bytes"
	"math/rand"
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

const FOLLOWER = 0
const CANDIDATE = 1
const LEADER = 2
const ELECTIONTIMEOUTBASE = 550

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
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// 3A: leader states
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of the highest log entry known to be replicated on that server

	lastHeartbeat time.Time

	numberVotes     int
	replicatedCount int
	mu2             sync.Mutex // protect shared variables among goroutines
	applyCh         chan ApplyMsg
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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
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
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).

	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).

	CurrentTerm int  // current term, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func isMoreUpToDate(lastLogTerm, lastLogIndex, candidateLastLogTerm, candidateLastLogIndex int) bool {
	if lastLogTerm > candidateLastLogTerm {
		return true
	} else if lastLogTerm == candidateLastLogTerm {
		return lastLogIndex > candidateLastLogIndex
	}
	return false
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.CurrentTerm = rf.currentTerm
		return
	}

	makeFollower(rf, args.Term) // enforce the follower state

	rf.lastHeartbeat = time.Now()

	// voting
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && !isMoreUpToDate(rf.log[len(rf.log)-1].Term, len(rf.log)-1, args.LastLogTerm, args.LastLogIndex) {
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
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// invoked by the leader to replicate log entries
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// make follower
	makeFollower(rf, args.Term) // since this is a new term, reset votedFor

	// check if is heartbeat
	if len(args.Entries) == 0 {
		reply.Success = true

		// update commitIndex
		if args.LeaderCommit >= rf.commitIndex {
			indexLastNewEntry := len(rf.log) - 1
			rf.commitIndex = int(math.Min(float64(indexLastNewEntry), float64(args.LeaderCommit))) // the leader keeps the highest commitIndex
		} else {
			// follower commitIndex larger than leader's
			panic("follower commitIndex larger than leader's")
		}

		// apply the log entries to the state machine when the majority of servers have replicated the entry. could be done in a separate goroutine

		rf.sendApplyMsg() // already up-to-date with leader's commitIndex. therefore can be called here

		return
	}

	// check coherency
	DPrintf("Checking coherency for server %d", rf.me)
	DPrintf("PrevLogIndex: %d, PrevLogTerm: %d", args.PrevLogIndex, args.PrevLogTerm)
	// print rf.log
	DPrintf("Log length for server %d is %d", rf.me, len(rf.log))

	// 如果PrevLogIndex是1的话 (上一个被commit的是第2个元素)，rf.log里应该有一个一样的1号元素 (第二个元素)
	if args.PrevLogIndex > len(rf.log)-1 || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		// delete all entries after PrevLogIndex
		rf.log = rf.log[:args.PrevLogIndex]
		DPrintf("Coherency check failed for server %d", rf.me)
		// coherency check failed
		return
	}
	DPrintf("Coherency check passed for server %d", rf.me)
	reply.Success = true

	// matched, append entries
	DPrintf("Appending entries for server %d, previous entries are: %v", rf.me, rf.log[:args.PrevLogIndex+1])
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)

	// update commitIndex
	if args.LeaderCommit >= rf.commitIndex {
		indexLastNewEntry := len(rf.log) - 1
		rf.commitIndex = int(math.Min(float64(indexLastNewEntry), float64(args.LeaderCommit))) // the leader keeps the highest commitIndex
	} else {
		// follower commitIndex larger than leader's
		panic("follower commitIndex larger than leader's")
	}

	// apply the log entries to the state machine when the majority of servers have replicated the entry. could be done in a separate goroutine

	rf.sendApplyMsg() // already up-to-date with leader's commitIndex. therefore can be called here

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
	// ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	// // check if the election was successful
	// // retry at most 3 times
	// for !ok {
	// 	time.Sleep(10 * time.Millisecond)
	// 	DPrintf("RETRYING: Raft server %d failed to send RequestVote to server %d", rf.me, server)
	// 	ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	// }

	// // i think there's a timeout for retries.
	// return ok
	timeout := 100 * time.Millisecond
	done := make(chan bool, 1)

	go func() {
		for {
			ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
			if ok {
				done <- true
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	select {
	case <-done:
		return true
	case <-time.After(timeout): // Stop retrying after timeout
		DPrintf("Timeout: Raft server %d couldn't send RequestVote to server %d", rf.me, server)
		return false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	// // check if the election was successful
	// for !ok {
	// 	time.Sleep(10 * time.Millisecond)
	// 	DPrintf("RETRYING: Raft server %d failed to send AppendEntries to server %d", rf.me, server)
	// 	ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// }

	// return ok
	timeout := 100 * time.Millisecond
	done := make(chan bool, 1)

	go func() {
		for {
			ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
			if ok {
				done <- true
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	select {
	case <-done:
		return true
	case <-time.After(timeout): // Stop retrying after timeout
		//DPrintf("Timeout: Raft server %d couldn't send AppendEntries to server %d", rf.me, server)
		return false
	}
}

//func (rf *Raft) ReplicateEntries(server int, cond *sync.Cond) {
//	// called by leader. leader would have already acquired the lock
//	// but the leader also starts many goroutines to replicate entries to all other servers, using this func
//
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//
//	DPrintf("Raft server %d replicating entries to server %d", rf.me, server)
//	// rf already locked outside
//	//prevlogId := rf.nextIndex[server] - 1
//	//if prevlogId < 0 {
//	//
//	//}
//	DPrintf("log length for server %d is %d", rf.me, len(rf.log))
//	DPrintf("Sending entries: %v", rf.log[len(rf.log)-1:])
//	args := AppendEntriesArgs{
//		Term:         rf.currentTerm,
//		LeaderId:     rf.me,
//		PrevLogIndex: len(rf.log) - 2, // first time: 0
//		PrevLogTerm:  rf.log[len(rf.log)-2].Term,
//		Entries:      rf.log[len(rf.log)-1:],
//		LeaderCommit: rf.commitIndex,
//	}
//	reply := AppendEntriesReply{}
//
//	half := (len(rf.peers) + 1) / 2
//
//	ok := rf.sendAppendEntries(server, &args, &reply)
//
//	// if !ok {
//	// 	DPrintf("Raft server %d failed to replicate entries to server %d, retrying indefinitely", rf.me, server)
//	// }
//	for !ok {
//		ok = rf.sendAppendEntries(server, &args, &reply)
//		time.Sleep(100 * time.Millisecond)
//	}
//
//	// parse the reply
//
//	// check term validity
//
//	if reply.Term > rf.currentTerm {
//		rf.revertToFollower(reply.Term)
//		DPrintf("Leader server %d outdated for term %d", rf.me, rf.currentTerm)
//		// invalidate leader data
//		// let the outside func check the status
//		return
//	}
//
//	// check for inconsistencies
//	for !reply.Success { // we could use recursion here, but take up too much stack space
//
//		DPrintf("Raft server %d failed to replicate entries to server %d due to inconsistency, retrying", rf.me, server)
//
//		// decrement nextIndex and retry
//		rf.mu2.Lock()
//
//		// retry
//		rf.nextIndex[server]--
//		args.PrevLogIndex = rf.nextIndex[server] - 1
//		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
//		args.Entries = rf.log[args.PrevLogIndex:]
//
//		rf.mu2.Unlock()
//
//		// repeat. will optimize the code to reduce redundancy later
//
//		ok := rf.sendAppendEntries(server, &args, &reply)
//
//		for !ok {
//			ok = rf.sendAppendEntries(server, &args, &reply)
//			time.Sleep(10 * time.Millisecond)
//		}
//
//		// parse the reply
//
//		// check term validity
//		if reply.Term > rf.currentTerm {
//			rf.revertToFollower(reply.Term)
//			// invalidate leader data
//			// let the outside func check the status
//			return
//		}
//
//		time.Sleep(20 * time.Millisecond)
//	}
//
//	DPrintf("Raft server %d successfully replicated entries to server %d", rf.me, server)
//
//	// success, matched
//	rf.mu2.Lock()
//	// update nextIndex and matchIndex
//	rf.matchIndex[server] = len(rf.log) - 1
//	rf.nextIndex[server] = len(rf.log)
//
//	// update replicatedCount
//	rf.replicatedCount++
//	if rf.replicatedCount == half {
//		// update commitIndex
//		DPrintf("Half of the servers have replicated the entry, lastest is %d, broadcasting", server)
//		cond.Broadcast() // notify all goroutines that the commitIndex has been updated
//	}
//	rf.mu2.Unlock()
//
//}

//func (rf *Raft) sendReplicateEntries() {
//	// what happens if the leader is not the leader anymore in this function??????
//
//	// send AppendEntries RPCs to all other servers
//
//	// var mu sync.Mutex
//	half := (len(rf.peers) + 1) / 2
//	cond := sync.NewCond(&rf.mu2)
//
//	// issue AppendEntries RPCs to all other servers
//	for i := 0; i < len(rf.peers); i++ {
//		if i == rf.me {
//			continue
//		}
//		DPrintf("Leader %d Firing off append entries to server %d", rf.me, i)
//		go rf.ReplicateEntries(i, cond)
//	}
//
//	rf.mu2.Lock()
//
//	for rf.replicatedCount < half {
//		DPrintf("replicatedCount is %d, waiting for majority", rf.replicatedCount)
//		cond.Wait()
//		time.Sleep(10 * time.Millisecond)
//	}
//	rf.mu2.Unlock()
//
//	// we ignore the cases where the leader gets ousted in the interim
//	// because this function is atomic to other functions that could change the leader status
//
//	rf.mu2.Lock()
//	defer rf.mu2.Unlock()
//
//	// print peer log contents to ensure consistency
//
//	if rf.state != 2 {
//		panic("Leader status changed while appending entries")
//	}
//
//	rf.commitIndex++
//	rf.matchIndex[rf.me] = len(rf.log) - 1
//	rf.nextIndex[rf.me] = len(rf.log)
//
//	/* did some research and here we use median */
//	// sort matchIndex
//	//copyMatchIndex := make([]int, len(rf.peers))
//	//copy(copyMatchIndex, rf.matchIndex)
//	//copyMatchIndex[rf.me] = len(rf.log)
//	//sort.Ints(copyMatchIndex)
//	//N := copyMatchIndex[len(rf.peers)/2]
//	//if N > rf.commitIndex && rf.log[N-1].Term == rf.currentTerm {
//	//	rf.commitIndex = N
//	//}
//
//	DPrintf("\n")
//
//	DPrintf("leader %d log contents: %v", rf.me, rf.log)
//	DPrintf("leader %d commitIndex: %d", rf.me, rf.commitIndex)
//	DPrintf("leader %d matchIndex: %v", rf.me, rf.matchIndex)
//	DPrintf("leader %d nextIndex: %v", rf.me, rf.nextIndex)
//
//	DPrintf("\n")
//
//	rf.sendApplyMsg()
//
//}

// sendApplyMsg sends an ApplyMsg to the applyCh channel
func (rf *Raft) sendApplyMsg() {
	// already locked outside

	// check if the commitIndex has been updated
	for rf.commitIndex > rf.lastApplied { // rf.log[lastApplied] has been applied. last applied is inited to 0

		rf.lastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		DPrintf("Server %d applying, commitIndex is %d, lastApplied is %d, command is %v", rf.me, rf.commitIndex, rf.lastApplied, rf.log[rf.lastApplied].Command)

		//rf.mu2.Lock()
		rf.applyCh <- msg
		//rf.mu2.Unlock()
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
// *committed*: An entry is considered committed if it is safe for that entry to be applied to state machines.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).

	rf.mu.Lock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}

	// is leader, start agreement
	DPrintf("Starting command %v for server %d", command, rf.me)
	DPrintf("Log length for server %d is %d", rf.me, len(rf.log))

	rf.log = append(rf.log, LogEntry{Term: term, Command: command}) // appends the command to its own log
	rf.replicatedCount = 1                                          // the leader has already replicated the entry to itself
	index = len(rf.log)

	rf.mu.Unlock()

	rf.syncLog()

	// replicate the entry
	//rf.sendReplicateEntries() // this will return when the entry has been replicated to a majority of servers

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

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		switch rf.state {
		case FOLLOWER:
			rf.detectElectTimeout()
		case CANDIDATE:
			rf.startElection()
		case LEADER:
			rf.syncLog()
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 100 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// detectElectTimeout detects if an election should be started. run by followers.
// protected by rf.mu
func (rf *Raft) detectElectTimeout() {
	if rf.state != FOLLOWER {
		DPrintf("detectElectTimeout called by non-follower")
		return
	}
	electionTimeout := ELECTIONTIMEOUTBASE + rand.Intn(150)
	lastHeartbeat := rf.lastHeartbeat
	if time.Since(lastHeartbeat) > time.Duration(electionTimeout)*time.Millisecond {
		rf.state = CANDIDATE
	}
}

// startElection checks if an election should be started and starts it if necessary; it
// is triggered when a follower hasn't received a heartbeat for a while.
// protected by rf.mu
func (rf *Raft) startElection() {
	if rf.state != CANDIDATE {
		DPrintf("startElection called by non-candidate")
		return
	}

	DPrintf("Raft server %d starting election for term %d", rf.me, rf.currentTerm)
	// randomized election timeout
	// time.Sleep(time.Duration(rand.Intn(300)) * time.Millisecond)

	// curTerm := rf.currentTerm
	rf.currentTerm++
	/*DO WE INCREMENT THE TERM RIGHT NOW?? OR WAIT TILL CONFIRMED TO BE THE LEADER*/
	// vote for self
	curTerm := rf.currentTerm
	rf.votedFor = rf.me
	rf.numberVotes = 1

	// send RequestVote RPCs to all other servers

	// create a wait group to wait for all RPCs to return
	var wg sync.WaitGroup

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		wg.Add(1)

		go func(server int) {
			defer wg.Done()
			args := RequestVoteArgs{
				Term:         rf.currentTerm, // curTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			reply := RequestVoteReply{}
			success := rf.sendRequestVote(server, &args, &reply)

			if success {
				if reply.CurrentTerm > curTerm {
					latestTerm := int(math.Max(float64(reply.CurrentTerm), float64(rf.currentTerm)))
					makeFollower(rf, latestTerm)
					return
				} else if reply.VoteGranted {
					rf.mu2.Lock()
					rf.numberVotes++
					rf.mu2.Unlock()
				}
			}
		}(i)
	}

	wg.Wait()

	// print vote count
	DPrintf("Raft server %d received %d votes for term %d", rf.me, rf.numberVotes, rf.currentTerm)
	if rf.numberVotes > len(rf.peers)/2 && rf.state == CANDIDATE { // make sure a heartbeat hasn't been received
		rf.state = 2 // leader

		// initialize nextIndex and matchIndex
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.log) // leader last log index + 1
			rf.matchIndex[i] = 0
		}

		DPrintf("Raft server %d is the leader for term %d", rf.me, rf.currentTerm)
	} else {
		// already taken care of in the go routine
		DPrintf("Raft server %d lost the election for term %d", rf.me, rf.currentTerm)
	}

}

// syncLog syncs the log with the other servers;
// it acts both as a heartbeat and a log replication.
// This function takes place periodically in the leader in ticker.
// protected by rf.mu
func (rf *Raft) syncLog() {
	// make sure the leader is still the leader
	if rf.state != LEADER {
		DPrintf("syncLog called by non-leader")
		return
	}

	halfMutex := sync.Mutex{}
	halfReached := false

	// send entries to other servers
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		DPrintf("Leader %d syncing log with server %d", rf.me, i)
		go func(server int) {
			//defer rf.mu2.Unlock()
			for {
				// retry until success
				// check if the leader is still the leader
				rf.mu2.Lock()

				if rf.state != LEADER {
					rf.mu2.Unlock()
					return
				}

				entriesToSend := []LogEntry{}
				if len(rf.log) > 1 { // in case of out of bounds
					entriesToSend = append(entriesToSend, rf.log[rf.nextIndex[server]:]...) // on init, next is 1, so we send log[1:]
				}
				DPrintf("Log now: %v", rf.log)
				DPrintf("NextIndex for server %d: %d", server, rf.nextIndex[server])
				DPrintf("Entries to send to server %d: %v", server, entriesToSend)

				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[server] - 1,            // on init, nextIndex[any] should be 1, since log[0] is a dummy entry
					PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term, // on init, log[0].Term is 0
					Entries:      entriesToSend,
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{}

				// send the entries
				ok := rf.sendAppendEntries(server, &args, &reply)

				if !ok {
					// retry. stay in the loop
				} else if reply.Term > rf.currentTerm || rf.currentTerm != args.Term {
					// not leader anymore

					makeFollower(rf, reply.Term)
					rf.mu2.Unlock()
					return
				} else if reply.Success {
					// successful, update nextIndex and matchIndex

					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1

					// update commit index;
					// find the index N such that a majority of matchIndex[i] >= N
					tempArr := make([]int, len(rf.peers))
					copy(tempArr, rf.matchIndex)
					sort.Ints(tempArr)
					N := tempArr[len(rf.peers)/2]
					if N > rf.commitIndex && rf.log[N].Term == rf.currentTerm {
						rf.commitIndex = N
						halfMutex.Lock()
						halfReached = true
						halfMutex.Unlock()
					}

					// apply the log entries to the state machine when the majority of servers have replicated the entry. could be done in a separate goroutine
					rf.sendApplyMsg()

					rf.mu2.Unlock()
					return
				} else {
					// unsuccessful, decrement nextIndex and retry
					rf.nextIndex[server]--
					// stay in the loop
				}

				rf.mu2.Unlock()
				time.Sleep(10 * time.Millisecond)
			}

		}(i)
	}

	// wait for the majority of servers to replicate the entry
	// acquire the lock before checking
	halfMutex.Lock()
	for !halfReached {
		halfMutex.Unlock()
		time.Sleep(50 * time.Millisecond)
		halfMutex.Lock()
	}
	halfMutex.Unlock()

}

// send AppendEntries RPCs to all other servers
func (rf *Raft) sendHeartbeats() {

	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	// already locked in ticker

	//DPrintf("Raft server %d sending heartbeats for term %d", rf.me, rf.currentTerm)

	beforeCurrentTerm := rf.currentTerm

	wg := sync.WaitGroup{}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(server int) {
			defer wg.Done()
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: len(rf.log) - 1,
				PrevLogTerm:  rf.log[len(rf.log)-1].Term,
				Entries:      nil,
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			success := rf.sendAppendEntries(server, &args, &reply)

			// check if the leader term is still valid
			rf.mu2.Lock()
			// rf.lastestTerm = rf.lastestTerm.lastestTerm && (reply.Term <= rf.currentTerm)
			if success && reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
			}
			rf.mu2.Unlock()
		}(i)
	}

	wg.Wait()
	// check if the leader term is still valid
	if (rf.currentTerm > beforeCurrentTerm) || rf.state != 2 {
		DPrintf("Raft server %d is no longer the leader for term %d", rf.me, beforeCurrentTerm)
		DPrintf("Because the current term is %d and the state is %d", rf.currentTerm, rf.state)
		rf.state = 0 // follower
		rf.votedFor = -1
	}
}

// makeFollower makes the server a follower. this func DOES NOT acquire the lock.
func makeFollower(rf *Raft, serverTerm int) {
	rf.state = FOLLOWER
	rf.currentTerm = serverTerm

	// if leader, votedfor is meaningless
	// if candidate, votedfor should be itself
	// if follower, votedfor should be reset to -1
	rf.votedFor = -1

	rf.numberVotes = 0

	rf.replicatedCount = 0

	rf.lastHeartbeat = time.Now()

	// re-initialized after election
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// log
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

	// print info
	DPrintf("Raft server %d created", rf.me)
	DPrintf("Peers: %v", rf.peers)

	// Your initialization code here (3A, 3B, 3C).
	makeFollower(rf, 0)
	rf.log = make([]LogEntry, 1) // log[0] is a dummy entry

	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
