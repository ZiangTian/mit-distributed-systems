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
	//	"bytes"
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

// A Go object implementing a single Raft *peer*.
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
	nextIndex  []int
	matchIndex []int

	lastHeartbeat time.Time

	numberVotes int
	mu2         sync.Mutex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == 2
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).

	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).

	CurrentTerm int  // current term, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.CurrentTerm = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = 0 // follower

		// since this is a new term, reset votedFor
		rf.votedFor = -1
	}

	rf.lastHeartbeat = time.Now()

	// voting
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && args.LastLogTerm >= rf.log[len(rf.log)-1].Term {
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
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = 0 // follower

		// since this is a new term, reset votedFor
		rf.votedFor = -1
	}

	// recognize the leader

	rf.lastHeartbeat = time.Now()

	// check if is heartbeat
	if len(args.Entries) == 0 {
		reply.Success = true
		rf.state = 0 // follower
		return
	}

	// verify entries
	locatedEntry := rf.log[args.PrevLogIndex]
	if locatedEntry.Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	// check for conflicting entries

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
		DPrintf("Timeout: Raft server %d couldn't send RequestVote to server %d", rf.me, server)
		return false
	}
}

// the service using Raft (e.g. a k/v server) wants to start
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
	defer rf.mu.Unlock()

	index := rf.me
	term := rf.currentTerm
	isLeader := rf.state == 2

	if !isLeader {
		return index, term, isLeader
	}

	// is leader, start agreement
	rf.log = append(rf.log, LogEntry{Term: term, Command: command})

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
		state := rf.state

		if state != 2 { // not leader
			rf.startElection() // check and start election if necessary
		} else { // leader
			rf.sendHeartbeats() // send heartbeats
		}

		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// startElection checks if an election should be started and starts it if necessary; it
// is triggered when a follower hasn't received a heartbeat for a while
func (rf *Raft) startElection() {

	electionTimeout := 550 + rand.Intn(150)
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	// already locked in ticker

	lastHeartbeat := rf.lastHeartbeat

	if time.Since(lastHeartbeat) > time.Duration(electionTimeout)*time.Millisecond {
		DPrintf("Raft server %d starting election for term %d", rf.me, rf.currentTerm)
		// randomized election timeout
		// time.Sleep(time.Duration(rand.Intn(300)) * time.Millisecond)

		// curTerm := rf.currentTerm

		rf.currentTerm++
		/*DO WE INCREMENT THE TERM RIGHT NOW?? OR WAIT TILL CONFIRMED TO BE THE LEADER*/
		rf.state = 1 // candidate
		// vote for self
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

				// check if the election was successful
				if success && reply.VoteGranted {
					// increment vote count
					rf.mu2.Lock()
					rf.numberVotes++
					rf.mu2.Unlock()
				}
			}(i)
		}

		wg.Wait()

		// print vote count
		DPrintf("Raft server %d received %d votes for term %d", rf.me, rf.numberVotes, rf.currentTerm)
		if rf.numberVotes > len(rf.peers)/2 && rf.state == 1 { // make sure a heartbeat hasn't been received
			rf.state = 2 // leader
			DPrintf("Raft server %d is the leader for term %d", rf.me, rf.currentTerm)
		} else {
			DPrintf("Raft server %d lost the election for term %d", rf.me, rf.currentTerm)
			rf.votedFor = -1
			rf.state = 0 // follower
			// reset vote count
			rf.numberVotes = 0

			// decrement term
			rf.currentTerm--
		}
	}
}

// send AppendEntries RPCs to all other servers
func (rf *Raft) sendHeartbeats() {

	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	// already locked in ticker

	DPrintf("Raft server %d sending heartbeats for term %d", rf.me, rf.currentTerm)

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

	// print info
	DPrintf("Raft server %d created", rf.me)
	DPrintf("Peers: %v", rf.peers)

	// Your initialization code here (3A, 3B, 3C).
	rf.state = 0 // follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1) // log[0] is a dummy entry

	// idk what these are
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.lastHeartbeat = time.Now()
	rf.numberVotes = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
