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

	// A spare lock in case
	mu2 sync.Mutex

	// 3B: applyCh
	applyCh         chan ApplyMsg
	cond            *sync.Cond
	commitIDUpdated bool
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
	candidateIsMoreUpdated := !isMoreUpToDate(rf.log[len(rf.log)-1].Term, len(rf.log)-1, args.LastLogTerm, args.LastLogIndex)
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
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	// invoked by the leader to replicate log entries
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm

		rf.mu.Unlock()
		return
	}

	makeFollower(rf, args.Term, true)

	// // even if it's heartbeat, we still need to check the term

	// check for conflicting entries
	if !coherencyCheck(args.PrevLogIndex, args.PrevLogTerm, rf.log) {
		Debug(dTest, "PrevLogIndex: %d, PrevLogTerm: %d, log: %v", args.PrevLogIndex, args.PrevLogTerm, rf.log)
		Debug(dLog, "S%d: log is not coherent", rf.me)
		// delete all entries starting from PrevLogIndex
		reply.Success = false

		rf.log = rf.log[:args.PrevLogIndex]

		// do we append the entries?

		rf.commitIDUpdated = true
		rf.mu.Unlock()
		//rf.tryApplyMsg()

		rf.cond.Broadcast()
		return
	} else {
		Debug(dTest, "PrevLogIndex: %d, PrevLogTerm: %d, log: %v", args.PrevLogIndex, args.PrevLogTerm, rf.log)
		Debug(dLog, "S%d: log is coherent", rf.me)
		reply.Success = true

		// WARNING we cannot append the entry by truncating the log!!
		// the RPC can be outdated, so truncation would mean losing entries

		// append the entries
		// if the follower has all the entries the leader sent, the log cannot be truncated.
		// Any elements following the entries sent by the leader MUST be kept
		curLog := append([]LogEntry{}, rf.log...)
		// idLastNewEntry := len(curLog) - 1
		if args.PrevLogIndex+len(args.Entries) < len(curLog) {
			// check for conflicts
			for i := 0; i < len(args.Entries); i++ {
				if args.Entries[i].Term != curLog[args.PrevLogIndex+1+i].Term {
					rf.log = append(rf.log[:args.PrevLogIndex+1+i], args.Entries[i:]...)
					// idLastNewEntry = args.PrevLogIndex + i
					break
				}
			}
		} else {
			rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
		}

		// update the commitIndex
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = smaller(args.LeaderCommit, len(rf.log)-1)
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
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		Debug(dCommit, "S%d Applying command %v at index %d", rf.me, rf.log[rf.lastApplied].Command, rf.lastApplied)
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
		for !rf.commitIDUpdated {
			rf.cond.Wait()
		}

		// check if the commitIndex has been updated
		for rf.commitIndex > rf.lastApplied { // rf.log[lastApplied] has been applied. last applied is inited to 0

			rf.lastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			Debug(dCommit, "S%d Applying command %v at index %d", rf.me, rf.log[rf.lastApplied].Command, rf.lastApplied)
			//rf.mu2.Lock()
			rf.cond.L.Unlock()
			rf.applyCh <- msg
			//rf.mu2.Unlock()
			rf.cond.L.Lock()
		}
		rf.commitIDUpdated = false
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
		index = len(rf.log) // if it ever gets committed, it will be at the end of the log
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
	defer rf.mu.Unlock()

	if rf.state != FOLLOWER {
		return
	}

	electionTimeout := 250 + rand.Intn(150)

	lastHeartbeat := rf.lastHeartbeat

	if time.Since(lastHeartbeat) > time.Duration(electionTimeout)*time.Millisecond {
		Debug(dTimer, "S%d Follower, election timeout", rf.me)
		rf.state = CANDIDATE
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
			rf.startElection()
		case LEADER:
			//rf.sendHeartbeats() // this does not block.
			rf.syncLog()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 50 + (rand.Int63() % 300)
		ms := 100
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
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
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
						//DPrintf("Raft server %d is the leader for term %d", rf.me, rf.currentTerm)
						Debug(dVote, "S%d is the leader for term %d", rf.me, rf.currentTerm)
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) sendHeartbeats() {

	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}

	Debug(dTimer, "S%d Leader, sending heartbeats", rf.me)

	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			// prepare args to send to all the servers
			// PrevLogIndex: the log index previous to the one to be sent

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
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// rf.lastestTerm = rf.lastestTerm.lastestTerm && (reply.Term <= rf.currentTerm)
			if success {
				if reply.Term > rf.currentTerm {
					makeFollower(rf, reply.Term, false)
					Debug(dDrop, "S%d No longer leader", rf.me)
					return
				}
				if rf.currentTerm != args.Term || rf.state != LEADER {
					return
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
				if len(rf.log) > 1 {
					entriesToSend = append(entriesToSend, rf.log[rf.nextIndex[server]:]...) // on init, next is 1, so we send log[1:]
				}

				Debug(dLog, "S%d -> S%d, Sending entries: %v", rf.me, server, entriesToSend)
				Debug(dTest, "S%d -> S%d, S%d log now %v, S%d nextIndex %d", rf.me, server, rf.me, rf.log, server, rf.nextIndex[server])
				Debug(dLeader, "S%d -> S%d, Sending PLI: %d, PLT: %d", rf.me, server, rf.nextIndex[server]-1, rf.log[rf.nextIndex[server]-1].Term)

				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[server] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
					Entries:      entriesToSend,
					LeaderCommit: rf.commitIndex,
				}

				rf.mu.Unlock()

				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, &args, &reply)

				// check if the leader term is still valid
				rf.mu.Lock()
				// rf.lastestTerm = rf.lastestTerm.lastestTerm && (reply.Term <= rf.currentTerm)
				Debug(dError, "1")
				if ok {
					Debug(dError, "2")
					if reply.Term > rf.currentTerm {
						makeFollower(rf, reply.Term, false)
						Debug(dDrop, "S%d No longer leader", rf.me)
						rf.mu.Unlock()
						return
					}
					if rf.currentTerm != args.Term || rf.state != LEADER {
						Debug(dError, "3")
						rf.mu.Unlock()
						return
					}

					// if the entries sent are empty, it's a heartbeat, we don't need to check the reply
					if len(args.Entries) == 0 {
						Debug(dError, "4")

						rf.commitIDUpdated = true
						rf.mu.Unlock()

						rf.cond.Broadcast()
						//rf.tryApplyMsg()
						return
					}

					if reply.Success {
						// update nextIndex and matchIndex
						Debug(dLeader, "S%d -> S%d, Success, updating nextIndex and matchIndex", rf.me, server)

						rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
						rf.matchIndex[server] = rf.nextIndex[server] - 1

						// check if the leader can commit the entry
						tempArr := make([]int, len(rf.peers))
						copy(tempArr, rf.matchIndex)
						sort.Ints(tempArr)
						N := tempArr[len(rf.peers)/2]
						if N > rf.commitIndex && rf.log[N].Term == rf.currentTerm {
							Debug(dLeader, "S%d commitIndex updated to %d", rf.me, N)
							rf.commitIndex = N

							rf.commitIDUpdated = true
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
						//rf.nextIndex[server]--
						rf.nextIndex[server] = bigger(1, rf.nextIndex[server]-1)
					}
				} else {
					// retry
					Debug(dError, "5")
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
