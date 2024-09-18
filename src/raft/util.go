package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Debugging
const DFlag = true

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

func DPrintf(format string, a ...interface{}) {
	if DFlag {
		log.Printf(format, a...)
	}
}

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

// constants

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
)

// helper functions

// makeFollower makes a server a follower with the given term.
// it does not matter what the state the server is in.
// if the server is already a follower, it's just reinforced.
// this function NOT acquire the lock.
func makeFollower(rf *Raft, lastestTerm int, resetTimer bool) {
	rf.state = FOLLOWER
	rf.currentTerm = lastestTerm

	// invalidate leader data
	rf.votedFor = -1
	rf.numberVotes = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// reset timer
	if resetTimer {
		rf.lastHeartbeat = time.Now()
	}
}

// makeLeader makes a server a leader.
// it initializes the leader states, and invalidates the candidate states.
func makeLeader(rf *Raft) {
	rf.state = LEADER
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	rf.votedFor = -1
	rf.numberVotes = 0
}

// isMoreUpToDate returns true if the server's log is more up-to-date than the candidate's.
func isMoreUpToDate(lastLogTerm, lastLogIndex, candidateLastLogTerm, candidateLastLogIndex int) bool {
	// fmt.Printf("voter lastLogTerm: %v, voter lastLogIndex: %v\n", lastLogTerm, lastLogIndex)
	// fmt.Printf("candidate LastLogTerm: %v, candidate LastLogIndex: %v\n", candidateLastLogTerm, candidateLastLogIndex)
	if lastLogTerm > candidateLastLogTerm {
		return true
	} else if lastLogTerm == candidateLastLogTerm {
		return lastLogIndex > candidateLastLogIndex
	}
	return false
}

// coherenceCheck checks if the log is coherent with the given prevLogIndex and prevLogTerm.
func coherencyCheck(prevLogIndex int, prevLogTerm int, log []LogEntry) bool {
	if prevLogIndex > len(log)-1 {
		return false
	}
	return log[prevLogIndex].Term == prevLogTerm
}

// bigger returns the bigger of the two integers.
func bigger(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func smaller(a, b int) int {
	if a < b {
		return a
	}
	return b
}
