package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Debugging
const DFlag = false

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

const debug = 1

func Debug(topic logTopic, format string, a ...interface{}) {
	if debug >= 1 {
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
func makeFollower(rf *Raft, lastestTerm int) {
	rf.state = FOLLOWER
	rf.currentTerm = lastestTerm

	// invalidate leader data
	rf.votedFor = -1
	rf.numberVotes = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// reset timer
	rf.lastHeartbeat = time.Now()
}

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

func isMoreUpToDate(lastLogTerm, lastLogIndex, candidateLastLogTerm, candidateLastLogIndex int) bool {
	if lastLogTerm > candidateLastLogTerm {
		return true
	} else if lastLogTerm == candidateLastLogTerm {
		return lastLogIndex > candidateLastLogIndex
	}
	return false
}

func coherencyCheck(prevLogIndex int, prevLogTerm int, log []LogEntry) bool {
	if prevLogIndex >= len(log)-1 {
		return false
	}
	return log[prevLogIndex].Term == prevLogTerm
}

func bigger(a, b int) int {
	if a > b {
		return a
	}
	return b
}
