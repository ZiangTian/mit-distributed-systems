package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type AssignmemtReply struct {
	taskType  bool // true for map, false for reduce
	filenames []string
	mapTaskId int
	nReduce   int
}

type TaskArgs struct {
	taskType bool

	// if is a map task, would need an array of filenames
	// if is a reduce task, would need an array of intermediate file names, actually indicated by the mr-X-Y
	filenames []string
}

// type MapResult struct { // we dont need a map struct for the results because we just output the file as mr-X-Y
// 	taskId int
// 	kva   []KeyValue
// }

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
