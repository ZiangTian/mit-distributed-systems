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
	taskType  int    // 0 for map, 1 for reduce, 2 for not assignable now
	filename  string // filenames for map tasks
	Y         int    // reduce task id
	fileDir   string // reduce task directory
	mapTaskId int    // map task id
	nReduce   int    // number of reduce tasks
}

type TaskArgs struct {
	taskType bool
	taskId   int // for map task, it is the index of the file in filenames;
	// for reduce task, it is Y
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
