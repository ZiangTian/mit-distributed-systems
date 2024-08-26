package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	nReduce int
	// two maps: one for filenames and one for the status of the file
	// filenames map[int]string
	// doneA map[int]bool
	filenames []string
	doneA     []bool
	assignedA []bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// assign a task to worker
func (c *Coordinator) AssignTask(args *ExampleArgs, reply *AssignmemtReply) error {
	for i := 0; i < len(c.filenames) && i < len(c.assignedA); i++ {
		if !c.assignedA[i] && !c.doneA[i] {
			c.assignedA[i] = true
			reply.filename = c.filenames[i]
			reply.taskId = i
			return nil
		}
	}
	return nil
}

func (c *Coordinator) ReceiveTask(args *TaskArgs, reply *ExampleReply) error {
	// do something
	return nil

}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Init coordinator
	c.filenames = files
	c.doneA = make([]bool, len(files))
	c.assignedA = make([]bool, len(files))
	c.nReduce = nReduce

	c.server()
	return &c
}
