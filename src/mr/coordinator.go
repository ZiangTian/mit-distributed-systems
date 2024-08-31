package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	nReduce         int
	filenames       []string // all files to be mapped, each as a map task
	mapTaskState    []int    // 0: idle, 1: in progress, 2: completed
	reduceTaskState []int    // 0: idle, 1: in progress, 2: completed
}

func bitCheck(states []int) (int, int) {
	// use bit manipulation to check if all tasks are completed
	bitAnd := 1
	bitOr := 0
	for _, state := range states {
		bitAnd &= state
		bitOr |= state
	}
	// if is all 01, then bit and is 01, bit or is 01
	// if is all 10, then bit and is 10, bit or is 10
	// if is all 00, then bit and is 00, bit or is 00
	// if is 01 and 10, then bit and is 00, bit or is 11
	// if is 01 and 00, then bit and is 00, bit or is 01
	// if is 01 and 10 and 00, then bit and is 00, bit or is 11
	// if is 00 and 10, then bit and is 00, bit or is 10
	return bitAnd, bitOr
}

func (c *Coordinator) isAllTasksDone(taskType bool) bool {
	if taskType { // true for reduce task
		bitAnd, bitOr := bitCheck(c.reduceTaskState)
		return (bitAnd == bitOr) && (bitAnd == 2)
	} else {
		bitAnd, bitOr := bitCheck(c.mapTaskState)
		return (bitAnd == bitOr) && (bitAnd == 2)
	}
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
	// check if all map tasks are done
	// if all map tasks are done, assign reduce tasks
	// if all reduce tasks are done, return false
	fmt.Print("Assigning Task\n")

	allMapTasksDone := true
	for _, state := range c.mapTaskState {
		if state != 1 {
			allMapTasksDone = false
			break
		}
	}

	if !allMapTasksDone { // try to assign map tasks
		// find the first idle map task
		found := false
		for i, state := range c.mapTaskState {
			if state == 0 {
				reply.taskType = 0
				reply.filename = c.filenames[i]
				fmt.Printf("Assigned map task %d, filename %s \n", reply.mapTaskId, reply.filename)
				reply.fileDir = "./"
				reply.mapTaskId = i
				reply.nReduce = c.nReduce
				c.mapTaskState[i] = 1

				reply.Y = -1

				found = true
				break
			}
		}
		if !found {
			reply.taskType = 2
		}

	} else { // assign reduce tasks
		// find the first idle reduce task
		found := false
		for i, state := range c.reduceTaskState {
			if state == 0 {
				reply.taskType = 1
				reply.fileDir = "./"
				reply.Y = i
				reply.nReduce = c.nReduce
				c.reduceTaskState[i] = 1

				found = true
				break
			}
		}
		if !found {
			reply.taskType = 2
		}
		fmt.Printf("Assigning reduce task %d\n", reply.Y)
	}
	return nil
}

func (c *Coordinator) registerDone(args *TaskArgs, reply *ExampleReply) error {
	if args.taskType { // map task
		c.mapTaskState[args.taskId] = 2
	} else { // reduce task
		c.reduceTaskState[args.taskId] = 2
	}
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

	// Your code here.
	ret := c.isAllTasksDone(true) // check all reduce tasks

	if ret && !c.isAllTasksDone(false) {
		panic("All reduce tasks are done but not all map tasks are done")
	}
	// assert map tasks are all finished if ret is true

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Init coordinator
	c.filenames = files
	// print filenames
	fmt.Println("Files to be mapped: ", files)
	c.nReduce = nReduce
	c.mapTaskState = make([]int, len(files))
	c.reduceTaskState = make([]int, nReduce)
	fmt.Println("Coordinator is created")
	fmt.Println("Number of map tasks: ", len(files))
	c.server()
	return &c
}
