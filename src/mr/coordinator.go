package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type taskState struct {
	states    []int // 0: idle, 1: in progress, 2: done
	startTime []time.Time
	mu        sync.Mutex
}

type Coordinator struct {
	// Your definitions here.
	nReduce         int
	filenames       []string // all files to be mapped, each as a map task
	mapTaskState    taskState
	reduceTaskState taskState
}

func (c *Coordinator) isAllMapTasksDone() bool {
	allMapTasksDone := true
	c.mapTaskState.mu.Lock()
	for _, state := range c.mapTaskState.states {
		if state != 2 {
			allMapTasksDone = false
			break
		}
	}
	c.mapTaskState.mu.Unlock()
	return allMapTasksDone
}

func (c *Coordinator) isAllReduceTasksDone() bool {
	allReduceTasksDone := true
	c.reduceTaskState.mu.Lock()
	for _, state := range c.reduceTaskState.states {
		if state != 2 {
			allReduceTasksDone = false
			break
		}
	}
	c.reduceTaskState.mu.Unlock()
	return allReduceTasksDone
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

	allMapTasksDone := c.isAllMapTasksDone()
	// print("All map tasks done: ", allMapTasksDone, "\n")

	if !allMapTasksDone { // try to assign map tasks
		// find the first idle map task
		// fmt.Println("Assigning map tasks")
		found := false
		for i, state := range c.mapTaskState.states {
			if state == 0 {
				reply.TaskType = 0
				reply.Filename = c.filenames[i]
				reply.FileDir = ""
				reply.MapTaskId = i
				reply.NReduce = c.nReduce
				c.mapTaskState.states[i] = 1
				c.mapTaskState.startTime[i] = time.Now()

				reply.Y = -1

				// fmt.Printf("Assigned map task %d, filename %s \n", reply.MapTaskId, reply.Filename)

				found = true
				break
			}
		}
		if !found { // all map tasks are at least in progress
			reply.TaskType = 2
		}
	} else { // assign reduce tasks
		// find the first idle reduce task
		// fmt.Println("Assigning reduce tasks")

		if c.isAllReduceTasksDone() {
			// fmt.Println("All reduce tasks are done")
			reply.TaskType = 3
			return nil
		}

		found := false
		for i, state := range c.reduceTaskState.states {
			// fmt.Println(i, state)
			if state == 0 {
				reply.TaskType = 1
				reply.FileDir = "./"
				reply.Y = i
				reply.NReduce = c.nReduce
				c.reduceTaskState.states[i] = 1
				c.reduceTaskState.startTime[i] = time.Now()

				reply.MapTaskId = -1
				// fmt.Printf("Assigned reply task %d, filename %s \n", reply.Y, reply.Filename)

				found = true
				break
			}
		}
		if !found {
			reply.TaskType = 2
			// fmt.Println("reduce tasks not found")
		} else {
			// fmt.Printf("Assigning reduce task %d\n", reply.Y)
		}

	}
	return nil
}

func (c *Coordinator) RegisterDone(args *TaskArgs, reply *ExampleReply) error {
	if args.TaskType { // map task
		// fmt.Println("Mark map tasks as done")
		c.mapTaskState.mu.Lock()
		c.mapTaskState.states[args.TaskId] = 2
		c.mapTaskState.mu.Unlock()
	} else { // reduce task
		// fmt.Println("GUARD!!! trying to check reduce tasks")
		c.reduceTaskState.mu.Lock()
		c.reduceTaskState.states[args.TaskId] = 2
		c.reduceTaskState.mu.Unlock()
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
	ret := c.isAllReduceTasksDone() // check all reduce tasks

	if ret && !c.isAllMapTasksDone() {
		panic("All reduce tasks are done but not all map tasks are done")
	}
	// assert map tasks are all finished if ret is true

	return ret
}

func (s *taskState) checkForFailure() {
	// check for failure
	for {
		time.Sleep(5 * time.Second)
		s.mu.Lock()
		for i, state := range s.states {
			if state == 1 && time.Since(s.startTime[i]) > 10*time.Second {
				s.states[i] = 0
				fmt.Printf("Task %d is failed\n", i)
			}
		}
		s.mu.Unlock()
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Init coordinator
	c.filenames = files
	// print filenames
	// fmt.Println("Files to be mapped: ", files)
	c.nReduce = nReduce
	c.mapTaskState.states = make([]int, len(files))
	c.mapTaskState.startTime = make([]time.Time, len(files)) // each element is time.Time{}
	c.reduceTaskState.states = make([]int, nReduce)
	c.reduceTaskState.startTime = make([]time.Time, nReduce)
	// fmt.Println("Coordinator is created")
	// fmt.Println("Number of map tasks: ", len(files))
	go c.mapTaskState.checkForFailure()
	c.server()

	// stall to check for beginning of reduce tasks
	for {
		if c.isAllMapTasksDone() {
			break
		}
		time.Sleep(1 * time.Second)
	}
	go c.reduceTaskState.checkForFailure()
	return &c
}
