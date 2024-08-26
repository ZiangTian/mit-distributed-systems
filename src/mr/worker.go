package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// worker takes two arguments: mapf and reducef

	// Your worker implementation here.
	success, AssignmemtReply := Call4Task()
	if !success {
		return
	}
	file, err := os.Open(AssignmemtReply.filename)
	if err != nil {
		log.Fatalf("cannot open %v", AssignmemtReply.filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", AssignmemtReply.filename)
	}
	file.Close()
	kva := mapf(AssignmemtReply.filename, string(content))

	// send the result to the coordinator

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// Call for tasks from coordinator
func Call4Task() (bool, AssignmemtReply) {
	args := ExampleArgs{}
	reply := AssignmemtReply{}

	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		return true, reply
	} else {
		// fmt.Printf("call failed!\n")
		return false, reply
	}
}

func SubmitTask(kva []KeyValue, taskId int) bool {
	args := TaskArgs{}
	args.kva = kva
	args.taskId = taskId
	reply := ExampleReply{}

	ok := call("Coordinator.ReceiveTask", &args, &reply)
	if ok {
		// fmt.Printf("reply.Y %v\n", reply.Y)
		return true
	} else {
		// fmt.Printf("call failed!\n")
		return false
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
