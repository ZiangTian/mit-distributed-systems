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

	// should change it to periodically call for tasks from coordinator?

	// Your worker implementation here.
	success, AssignmemtReply := Call4Task()
	if !success {
		return
	}

	if AssignmemtReply.taskType { // map
		for _, filename := range AssignmemtReply.filenames {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))

			// output to intermediate file
			y := ihash(filename) % AssignmemtReply.nReduce

			// lock the file TODO
			intermediateFileName := fmt.Sprintf("mr-%v-%v", AssignmemtReply.mapTaskId, y)
			intermediateFile, err := os.Create(intermediateFileName)
			if err != nil {
				log.Fatalf("cannot create %v", intermediateFileName)
			}
			// for _, kv := range kva {
			// 	fmt.Fprintf(intermediateFile, "%v %v\n", kv.Key, kv.Value)
			// }
			// write key-value pairs in json format using encoding/json
			json.NewEncoder(intermediateFile).Encode(kva)
			intermediateFile.Close()

	} else { // reduce task


	}

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
