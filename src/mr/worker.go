package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) } // a is the slice of KeyValue pairs
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	// CallExample()
	// periodically ask for tasks from coordinator
	for {
		success, AssignmemtReply := Call4Task()
		if !success {
			break
		}
		fmt.Printf("got task %v\n", AssignmemtReply)
		if AssignmemtReply.taskType == 0 { // map
			// read from file
			fmt.Printf("got file %s \n", AssignmemtReply.filename)
			filename := AssignmemtReply.fileDir + AssignmemtReply.filename
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

			// Create the intermediate file
			intermediateFile, err := os.Create(intermediateFileName)
			if err != nil {
				log.Fatalf("cannot create %v", intermediateFileName)
			}
			// for _, kv := range kva {
			// 	fmt.Fprintf(intermediateFile, "%v %v\n", kv.Key, kv.Value)
			// }
			// write key-value pairs in json format using encoding/json
			enc := json.NewEncoder(intermediateFile)
			for _, kv := range kva {
				if err := enc.Encode(&kv); err != nil {
					log.Fatalf("cannot encode %v", kv)
					break
				}
			}
			intermediateFile.Close()

			// notify coordinator that the task is done
			NotifyDone(false, AssignmemtReply.mapTaskId) // false for map

		} else if AssignmemtReply.taskType == 1 { // reduce task
			// read from intermediate files
			intermediate := []KeyValue{}
			y := AssignmemtReply.Y
			// go to the file dir and read all the file names of mr-*-Y
			// get the path: AssignmemtReply.fileDir+"/mr-*-Y"
			regexFileName := AssignmemtReply.fileDir + fmt.Sprintf("mr-*-%v", y)
			matches, err := filepath.Glob(regexFileName)
			if err != nil {
				log.Fatalf("cannot read %v", regexFileName)
			}
			for _, filename := range matches {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				// read key-value pairs in json format using encoding/json
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}
			// sort the intermediate key-value pairs
			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%v", y)
			ofile, _ := os.Create(oname)

			// call Reduce on each distinct key in intermediate[],
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			ofile.Close()

			// notify coordinator that the task is done
			NotifyDone(true, AssignmemtReply.Y) // true for reduce
		} else { // cannot get task now
			// sleep for 0.1 second
			time.Sleep(2000 * time.Millisecond)
		}
		time.Sleep(1000 * time.Millisecond)

	}
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
		fmt.Printf("got task %s\n", reply.filename)
		return true, reply
	} else {
		// fmt.Printf("call failed!\n")
		return false, reply
	}
}

func NotifyDone(taskType bool, taskId int) bool {
	// TODO
	args := TaskArgs{}
	reply := ExampleReply{}

	args.taskType = taskType
	args.taskId = taskId

	ok := call("Coordinator.registerDone", &args, &reply)
	if ok {
		return true
	} else {
		fmt.Printf("call failed!\n")
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
