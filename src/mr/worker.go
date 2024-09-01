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

func saveIntermediateFile(kva []KeyValue, nReduce int, taskId int) {
	var fps []*os.File
	for i := 0; i < nReduce; i++ {
		temp, _ := os.CreateTemp("", "mr-tmp-")
		fps = append(fps, temp)
	}
	// scatter the intermediate key-value pairs to the reduce tasks
	// with the same key going to the same reduce task

	encoders := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		encoders[i] = json.NewEncoder(fps[i])
	}
	for _, keyValue := range kva {
		encoder := encoders[ihash(keyValue.Key)%nReduce]
		if err := encoder.Encode(&keyValue); err != nil {
			log.Fatalf("cannot encode %v", keyValue)
		}
	}

	for index, fp := range fps {
		_ = fp.Close()
		newFileName := fmt.Sprintf("mr-%v-%v", taskId, index)
		if err := os.Rename(fp.Name(), fmt.Sprintf("mr-%v-%v", taskId, index)); err != nil {
			log.Fatalf("cannot rename %s to %s", fp.Name(), newFileName)
		}
	}

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
		// fmt.Printf("got task %v\n", AssignmemtReply)
		if AssignmemtReply.TaskType == 0 { // map
			// read from file
			// fmt.Printf("got file %s \n", AssignmemtReply.Filename)
			filename := AssignmemtReply.FileDir + AssignmemtReply.Filename
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

			saveIntermediateFile(kva, AssignmemtReply.NReduce, AssignmemtReply.MapTaskId)

			NotifyDone(true, AssignmemtReply.MapTaskId) // false for map

		} else if AssignmemtReply.TaskType == 1 { // reduce task
			// read from intermediate files
			intermediate := []KeyValue{}
			y := AssignmemtReply.Y
			// go to the file dir and read all the file names of mr-*-Y
			// get the path: AssignmemtReply.fileDir+"/mr-*-Y"
			regexFileName := AssignmemtReply.FileDir + fmt.Sprintf("mr-*-%v", y)
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
			NotifyDone(false, AssignmemtReply.Y) // true for reduce
		} else if AssignmemtReply.TaskType == 2 { // cannot get task now
			// sleep for 0.1 second
			time.Sleep(100 * time.Millisecond)
		} else {
			// please exit
			break
		}
		time.Sleep(100 * time.Millisecond)

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
		// fmt.Printf("got task %s\n", reply.Filename)
		return true, reply
	} else {
		// fmt.Printf("call failed!\n")
		return false, reply
	}
}

func NotifyDone(TaskType bool, TaskId int) bool {
	// TODO
	args := TaskArgs{}
	reply := ExampleReply{}

	args.TaskType = TaskType
	args.TaskId = TaskId

	ok := call("Coordinator.RegisterDone", &args, &reply)
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
