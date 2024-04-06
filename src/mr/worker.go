package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// KeyValue
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker
// main/mrworker.go calls this function.
// 1. read the input file from stdin, call mapf for each line
// 2. request map/reduce/done
// 3. write output to stdout
// 4. have heartbeat with each other worker
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		task := Task{}
		if !GetTask(&task) {
			break // Exit the loop if getting a task fails
		}

		switch task.Type {
		case TaskTypeWaiting:
			time.Sleep(time.Second) // Wait before fetching the next task
		case TaskTypeRequestMap:
			DoMapAction(mapf, &task)
			TaskFinish(&task)
		case TaskTypeRequestReduce:
			DoReduceAction(reducef, &task)
			TaskFinish(&task)
		default:
			return
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// CallExample
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

// DoMapAction do map action
// 1. read input from stdin
// 2. do mapf()
// 3. output flies to stdout
// 4. send task finish to coordinator
func DoMapAction(mapf func(string, string) []KeyValue, task *Task) {
	var kvs []KeyValue
	kvsMap := make([][]KeyValue, task.NReduce)

	file, err := os.Open(task.ToBeOperatedFileName)
	if err != nil {
		log.Fatalf("could not open file:%v err:%v", task.ToBeOperatedFileName, err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("could not read file:%v err:%v", task.ToBeOperatedFileName, err)
	}
	if err := file.Close(); err != nil {
		log.Fatalf("could not close file:%v err:%v", task.ToBeOperatedFileName, err)
	}
	kva := mapf(task.ToBeOperatedFileName, string(content))
	kvs = append(kvs, kva...)

	for _, kv := range kvs {
		index := ihash(kv.Key) % task.NReduce
		kvsMap[index] = append(kvsMap[index], kv)
	}

	sort.Sort(ByKey(kvs))
	for i := 0; i < task.NReduce; i++ {
		outputFileName := "mr-" + strconv.Itoa(task.TaskID) + "-" + strconv.Itoa(i)
		outputFile, _ := os.Create(outputFileName)
		enc := json.NewEncoder(outputFile)
		for _, kv := range kvsMap[i] {
			enc.Encode(&kv)
		}
		outputFile.Close()
	}
}

// DoReduceAction do reduce action
// 1. read input from stdin
// 2. sort by key
// 3. do reducef()
// 4. output flies to stdout
// 5. send task finish to coordinator
func DoReduceAction(reducef func(string, []string) string, task *Task) {
	var kvs []KeyValue

	for i := 0; i < task.NReduce; i++ {
		filepath := task.ToBeOperatedFileName + strconv.Itoa(i) + "-" + strconv.Itoa(task.TaskID)
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		if err := file.Close(); err != nil {
			log.Fatalf("could not close file:%v err:%v", filepath, err)
		}
	}

	sort.Sort(ByKey(kvs))
	outputFileName := "mr-out-" + strconv.Itoa(task.TaskID)
	outputFile, err := os.Create(outputFileName)
	if err != nil {
		log.Fatalf("Failed to create output file '%s': %v", outputFileName, err)
	}
	defer outputFile.Close()

	start := 0
	for start < len(kvs) {
		end := start + 1
		// find all values with the same key
		for end < len(kvs) && kvs[end].Key == kvs[start].Key {
			end++
		}

		// according to the start and end indexes, collect all values
		values := make([]string, end-start)
		for idx := start; idx < end; idx++ {
			values[idx-start] = kvs[idx].Value
		}

		// apply reduce function and output result
		aggregatedOutput := reducef(kvs[start].Key, values)
		if _, err := fmt.Fprintf(outputFile, "%v %v\n", kvs[start].Key, aggregatedOutput); err != nil {
			log.Fatalf("Failed to write to output file: %v", err)
		}

		// move to the next different key
		start = end
	}
}

// TaskFinish task finish
func TaskFinish(task *Task) {
	res := ExampleReply{}
	// call task finish
	call("Coordinator.TaskDone", task, &res)
}

func GetTask(reply *Task) bool {
	args := ExampleArgs{}

	if reply.Type == TaskTypeWaiting {
		args.X = TaskTypeWaiting.Type2Int()
	}

	return call("Coordinator.DoTask", &ExampleArgs{}, reply)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
