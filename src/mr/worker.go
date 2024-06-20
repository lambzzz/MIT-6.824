package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func coorisalive() bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
	}
	defer c.Close()
	return true
}

func execmaptask(mapf func(string, string) []KeyValue, reply *Replytype, args *Argstype) {
	filename := reply.Filename
	filenamehash := ihash(filename)
	nReduce := reply.NReduce
	intermediate := make([][]KeyValue, nReduce)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	for _, kv := range kva {
		reduceidx := ihash(kv.Key) % nReduce
		intermediate[reduceidx] = append(intermediate[reduceidx], kv)
	}

	for i := 0; i < nReduce; i++ {
		jsonData, err := json.Marshal(intermediate[i])
		if err != nil {
			log.Fatalf("JSON marshaling failed: %s", err)
		}
		intermediateFile, err := os.Create(fmt.Sprintf("mr-%d-%d", i, filenamehash))
		if err != nil {
			log.Fatalf("cannot create %v", fmt.Sprintf("mr-%d-%d", i, filenamehash))
		}
		intermediateFile.Write(jsonData)
		intermediateFile.Close()
	}

	args.Filename = filename
	*reply = Replytype{}
	ok := call("Coordinator.Submittask", &args, &reply)
	if ok {

	} else {
		fmt.Printf("call failed!\n")
	}
	args.Filename = ""
}

func execreducetask(reducef func(string, []string) string, reply *Replytype, args *Argstype) {
	filename := "mr-" + reply.Filename + "-"
	var reduceout, t []KeyValue
	reducekv := make(map[string][]string)
	files, err := os.ReadDir(".")
	if err != nil {
		log.Fatalf("Failed to read directory: %s", err)
	}
	for _, file := range files {
		if strings.HasPrefix(file.Name(), filename) {
			jsonData, err := os.ReadFile(file.Name())
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			err = json.Unmarshal(jsonData, &t)
			if err != nil {
				log.Fatalf("JSON unmarshaling failed: %s", err)
			}
			for _, kv := range t {
				reducekv[kv.Key] = append(reducekv[kv.Key], kv.Value)
			}
		}
	}
	for k, v := range reducekv {
		output := reducef(k, v)
		reduceout = append(reduceout, KeyValue{k, output})
	}
	sort.Sort(ByKey(reduceout))
	outname := "mr-out-" + reply.Filename
	outfile, _ := os.Create(outname)

	for i := 0; i < len(reduceout); i++ {
		fmt.Fprintf(outfile, "%v %v\n", reduceout[i].Key, reduceout[i].Value)
	}
	outfile.Close()

	args.Filename = reply.Filename
	*reply = Replytype{}
	ok := call("Coordinator.Submittask", &args, &reply)
	if ok {

	} else {
		fmt.Printf("call failed!\n")
	}
	args.Filename = ""
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	if !coorisalive() {
		fmt.Printf("coordinator is not alive\n")
		return
	}
	args := Argstype{}
	reply := Replytype{}
	for {
		reply = Replytype{}
		ok := call("Coordinator.Applytask", &args, &reply)
		if ok {
			if reply.Tasktype == "none" {

			} else if reply.Tasktype == "map" {
				execmaptask(mapf, &reply, &args)

			} else if reply.Tasktype == "reduce" {
				execreducetask(reducef, &reply, &args)

			} else if reply.Tasktype == "done" {
				break
			} else {
				fmt.Printf("error tasktype\n")
			}
		} else {
			fmt.Printf("call failed!\n")
		}
	}

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
