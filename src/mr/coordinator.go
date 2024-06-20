package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	status       int
	taskidx      int
	tasknum      int
	nReduce      int
	tasklists    []string
	workinglists map[string]int
	mu           sync.Mutex
	done         bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Applytask(args *Argstype, reply *Replytype) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.done {
		reply.Tasktype = "done"
		return nil
	}

	if c.taskidx == c.tasknum {
		reply.Tasktype = "none"

	} else if c.status == 0 {
		reply.Tasktype = "map"
		reply.Filename = c.tasklists[c.taskidx]
		reply.NReduce = c.nReduce
		c.taskidx++
		c.workinglists[reply.Filename] = 5

	} else if c.status == 1 {
		reply.Tasktype = "reduce"
		reply.Filename = c.tasklists[c.taskidx]
		reply.NReduce = c.nReduce
		c.taskidx++
		c.workinglists[reply.Filename] = 5
	} else {
		reply.Tasktype = "none"
		fmt.Printf("error status\n")
	}
	return nil
}

func (c *Coordinator) Submittask(args *Argstype, reply *Replytype) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.workinglists, args.Filename)

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := false

	// Your code here.

	for key, value := range c.workinglists {
		if value == 0 {
			delete(c.workinglists, key)
			c.tasklists = append(c.tasklists, key)
			c.tasknum++
		} else {
			c.workinglists[key] = value - 1
		}
	}
	if c.tasknum == c.taskidx && len(c.workinglists) == 0 {
		if c.status == 0 {
			c.status = 1
			c.taskidx = 0
			c.tasknum = c.nReduce
			c.tasklists = make([]string, c.nReduce)
			for i := 0; i < c.nReduce; i++ {
				c.tasklists[i] = strconv.Itoa(i)
			}
		} else {
			c.done = true
			return true
		}
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.status = 0
	c.tasklists = files
	c.taskidx = 0
	c.tasknum = len(files)
	c.workinglists = make(map[string]int)
	c.nReduce = nReduce
	c.mu = sync.Mutex{}
	c.done = false

	c.server()
	return &c
}
