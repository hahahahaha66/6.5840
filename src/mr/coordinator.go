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

type Task struct {
	TaskID int
	Type string
	File string
	WorkerID int
	Status string
	TimeOut time.Time
}

type Coordinator struct {
	mapTasks []Task
	reduceTasks []Task
	reducenum int
	mapnum int
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AskForWork(args *TaskRequest, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.mapTasks {
		if c.mapTasks[i].WorkerID == 0 && c.mapTasks[i].Status == "Idle"{
			reply.Taskid = c.mapTasks[i].TaskID
			reply.Filename = c.mapTasks[i].File
			reply.Tasktype = c.mapTasks[i].Type
			reply.Reducenum = c.reducenum
			reply.Mapnum = c.mapnum
			c.mapTasks[i].WorkerID = args.Workerid
			c.mapTasks[i].TimeOut = time.Now()
			c.mapTasks[i].Status = "Working"
			return nil
		} else {
			continue
		}
	}

	for i := range c.mapTasks {
		if c.mapTasks[i].Status != "Finish" {
			reply.Tasktype = "Waiting"
			return nil
		}
	}

	for i := range c.reduceTasks {
		if c.reduceTasks[i].WorkerID == 0 && c.reduceTasks[i].Status == "Idle" {
			reply.Taskid = c.reduceTasks[i].TaskID
			reply.Tasktype = c.reduceTasks[i].Type
			reply.Reducenum = c.reducenum
			reply.Mapnum = c.mapnum
			c.reduceTasks[i].WorkerID = args.Workerid
			c.reduceTasks[i].TimeOut = time.Now()
			c.reduceTasks[i].Status = "Working"
			return nil
		} else {
			continue
		}
	}

	for i := range c.reduceTasks {
		if c.reduceTasks[i].Status != "Finish" {
			reply.Tasktype = "Waiting"
			return nil
		}
	}

	reply.Tasktype = "None"
	return nil
}

func (c *Coordinator) ReplyForWork(args *ReportTask, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Tasktype == "map" {
		for i := range c.mapTasks {
			if c.mapTasks[i].TaskID == args.Taskid && c.mapTasks[i].WorkerID == args.WorkerId {
				c.mapTasks[i].Status = "Finish"
				c.mapTasks[i].TimeOut = time.Now()
				break
			}
		}
	} else if args.Tasktype == "reduce"{
		for i := range c.reduceTasks {
			if c.reduceTasks[i].TaskID == args.Taskid && c.reduceTasks[i].WorkerID == args.WorkerId{
				c.reduceTasks[i].Status = "Finish"
				c.reduceTasks[i].TimeOut = time.Now()
				break
			}
		}
	} else {
		fmt.Println("Woring type")
	}
	return nil
}

func (c* Coordinator) SendHeartbeat(args *HeartPacket, reply *HeartPacketreply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	workerid := *&args.WorkerId
	for i, _ := range c.mapTasks {
		if workerid == c.mapTasks[i].WorkerID {
			c.mapTasks[i].TimeOut = time.Now()
			return nil
		}
	}
	for i, _ := range c.reduceTasks {
		if workerid == c.reduceTasks[i].WorkerID {
			c.reduceTasks[i].TimeOut = time.Now()
			return nil
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := false
	for _, it := range c.mapTasks {
		if it.Status != "Finish" {
			return ret
		}
	}

	for _, it := range c.reduceTasks {
		if it.Status != "Finish" {
			return ret	
		}
	}
	ret = true
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//

func (c *Coordinator) TimeoutDetection() {
	for {
		time.Sleep(2 * time.Millisecond)
		c.mu.Lock()
		for i, _ := range c.mapTasks {
			diff := time.Now().Sub(c.mapTasks[i].TimeOut)
			if c.mapTasks[i].Status == "Working" && diff > 10 * time.Second {
				// fmt.Printf("having a map task free\n")
				c.mapTasks[i].WorkerID = 0
				c.mapTasks[i].Status = "Idle"
			}
		}
		for j, _ := range c.reduceTasks {
			diff := time.Now().Sub(c.reduceTasks[j].TimeOut)
			if c.reduceTasks[j].Status == "Working" && diff > 3 * time.Second {
				// fmt.Printf("having a reduce task free\n")
				c.reduceTasks[j].WorkerID = 0
				c.reduceTasks[j].Status = "Idle"
			}
		}
		c.mu.Unlock()
	}
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mapnum = len(files)
	c.mapTasks = make([]Task, len(files))
	for i, file := range files {
		c.mapTasks[i].TaskID = i
		c.mapTasks[i].File = file
		c.mapTasks[i].Type = "map"
		c.mapTasks[i].Status = "Idle"
	}
	// Your code here.
	c.reducenum = nReduce
	c.reduceTasks = make([]Task, nReduce)
	for i, _ := range c.reduceTasks {
		c.reduceTasks[i].TaskID = i
		c.reduceTasks[i].Type = "reduce"
		c.reduceTasks[i].Status = "Idle"
	}

	go c.TimeoutDetection()
	c.server()
	return &c
}
