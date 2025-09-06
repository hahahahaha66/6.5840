package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Task struct {
	TaskID int
	Type string
	File string
	WorkerID int
	Status string
}

type Coordinator struct {
	mapTasks []Task
	reduceTasks []Task
	reducenum int
	mapnum int
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
	for i := range c.mapTasks {
		if c.mapTasks[i].WorkerID == 0 && c.mapTasks[i].Status == "Idle"{
			reply.Taskid = c.mapTasks[i].TaskID
			reply.Filename = c.mapTasks[i].File
			reply.Tasktype = c.mapTasks[i].Type
			reply.Reducenum = c.reducenum
			reply.Mapnum = c.mapnum
			c.mapTasks[i].WorkerID = args.Workerid
			c.mapTasks[i].Status = "Working"
			fmt.Println(c.mapTasks[i])
			return nil
		} else {
			continue
		}
	}

	for {
		judge := true
		for i := range c.mapTasks {
			if c.mapTasks[i].Status != "Finish" {
				judge = false
				break
			}
		}
		if judge {
			break
		} else {
			time.Sleep(2)
		}
	}

	for i := range c.reduceTasks {
		if c.reduceTasks[i].WorkerID == 0 && c.reduceTasks[i].Status == "Idle" {
			reply.Taskid = c.reduceTasks[i].TaskID
			reply.Tasktype = c.reduceTasks[i].Type
			reply.Reducenum = c.reducenum
			reply.Mapnum = c.mapnum
			c.reduceTasks[i].WorkerID = args.Workerid
			c.reduceTasks[i].Status = "Working"
			fmt.Println(c.reduceTasks[i])
			return nil
		} else {
			continue
		}
	}

	reply.Tasktype = "None"
	return nil
}

func (c *Coordinator) ReplyForWork(args *ReportTask, reply *ReportTaskReply) error {
	if args.Tasktype == "map" {
		for i := range c.mapTasks {
			if c.mapTasks[i].TaskID == args.Taskid {
				c.mapTasks[i].Status = "Finish"
				break
			}
		}
	} else if args.Tasktype == "reduce" {
		for i := range c.reduceTasks {
			if c.reduceTasks[i].TaskID == args.Taskid {
				c.reduceTasks[i].Status = "Finish"
				break
			}
		}
	} else {
		fmt.Println("Woring type")
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
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
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

	c.server()
	return &c
}
