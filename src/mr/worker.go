package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"math/rand"
	"time"
	"strings"
	"bufio"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {

}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func readFile(filename string) (string, error) {
	content, err := os.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return string(content), err
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	
	rand.Seed(time.Now().UnixNano()) // 设置随机种子
	workerID := rand.Intn(900000) + 100000 
	for {
		request := TaskRequest{}
		reply := TaskReply{}
		request.Workerid = workerID
		ok := call("Coordinator.AskForWork", &request, &reply)
		fmt.Println(reply)
		if ok {
			// reply.Y should be 100.
			fmt.Printf("reply.Y %d\n", reply.Taskid)
		} else {
			fmt.Printf("call failed!\n")
		}

		if reply.Tasktype == "map" {
			taskid := reply.Taskid
			filename := reply.Filename
			reducenum := reply.Reducenum
			content, err := readFile(filename)
			if err != nil {
				fmt.Printf("Error reading file %s: %v\n", filename, err)
				// 还需要向协调器报告任务失败
				return
			}
			intermediate := mapf(filename, content)
			sort.Sort(ByKey(intermediate))
			files := make(map[string]*os.File)
			for i := 0; i < reducenum ;i++ {
				oname := "mr-" + strconv.Itoa(taskid) + "-" + strconv.Itoa(i)
				files[oname], err = os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					fmt.Printf("打开文件失败: %v\n", err)
					return
				}
				defer files[oname].Close()
			}
			for _, kv := range intermediate {
				oname := "mr-" + strconv.Itoa(taskid) + "-" + strconv.Itoa(ihash(kv.Key) % reducenum)
				file := files[oname]
				line := fmt.Sprintf("%s %s\n", kv.Key, kv.Value)
				_, err = file.WriteString(line)
				if err != nil {
					fmt.Printf("写入文件 %s 失败: %v\n", oname, err)
					return
				}
			}
			report := ReportTask{}
			reportreply := ReportTaskReply{}
			report.Taskid = taskid
			report.Tasktype = "map"
			ok := call("Coordinator.ReplyForWork", &report, &reportreply)
			if ok {
			} else {
				fmt.Printf("call failed!\n")
			}
		} else if reply.Tasktype == "reduce" {
			taskid := reply.Taskid
			mapnum := reply.Mapnum
			var err error

			files := make(map[string]*os.File)
			for i := 0; i < mapnum ;i++ {
				oname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(taskid)
				files[oname], err = os.Open(oname)
				if err != nil {
					fmt.Printf("打开文件失败: %v\n", err)
					return
				}
				defer files[oname].Close()
			}

			outputfilename := "mr-out-" + strconv.Itoa(taskid)
			outputfile, err := os.OpenFile(outputfilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				fmt.Printf("Error reading file %s: %v\n", outputfilename, err)
				return
			}
			defer outputfile.Close()

			kvs := []KeyValue{}
			for _, file := range files {	
				scanner := bufio.NewScanner(file)

				for scanner.Scan() {
					line := scanner.Text()
					parts := strings.Fields(line) // 按空格分割
					if len(parts) >= 2 {
						kvs = append(kvs, KeyValue{
							Key:   parts[0],
							Value: parts[1],
						})
					}
				}
				if err := scanner.Err(); err != nil {
					panic(err)
				}
			}

			i := 0
			for i < len(kvs) {
				j := i + 1
				for j < len(kvs) && kvs[j].Key == kvs[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kvs[k].Value)
				}
				output := reducef(kvs[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(outputfile, "%v %v\n", kvs[i].Key, output)
				i = j
			}

			report := ReportTask{}
			reportreply := ReportTaskReply{}
			report.Taskid = taskid
			report.Tasktype = "reduce"
			ok := call("Coordinator.ReplyForWork", &report, &reportreply)
			if ok {
			} else {
				fmt.Printf("call failed!\n")
			}
		} else if reply.Tasktype == "None" {
			fmt.Println("Mission Completed\n")
			return
		} else {
			fmt.Printf("Woring Tasktype!!!\n")
			return
		}
	}
	// CallExample()
}

//
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
