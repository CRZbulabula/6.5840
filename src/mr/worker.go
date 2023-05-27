package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

var nReduce int
var filenames []string
var heartbeatInterval = 500 * time.Millisecond

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workerId := os.Getpid()
	go heartbeatLoop(workerId)

	allTasks := AllTasks{}
	call("Coordinator.GetAllTasks", new(interface{}), &allTasks)
	nReduce = allTasks.NReduce
	filenames = allTasks.Filenames

	lastTaskType := Waiting
	for lastTaskType != Finish {
		// Ask for a task
		mrTask := MRTask{}
		call("Coordinator.AssignTask", &workerId, &mrTask)

		// Check TaskType
		lastTaskType = mrTask.TaskType
		if mrTask.TaskType == MapTask {
			mapFromFile(mrTask.TaskId, mapf)
			call("Coordinator.ReportTask", TaskReport{WorkerId: workerId, Task: mrTask}, new(interface{}))

		} else if mrTask.TaskType == ReduceTask {
			reduceToFile(mrTask.TaskId, reducef)
			call("Coordinator.ReportTask", TaskReport{WorkerId: workerId, Task: mrTask}, new(interface{}))

		} else if mrTask.TaskType == Waiting {
			time.Sleep(time.Second)
		}
	}
}

func mapFromFile(mapId int, mapf func(string, string) []KeyValue) {
	filename := filenames[mapId]
	file, _ := os.Open(filename)
	content, _ := ioutil.ReadAll(file)
	_ = file.Close()

	kvList := mapf(filename, string(content))

	fileMap := make(map[int]*os.File)
	encoderMap := make(map[int]*json.Encoder)
	for i := 0; i < len(kvList); i++ {
		reduceId := ihash(kvList[i].Key) % nReduce
		encoder, ok := encoderMap[reduceId]
		if !ok {
			tmpFile, _ := os.Create(fmt.Sprintf("mr-%d-%d.json", mapId, reduceId))
			fileMap[reduceId] = tmpFile
			encoderMap[reduceId] = json.NewEncoder(tmpFile)
			encoder = encoderMap[reduceId]
		}
		_ = encoder.Encode(kvList[i])
	}

	for _, tmpFile := range fileMap {
		_ = tmpFile.Close()
	}
}

func reduceToFile(reduceId int, reducef func(string, []string) string) {
	var kvList []KeyValue
	for mapId := 0; mapId < len(filenames); mapId++ {
		filename := fmt.Sprintf("mr-%d-%d.json", mapId, reduceId)
		_, err := os.Stat(filename)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
		}

		file, _ := os.Open(filename)
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kvList = append(kvList, kv)
		}
		_ = file.Close()
	}

	sort.Sort(ByKey(kvList))
	file, _ := os.Create(fmt.Sprintf("mr-out-%d", reduceId))
	for i := 0; i < len(kvList); {
		j := i
		var values []string
		for j < len(kvList) && kvList[j].Key == kvList[i].Key {
			values = append(values, kvList[j].Value)
			j += 1
		}
		fmt.Fprintf(file, "%v %v\n", kvList[i].Key, reducef(kvList[i].Key, values))
		i = j
	}
	_ = file.Close()
}

func heartbeatLoop(workerId int) {
	for {
		heartbeat := Heartbeat{}
		heartbeat.WorkerId = workerId
		heartbeat.Timestamp = time.Now().UnixNano()
		call("Coordinator.ReportHeartbeat", &heartbeat, new(interface{}))
		time.Sleep(heartbeatInterval)
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
