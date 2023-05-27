package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Queue []int

func (q *Queue) push(v int) {
	*q = append(*q, v)
}

func (q *Queue) pop() int {
	head := (*q)[0]
	*q = (*q)[1:]
	return head
}

func (q *Queue) isEmpty() bool {
	return len(*q) == 0
}

var heartbeatTimeout = 3 * time.Second.Nanoseconds()

type Coordinator struct {
	taskLock sync.Mutex

	filenames []string
	// Remaining MapTasks
	mapTasks        Queue
	finishedMapTask int

	nReduce int
	// Remaining ReduceTasks
	reduceTasks        Queue
	finishedReduceTask int

	// Map<workerId, mrTask>
	workerMap map[int]MRTask

	heartbeatLock sync.Mutex
	// Map<workerId, lastTimestamp>
	heartbeatMap map[int]int64
}

func (c *Coordinator) GetAllTasks(request *interface{}, allTasks *AllTasks) error {
	allTasks.Filenames = c.filenames
	allTasks.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) AssignTask(workerId *int, mrTask *MRTask) error {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()

	if !c.mapTasks.isEmpty() {
		// Assign a MapTask
		mrTask.TaskType = MapTask
		mrTask.TaskId = c.mapTasks.pop()
		c.workerMap[*workerId] = *mrTask
		// fmt.Printf("[Assign] Worker: %d MapTask: %d\n", *workerId, mrTask.TaskId)

	} else if c.finishedMapTask < len(c.filenames) {
		// Waiting for all MapTasks finished
		mrTask.TaskType = Waiting

	} else if !c.reduceTasks.isEmpty() {
		// Assign a ReduceTask
		mrTask.TaskType = ReduceTask
		mrTask.TaskId = c.reduceTasks.pop()
		c.workerMap[*workerId] = *mrTask
		// fmt.Printf("[Assign] Worker: %d ReduceTask: %d\n", *workerId, mrTask.TaskId)

	} else if c.finishedReduceTask < c.nReduce {
		// Waiting for all ReduceTasks finished
		mrTask.TaskType = Waiting

	} else {
		mrTask.TaskType = Finish
	}

	return nil
}

func (c *Coordinator) ReportTask(task *TaskReport, response *interface{}) error {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()

	if task.Task.TaskType == MapTask {
		c.finishedMapTask += 1
		// fmt.Printf("[Finish] Worker: %d, MapTask: %d\n", task.WorkerId, task.Task.TaskId)
	} else {
		c.finishedReduceTask += 1
		// fmt.Printf("[Finish] Worker: %d, ReduceTask: %d\n", task.WorkerId, task.Task.TaskId)
	}
	delete(c.workerMap, task.WorkerId)

	return nil
}

func (c *Coordinator) ReportHeartbeat(heartbeat *Heartbeat, response *interface{}) error {
	c.heartbeatLock.Lock()
	defer c.heartbeatLock.Unlock()

	c.heartbeatMap[heartbeat.WorkerId] = heartbeat.Timestamp

	return nil
}

func (c *Coordinator) checkHeartbeat() {
	for {
		c.heartbeatLock.Lock()
		var crashedWorkers []int
		currentTime := time.Now().UnixNano()
		for workerId, lastHeartbeat := range c.heartbeatMap {
			// A worker crashed
			if currentTime-lastHeartbeat > heartbeatTimeout {
				crashedWorkers = append(crashedWorkers, workerId)
			}
		}
		for _, workerId := range crashedWorkers {
			delete(c.heartbeatMap, workerId)
		}
		c.heartbeatLock.Unlock()

		if len(crashedWorkers) > 0 {
			c.taskLock.Lock()
			for _, workerId := range crashedWorkers {
				mrTask, ok := c.workerMap[workerId]
				if ok {
					// Reconstruct Task
					if mrTask.TaskType == MapTask {
						c.mapTasks.push(mrTask.TaskId)
					} else {
						c.reduceTasks.push(mrTask.TaskId)
					}
					delete(c.workerMap, workerId)
				}
			}
			c.taskLock.Unlock()
		}

		time.Sleep(5 * time.Second)
	}
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
	c.taskLock.Lock()
	defer c.taskLock.Unlock()
	return c.finishedMapTask == len(c.filenames) && c.finishedReduceTask == c.nReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.filenames = files
	c.finishedMapTask = 0
	c.mapTasks = Queue{}
	for i := 0; i < len(files); i++ {
		c.mapTasks.push(i)
	}

	c.nReduce = nReduce
	c.finishedReduceTask = 0
	c.reduceTasks = Queue{}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks.push(i)
	}

	c.workerMap = make(map[int]MRTask)
	c.heartbeatMap = make(map[int]int64)

	go c.checkHeartbeat()

	c.server()
	return &c
}
