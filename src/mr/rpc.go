package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

const (
	MapTask int = iota
	ReduceTask
	Waiting
	Finish
)

type MRTask struct {
	TaskType int
	TaskId   int
}

type TaskReport struct {
	WorkerId int
	Task     MRTask
}

type AllTasks struct {
	Filenames []string
	NReduce   int
}

type Heartbeat struct {
	WorkerId  int
	Timestamp int64
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
