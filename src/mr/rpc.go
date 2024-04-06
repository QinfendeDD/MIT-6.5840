package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"sync"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

var mux sync.Mutex

// TaskType RPC status
type TaskType uint8

const (
	// TaskTypeUnknown Unknown status
	TaskTypeUnknown TaskType = 0
	// TaskTypeRequestMap request map status
	TaskTypeRequestMap TaskType = 1
	// TaskTypeRequestReduce request reduce status
	TaskTypeRequestReduce TaskType = 2
	// TaskTypeWaiting Waiting
	TaskTypeWaiting TaskType = 3
)

func (v TaskType) Type2Int() int {
	return int(v)
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Task task struct
type Task struct {
	// TaskID task id
	TaskID int
	// TaskType type of task
	Type TaskType
	// StartTime task start time
	StartTime time.Time
	// IsFinished task is finished
	IsFinished bool
	// ToBeOperated to be operated file name
	ToBeOperatedFileName string
	// NReduce number of reduce
	NReduce int
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
