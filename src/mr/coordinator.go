package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	// task status
	TaskType TaskType
	// map tasks
	MapTasks []*Task
	// reduce tasks
	ReduceTasks []*Task
	// chan to notify map task
	MapTaskChan chan *Task
	// chan to notify reduce task
	ReduceTaskChan chan *Task
	// remain tasks
	RemainTasks int
}

// task timeout configuration
const taskTimeout = time.Second * 15

// Your code here -- RPC handlers for the worker to call.

// TaskController task controller
type TaskController interface {
	// DoTask do task
	DoTask(args *ExampleArgs, reply *Task) error
	// TaskDone task done
	TaskDone(task *Task, reply *ExampleReply) error
	// TaskCheck task check
	TaskCheck(nReduce int) bool
	// CreateReduce create reduce task
	CreateReduce(nReduce int)
}

// DoTask do task
func (c *Coordinator) DoTask(args *ExampleArgs, reply *Task) error {
	// lock
	mux.Lock()
	defer mux.Unlock()
	// check task type and do task
	if c.TaskType == TaskTypeRequestMap {
		// if chan of map task is not empty
		if len(c.MapTaskChan) > 0 {
			// get task
			*reply = *<-c.MapTaskChan
			// set start time
			c.MapTasks[reply.TaskID].StartTime = time.Now()
		} else {
			if args.X != TaskTypeWaiting.Type2Int() {
				reply.Type = TaskTypeWaiting
			}
			for _, task := range c.MapTasks {
				// if task is not finished and timeout, do operation
				if !task.IsFinished && time.Since(task.StartTime) > taskTimeout {
					*reply = *task
					// update task start time
					task.StartTime = time.Now()
					break
				}
			}
		}
	} else if c.TaskType == TaskTypeRequestReduce {
		// if chan of reduce task is not empty
		if len(c.ReduceTaskChan) > 0 {
			// get task
			*reply = *<-c.ReduceTaskChan
			// set start time
			c.ReduceTasks[reply.TaskID].StartTime = time.Now()
		} else {
			if args.X != TaskTypeWaiting.Type2Int() {
				reply.Type = TaskTypeWaiting
			}
			for _, task := range c.ReduceTasks {
				// if task is not finished and timeout, do operation
				if !task.IsFinished && time.Since(task.StartTime) > taskTimeout {
					*reply = *task
					// update task start time
					task.StartTime = time.Now()
					break
				}
			}
		}
	} else {
		reply.Type = TaskTypeUnknown
	}

	return nil
}

// TaskDone task done
func (c *Coordinator) TaskDone(task *Task, reply *ExampleReply) error {
	if task.Type == TaskTypeRequestMap {
		mux.Lock()
		if !c.MapTasks[task.TaskID].IsFinished {
			c.MapTasks[task.TaskID].IsFinished = true
			c.RemainTasks--
		}
		mux.Unlock()
	} else if task.Type == TaskTypeRequestReduce {
		mux.Lock()
		if !c.ReduceTasks[task.TaskID].IsFinished {
			c.ReduceTasks[task.TaskID].IsFinished = true
			c.RemainTasks--
		}
		mux.Unlock()
	}

	if c.TaskCheck(task.NReduce) {
		c.TaskType = TaskTypeWaiting
	}

	return nil
}

// TaskCheck
// Check checks if the Coordinator is ready to create new Reduce tasks.
// It returns true if there are no remaining tasks and no ongoing reduce tasks,
// or if there are already some reduce tasks in progress.
func (c *Coordinator) TaskCheck(nReduce int) bool {
	if c.RemainTasks == 0 {
		reduceTasksLen := len(c.ReduceTasks)
		if reduceTasksLen == 0 {
			c.CreateReduce(nReduce)
			return false
		}

		return true
	}

	return false
}

// create reduce task
func (c *Coordinator) createReduceTask(taskID int) *Task {
	return &Task{
		Type:                 TaskTypeRequestReduce,
		ToBeOperatedFileName: "mr-",
		NReduce:              len(c.MapTasks),
		TaskID:               taskID,
		IsFinished:           false,
		StartTime:            time.Now(),
	}
}

// CreateReduce create reduce task and put it into chan
func (c *Coordinator) CreateReduce(nReduce int) {
	for i := 0; i < nReduce; i++ {
		task := c.createReduceTask(i)
		c.ReduceTaskChan <- task
		c.ReduceTasks = append(c.ReduceTasks, task)
	}

	c.TaskType = TaskTypeRequestReduce
	c.RemainTasks = nReduce
}

// Example
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	return c.TaskType == TaskTypeWaiting
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskType:       TaskTypeRequestMap,
		MapTaskChan:    make(chan *Task, len(files)),
		ReduceTaskChan: make(chan *Task, nReduce),
		MapTasks:       make([]*Task, len(files)),
		ReduceTasks:    make([]*Task, 0),
		RemainTasks:    len(files),
	}

	for i, file := range files {
		task := Task{
			Type:                 TaskTypeRequestMap,
			ToBeOperatedFileName: file,
			NReduce:              nReduce,
			TaskID:               i,
			IsFinished:           false,
			StartTime:            time.Now(),
		}
		c.MapTaskChan <- &task
		c.MapTasks[i] = &task
	}

	c.server()
	return &c
}
