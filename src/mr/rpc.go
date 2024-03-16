package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

type TaskType int
type TaskStatus int
type AssignPhase int

const (
	Map    TaskType = 0
	Reduce TaskType = 1

	MapPhase    AssignPhase = 0
	ReducePhase AssignPhase = 1
	DonePhase   AssignPhase = 2

	// TmpMapFilePath mkdir -p /tmp/tmp_map_file /tmp/tmp_reduce_file /tmp/final_map_file /tmp/final_reduce_file
	TmpMapFilePath      = "/tmp/tmp_map_file/"
	TmpReduceFilePath   = "/tmp/tmp_reduce_file/"
	FinalMapFilePath    = "/tmp/final_map_file/"
	FinalReduceFilePath = "../mr-tmp/"

	TaskExpiredTime = 10

	Ready    TaskStatus = 0
	Running  TaskStatus = 1
	Finished TaskStatus = 2
)

// key-val pair for intermediate
type KeyValue struct {
	Key   string
	Value string
}

type Task struct {
	TaskType   TaskType   //任务类型
	WorkerId   int        //任务id
	InputFile  string     // 输入file
	BeginTime  time.Time  //开始时间
	TaskStatus TaskStatus //任务状态
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

// worker request master for task
type TaskArgs struct {
	WorkerId int
}

// master reply worker a task(the task might be nil if no task available)
type TaskReply struct {
	Task    *Task
	NReduce int
	Done    bool
}

// mapper reports to master that map task should be done
type TaskDoneArgs struct {
	WorkerId int
	TaskType int
	Files    []string
}

// master reply for mapper's map task done request
type TaskDoneReply struct {
	Err error
}

// intermediate
type ByKey []KeyValue

// for sort the intermediate
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
