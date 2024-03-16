package mr

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Coordinator struct {
	//任务channel
	ReadyMapChannel chan *Task
	//Done channel
	ReadyReduceChannel chan *Task
	//Count
	FinishedMapCnt    int
	FinishedReduceCnt int
	TaskTable         map[int]*Task
	TaskList          []*Task
	Files             []string
	MapperNum         int
	ReducerNum        int
	AssignPhase       AssignPhase
	workerId          int

	Lock sync.RWMutex
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
	//ret := c.AllMapDone && c.AllReduceDone
	//return ret
	c.Lock.Lock()
	defer c.Lock.Unlock()
	return c.AssignPhase == DonePhase
}

//
// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.Printf("[STATUES] begin ")
	//创建有一个master
	c := Coordinator{
		ReadyMapChannel: make(chan *Task, len(files)),

		ReadyReduceChannel: make(chan *Task, nReduce),
		TaskTable:          make(map[int]*Task),
		Files:              files,      // input file list
		MapperNum:          len(files), // the number of map worker, determined by the length of input file list
		ReducerNum:         nReduce,    // the number of reduce worker, determined by the user program
		AssignPhase:        MapPhase,   // start with map phase
		workerId:           0,          // starts from 0
	}
	//配置master
	func() {
		for _, file := range files {
			task := Task{
				TaskType:   Map,
				InputFile:  file,
				TaskStatus: Ready,
			}
			c.ReadyMapChannel <- &task
		}
		log.Printf("[STATUES] create file ")

	}()

	go c.RunningWorkRemove()
	//master server up
	c.server()
	return &c
}

// RunningWorkRemove 不断的从检查正在执行的Task
// 过期超时的放到 Ready队列
// 去除Running Work中已经完成的
func (c *Coordinator) RunningWorkRemove() {
	for !c.Done() {
		time.Sleep(time.Second)
		//设计队列操作必须加锁
		c.Lock.Lock()
		if c.AssignPhase == MapPhase {
			for _, task := range c.TaskList {
				if task.TaskStatus == Running && (time.Now().Sub(task.BeginTime) > (TaskExpiredTime)*time.Second) {
					task.TaskStatus = Ready
					c.ReadyMapChannel <- task
					log.Printf("[expired]  id = %d ", task.WorkerId)
				}
			}
		} else {
			for _, task := range c.TaskList {
				if task.TaskStatus == Running && (time.Now().Sub(task.BeginTime) > (TaskExpiredTime)*time.Second) {
					task.TaskStatus = Ready
					c.ReadyReduceChannel <- task
					log.Printf("[expired] T id = %d ", task.WorkerId)
				}
			}
		}
		c.Lock.Unlock()
	}
	time.Sleep(3 * time.Second)
}

// AssignTask  远程调用的函数用来分配
// 从 ReadyMapChannel 和 ReadyReduceChannel 里面分配， 没有就分配 Task 为nil
// 分配任务的类型取决于阶段
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	switch c.AssignPhase {
	case DonePhase:
		reply.Done = true
		return nil
	case MapPhase:
		select {
		case task := <-c.ReadyMapChannel:
			task.WorkerId = c.workerId
			c.TaskTable[c.workerId] = task
			c.workerId++
			task.TaskStatus = Running
			task.TaskType = Map
			task.BeginTime = time.Now()
			reply.Task = task
			reply.NReduce = c.ReducerNum
			reply.Done = false
			c.TaskList = append(c.TaskList, task)
			return nil
		default:
			return nil
		}
	case ReducePhase:
		select {
		case task := <-c.ReadyReduceChannel:
			task.WorkerId = c.workerId
			c.TaskTable[c.workerId] = task
			c.workerId++
			task.TaskStatus = Running
			task.TaskType = Reduce
			task.BeginTime = time.Now()
			reply.Task = task
			reply.NReduce = c.ReducerNum
			reply.Done = false
			c.TaskList = append(c.TaskList, task)
			return nil
		default:
			return nil
		}
	default:
		return errors.New("No reduce task available")
	}
}

// Response to Map task done request from worker
func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	task := c.TaskTable[args.WorkerId]
	task.TaskStatus = Finished
	switch args.TaskType {
	case int(Map):
		files := args.Files
		for _, file := range files {
			tmpFile, _ := os.Open(file)
			tmpFileName := filepath.Base(file)
			finalFilePath := FinalMapFilePath + tmpFileName
			finalFile, _ := os.Create(finalFilePath)
			_, err := io.Copy(finalFile, tmpFile)
			if err != nil {
				log.Println("finalFile fail", finalFilePath)
			}
			_ = tmpFile.Close()
			_ = finalFile.Close()
		}
		c.FinishedMapCnt++
		log.Println("FinishedMapCnt", c.FinishedMapCnt, "MapperNum", c.MapperNum)
		if c.FinishedMapCnt == c.MapperNum {
			c.AssignPhase = ReducePhase
			// Make Reduce Worker
			for i := 0; i < c.ReducerNum; i++ {
				task := Task{
					TaskType:   Reduce,
					InputFile:  fmt.Sprintf("%vmr-*-%v", FinalMapFilePath, i),
					TaskStatus: Ready,
				}
				c.ReadyReduceChannel <- &task
			}
			_ = c.rmMapTmpFiles()
		}
		log.Printf("[finished]  MapWorkerId = %d ", task.WorkerId)
		return nil
	case int(Reduce):
		file := args.Files[0]
		tmpFile, _ := os.Open(file)
		tmpFileName := filepath.Base(file)
		finalFilePath := FinalReduceFilePath + tmpFileName
		finalFile, _ := os.Create(finalFilePath)
		_, err := io.Copy(finalFile, tmpFile)
		if err != nil {
			log.Println("FinalReduceFilePath finalFile fail", finalFilePath)
		}
		_ = tmpFile.Close()
		_ = finalFile.Close()
		c.FinishedReduceCnt++
		log.Println("FinishedReduceCnt", c.FinishedReduceCnt, "ReducerNum", c.ReducerNum)
		if c.FinishedReduceCnt == c.ReducerNum {
			c.AssignPhase = DonePhase
			_ = c.rmMapFinalFiles()
			_ = c.rmReduceTmpFiles()
		}
		log.Printf("[finished]  ReduceWorkerId = %d ", task.WorkerId)
		return nil
	default:
		return errors.New("TaskType unknown")
	}
}

// Remove map task's  temporary files
func (c *Coordinator) rmMapTmpFiles() error {
	d, _ := os.Open(TmpMapFilePath)
	defer d.Close()
	names, _ := d.Readdirnames(-1)
	for _, name := range names {
		_ = os.RemoveAll(TmpMapFilePath + name)
	}
	return nil
}

// Remove map task's final files
func (c *Coordinator) rmMapFinalFiles() error {
	d, _ := os.Open(FinalMapFilePath)
	defer d.Close()
	names, _ := d.Readdirnames(-1)
	for _, name := range names {
		_ = os.RemoveAll(FinalMapFilePath + name)
	}
	return nil
}

// Remove reduce task's temporary files
func (c *Coordinator) rmReduceTmpFiles() error {
	d, _ := os.Open(TmpReduceFilePath)
	defer d.Close()
	names, _ := d.Readdirnames(-1)
	for _, name := range names {
		_ = os.RemoveAll(TmpReduceFilePath + name)
	}
	return nil
}
