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
	ReadyMapChannel    chan *Task
	RunningMapChannel  chan *Task
	FinishedMapChannel chan []string
	//Done channel
	ReadyReduceChannel    chan *Task
	RunningReduceChannel  chan *Task
	FinishedReduceChannel chan string
	//Count
	FinishedMapCnt    int
	FinishedReduceCnt int
	TaskTable         map[int]*Task
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
		ReadyMapChannel:    make(chan *Task, len(files)),
		RunningMapChannel:  make(chan *Task, len(files)),
		FinishedMapChannel: make(chan []string, len(files)),

		ReadyReduceChannel:    make(chan *Task, nReduce),
		RunningReduceChannel:  make(chan *Task, nReduce),
		FinishedReduceChannel: make(chan string, nReduce),
		TaskTable:             make(map[int]*Task),
		Files:                 files,      // input file list
		MapperNum:             len(files), // the number of map worker, determined by the length of input file list
		ReducerNum:            nReduce,    // the number of reduce worker, determined by the user program
		AssignPhase:           MapPhase,   // start with map phase
		workerId:              0,          // starts from 0
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
	go c.waitForDone()
	//master server up
	c.server()
	return &c
}
func (c *Coordinator) waitForDone() {
	for !c.Done() {
		select {
		case files := <-c.FinishedMapChannel:
			for _, file := range files {
				tmpFile, _ := os.Open(file)
				tmpFileName := filepath.Base(file)
				finalFilePath := FinalMapFilePath + tmpFileName
				finalFile, _ := os.Create(finalFilePath)
				_, _ = io.Copy(finalFile, tmpFile)
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

		case file := <-c.FinishedReduceChannel:
			tmpFile, _ := os.Open(file)
			tmpFileName := filepath.Base(file)
			finalFilePath := FinalReduceFilePath + tmpFileName
			finalFile, _ := os.Create(finalFilePath)
			_, _ = io.Copy(finalFile, tmpFile)
			_ = tmpFile.Close()
			_ = finalFile.Close()
			c.FinishedReduceCnt++
			log.Println("FinishedReduceCnt", c.FinishedReduceCnt, "ReducerNum", c.ReducerNum)
			if c.FinishedReduceCnt == c.ReducerNum {
				c.AssignPhase = DonePhase
				_ = c.rmMapFinalFiles()
				_ = c.rmReduceTmpFiles()
			}
		}
	}
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
			for i := 0; i < c.MapperNum; i++ {
				task := <-c.RunningMapChannel
				if task.TaskStatus == Running && (time.Now().Sub(task.BeginTime) > (TaskExpiredTime)*time.Second) {
					task.TaskStatus = Ready
					c.ReadyMapChannel <- task
					log.Printf("[expired]  id = %d ", task.WorkerId)
				} else if task.TaskStatus == Running {
					c.RunningMapChannel <- task
				}
			}
		} else {
			for i := 0; i < c.ReducerNum; i++ {
				task := <-c.RunningReduceChannel
				if task.TaskStatus == Running && (time.Now().Sub(task.BeginTime) > (TaskExpiredTime)*time.Second) {
					task.TaskStatus = Ready
					c.ReadyReduceChannel <- task
					log.Printf("[expired] T id = %d ", task.WorkerId)
				} else if task.TaskStatus == Running {
					c.RunningReduceChannel <- task
				}
			}
		}
		c.Lock.Unlock()
	}
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
			c.RunningMapChannel <- task
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
			c.RunningReduceChannel <- task
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
		c.FinishedMapChannel <- args.Files
		log.Printf("[finished]  MapWorkerId = %d ", task.WorkerId)
		return nil
	case int(Reduce):
		c.FinishedReduceChannel <- args.Files[0]
		log.Printf("[finished]  ReduceWorkerId = %d ", task.WorkerId)
		return nil
	default:
		return errors.New("TaskType unknown")
	}
}

// Remove map task's  temporary files
func (c *Coordinator) rmMapTmpFiles() error {
	d, err := os.Open(TmpMapFilePath)
	if err != nil {
		log.Println("rmMapTmpFiles os.Open err = ", err)
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		log.Println("rmMapTmpFiles.d.Readdirnames err = ", err)
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(TmpMapFilePath + name)
		if err != nil {
			log.Println("rmMapTmpFiles.os.RemoveAll err = ", err)
			return err
		}
	}
	return nil
}

// Remove map task's final files
func (c *Coordinator) rmMapFinalFiles() error {
	d, err := os.Open(FinalMapFilePath)
	if err != nil {
		log.Println("rmMapFinalFiles.os.Open err = ", err)
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		log.Println("rmMapFinalFiles.d.Readdirnames err = ", err)
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(FinalMapFilePath + name)
		if err != nil {
			log.Println("rmMapFinalFiles.os.RemoveAll err = ", err)
			return err
		}
	}
	return nil
}

// Remove reduce task's temporary files
func (c *Coordinator) rmReduceTmpFiles() error {
	d, err := os.Open(TmpReduceFilePath)
	if err != nil {
		log.Println("rmReduceTmpFiles.os.Open err = ", err)
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		log.Println("rmReduceTmpFiles.d.Readdirnames err = ", err)
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(TmpReduceFilePath + name)
		if err != nil {
			log.Println("rmReduceTmpFiles.os.RemoveAll err = ", err)
			return err
		}
	}
	return nil
}
