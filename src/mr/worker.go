package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type MRWorker struct {
	WorkerId   int
	MapFunc    func(string, string) []KeyValue
	ReduceFunc func(string, []string) string
	Task       *Task
	NReduce    int
	Finished   bool
}

func Worker(mapFunc func(string, string) []KeyValue, reduceFunc func(string, []string) string) {
	worker := MRWorker{
		WorkerId:   -1, // initialized to -1 when the worker isn't handle a task
		MapFunc:    mapFunc,
		ReduceFunc: reduceFunc,
		Finished:   false,
	}
	worker.work()
}

// work  main function
func (worker *MRWorker) work() {
	log.Printf("[WORKER] start worker")
	for !worker.Finished {
		task, _ := worker.requestForTask()
		if task == nil {
			continue
		}
		worker.Task = task
		worker.WorkerId = task.WorkerId
		if task.TaskType == Map {
			log.Printf("[WORKER] start MapWork")
			worker.WorkerId = task.WorkerId
			_ = worker.MapWork()
		} else {
			log.Printf("[WORKER] start ReduceWork")
			worker.WorkerId = task.WorkerId
			_ = worker.ReduceWork()
		}
	}
}

// Request task to coordinator
func (worker *MRWorker) requestForTask() (*Task, error) {
	args := TaskArgs{}
	reply := TaskReply{}
	_ = call("Coordinator.AssignTask", &args, &reply)

	worker.Task = reply.Task
	if reply.Done {
		worker.Finished = true
		return nil, nil
	}
	worker.NReduce = reply.NReduce
	return reply.Task, nil
}

// Execute map tesk details
func (worker *MRWorker) MapWork() error {
	log.Printf("[WORKER] MapWork")
	task := worker.Task
	//输入只有一个
	intermediate, err := worker.MakeIntermediate(task.InputFile)
	if err != nil {
		log.Println("MapWork.MakeIntermediate err = ", err)
		return err
	}

	tmpFiles, _ := worker.writeIntermediateToTmpFiles(intermediate)
	if err != nil {
		log.Println("MapWork.writeIntermediateToTmpFiles :", err)
		return err
	}

	err = worker.mapTaskDone(tmpFiles)
	if err != nil {
		log.Println("MapWork.mapTaskDone err = ", err)
		for _, file := range tmpFiles {
			err := os.Remove(file)
			if err != nil {
				log.Println("worker.mapTaskDone.os.Remove err = ", err)
			}
		}
		return err
	}
	return nil
}

func (worker *MRWorker) MakeIntermediate(filename string) ([]KeyValue, error) {
	intermediate := make([]KeyValue, 0)
	file, _ := os.Open(filename)
	defer file.Close()
	content, _ := io.ReadAll(file)

	kva := worker.MapFunc(filename, string(content))
	intermediate = append(intermediate, kva...)
	return intermediate, nil
}

// Write intermediate to map task's temporary files
func (worker *MRWorker) writeIntermediateToTmpFiles(intermediate []KeyValue) ([]string, error) {

	var tmpFiles []string
	hashedIntermediate := make([][]KeyValue, worker.NReduce)

	for _, kv := range intermediate {
		hashVal := ihash(kv.Key) % worker.NReduce
		hashedIntermediate[hashVal] = append(hashedIntermediate[hashVal], kv)
	}

	for i := 0; i < worker.NReduce; i++ {
		tmpFile, err := os.CreateTemp(TmpMapFilePath, "mr-*.txt")
		if err != nil {
			log.Println("writeIntermediateToTmpFiles.os.CreateTemp err = ", err)
			return nil, err
		}
		defer os.Remove(tmpFile.Name())
		defer tmpFile.Close()

		enc := json.NewEncoder(tmpFile)
		for _, kv := range hashedIntermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Println("writeIntermediateToTmpFiles.enc.Encode", err)
				return nil, err
			}
		}

		file_path := fmt.Sprintf("mr-%v-%v", worker.WorkerId, i)
		err = os.Rename(tmpFile.Name(), TmpMapFilePath+file_path)
		if err != nil {
			log.Println("writeIntermediateToTmpFiles os.Rename: ", err)
			return nil, err
		}
		tmpFiles = append(tmpFiles, TmpMapFilePath+file_path)
	}

	return tmpFiles, nil
}

// Report map task done to coordinator
func (worker *MRWorker) mapTaskDone(files []string) error {
	args := TaskDoneArgs{
		WorkerId: worker.WorkerId,
		TaskType: int(Map),
		Files:    files,
	}
	reply := TaskDoneReply{}
	err := call("Coordinator.TaskDone", &args, &reply)
	if err != nil {
		log.Println("mapTaskDone.call err = ", err)
		return err
	}
	if reply.Err != nil {
		log.Println("mapTaskDone.reply.Err = ", reply.Err)
		return reply.Err
	}
	return nil
}

// Execute reduce tesk details
func (worker *MRWorker) ReduceWork() error {

	log.Printf("[WORKER] ReduceWork")
	intermediate, _ := worker.collectIntermediate(worker.Task.InputFile)
	sort.Sort(ByKey(intermediate))
	res := worker.MakeReduceRes(intermediate)
	tmpFile, err := worker.writeReduceResToTmpFile(res)
	err = worker.reduceTaskDone(tmpFile)
	if err != nil {
		log.Println("ReduceWork.reduceTaskDone err = ", err)
		err := os.Remove(tmpFile)
		if err != nil {
			log.Println("ReduceWork.os.Remove err = ", err)
		}
		return err
	}

	return nil
}

// Collect intermediate from different map workers' result files
func (worker *MRWorker) collectIntermediate(filePattern string) ([]KeyValue, error) {
	intermediate := make([]KeyValue, 0)
	files, err := filepath.Glob(filePattern)
	if err != nil {
		log.Println("collectIntermediate.filepath.Glob err = ", err)
		return nil, err
	}

	for _, filePath := range files {
		file, err := os.Open(filePath)
		if err != nil {
			log.Println("collectIntermediates.Open err = ", err)
			return nil, err
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	return intermediate, nil
}

func (worker *MRWorker) MakeReduceRes(intermediate []KeyValue) []KeyValue {
	res := make([]KeyValue, 0)
	i := 0
	for i < len(intermediate) {
		//  the key in intermediate [i...j]  is the same since intermediate is already sorted
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		// sum the val number of intermediate [i...j]
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := worker.ReduceFunc(intermediate[i].Key, values)

		kv := KeyValue{Key: intermediate[i].Key, Value: output}
		res = append(res, kv)

		i = j
	}
	return res
}

// write reduce task's result to temporary file
func (worker *MRWorker) writeReduceResToTmpFile(res []KeyValue) (string, error) {
	tempFile, err := os.CreateTemp(TmpReduceFilePath, "mr-") ///home/distributed_system/tmp_res_file/mr-xxxxx(随机字符串)
	if err != nil {
		log.Println("writeReduceResToTmpFile.os.CreateTemp err = ", err)
		return "", err
	}

	// write key-val pair into tmp file
	for _, kv := range res {
		_, _ = fmt.Fprintf(tempFile, "%s %s\n", kv.Key, kv.Value)
	}

	tempName := TmpReduceFilePath + "mr-out-" + strconv.Itoa(worker.WorkerId) + ".txt"
	err = os.Rename(tempFile.Name(), tempName)
	if err != nil {
		log.Println("writeReduceResToTmpFile.os.Rename err = ", err)
		return "", err
	}

	return tempName, nil
}

// Report reduce task done to coordinator
func (worker *MRWorker) reduceTaskDone(file string) error {
	args := TaskDoneArgs{
		WorkerId: worker.WorkerId,
		TaskType: int(Reduce),
		Files:    []string{file},
	}
	reply := TaskDoneReply{}
	err := call("Coordinator.TaskDone", &args, &reply)
	if err != nil {
		log.Println("reduceTaskDone.call ", err)
		return err
	}
	if reply.Err != nil {
		log.Println(err)
		return reply.Err
	}
	return nil
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// rpc call function that send request to coordinator
func call(funName string, args interface{}, reply interface{}) error {

	name := coordinatorSock()
	c, err := rpc.DialHTTP("unix", name)
	if err != nil {
		log.Println("[ERROR] rpc error", err)
		time.Sleep(3 * time.Second)
		return err
	}
	defer c.Close()
	_ = c.Call(funName, args, reply)
	return nil
}
