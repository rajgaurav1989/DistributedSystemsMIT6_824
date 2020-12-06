package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
	"strings"
)

//MapInProgres ...
const (
	MapInProgres = iota
	ReduceInProgress
	MasterCompleted
)

//TaskIdle ...
const (
	TaskIdle = iota
	TaskRunning
	TaskCompleted
)

//WorkerIdle...
const (
	WorkerIdle = iota
	WorkerBusy
	WorkerFailed
)

//MapType ...
const (
	MapType = iota
	ReduceType
)

var mutex = sync.RWMutex{}

//Task ...
type Task struct {
	taskNum   int
	taskType  int
	taskState int
	taskFiles []string
}

type workerMachine struct {
	workerID    string
	workerState int
	timestamp   int
	task        []Task
}

//Master ...
type Master struct {
	masterState int
	failedTasks []Task
	mapFiles    []string
	reduceFiles [][]string
	workerMap   map[string]workerMachine
}

var numInitialMapFile int
var numInitialReduceFiles int

//AssignTask ...
func (m *Master) AssignTask(request *AssignTaskRequest, response *AssignTaskResponse) error {
	response.NumReducers = numInitialReduceFiles
	mutex.Lock()
	if m.masterState == MasterCompleted {
		response.MasterState = MasterCompleted
		mutex.Unlock()
		return nil
	}

	var workerID string = request.WorkerID
	
	if len(m.failedTasks) > 0 {
		failedTask := m.failedTasks[0]
		m.failedTasks = m.failedTasks[1:]
		failedTask.taskState = TaskRunning
		response.TaskType = failedTask.taskType
		response.TaskNum = failedTask.taskNum
		response.Files = failedTask.taskFiles
		response.MasterState = m.masterState
		addWorker(m, workerID, failedTask)
		mutex.Unlock()
		return nil
	}

	if m.masterState == MapInProgres {
		isMapCompleted := true

		for i := 0 ; i < numInitialReduceFiles ; i++ {
			if len(m.reduceFiles[i]) != numInitialMapFile {
				isMapCompleted = false
				break
			}
		}
		if (isMapCompleted){
			m.masterState = ReduceInProgress
		}
	}

	newTask := Task{}
	if m.masterState == MapInProgres && len(m.mapFiles) > 0 {
		newTask.taskNum = numInitialMapFile - len(m.mapFiles)
		newTask.taskType = MapType
		newTask.taskFiles = append(newTask.taskFiles, m.mapFiles[0])
		m.mapFiles = m.mapFiles[1:]
	} else if m.masterState == ReduceInProgress && len(m.reduceFiles) > 0 {
		newTask.taskNum = numInitialReduceFiles - len(m.reduceFiles)
		newTask.taskType = ReduceType
		newTask.taskFiles = append(newTask.taskFiles, m.reduceFiles[0]...)
		m.reduceFiles = m.reduceFiles[1:]
	} else {
		mutex.Unlock()
		return nil
	}
	response.Files = newTask.taskFiles
	response.TaskNum = newTask.taskNum
	response.TaskType = newTask.taskType
	response.MasterState = m.masterState
	newTask.taskState = TaskRunning
	addWorker(m, workerID, newTask)
	mutex.Unlock()
	return nil
}

//WorkerHeartBeat ...
func (m *Master) WorkerHeartBeat(request *WorkerHeartBeatRequest, response *WorkerHeartBeatResponse) error {
	mutex.Lock()
	response.MasterState = m.masterState
	workerID := request.WorkerID
	worker := m.workerMap[workerID]
	worker.workerID = workerID
	worker.workerState = WorkerBusy
	worker.timestamp = time.Now().Second()
	m.workerMap[workerID] = worker
	mutex.Unlock()
	return nil
}

func isPresentIntmFile(files []string,fileName string) bool {
	for _,val := range files {
			if strings.EqualFold(fileName,val){
					return true
			}
	}
	return false
}

//SuccessFromWorker ...
func (m *Master) SuccessFromWorker(request *SuccessRequest, response *SuccessResponse) error {
	mutex.Lock()
	workerID := request.WorkerID
	response.MasterState = m.masterState

	intmFiles := request.Files

	if m.masterState == MasterCompleted {
		mutex.Unlock()
		return nil
	}
	
	if m.masterState == MapInProgres {
		for i := 0; i < len(intmFiles); i++ {
			if !isPresentIntmFile(m.reduceFiles[i],intmFiles[i]){
				m.reduceFiles[i] = append(m.reduceFiles[i], intmFiles[i])
			}
		}
	}

	worker := m.workerMap[workerID]
	taskNum := request.TaskNum
	taskList := []Task{}
	for _, t := range worker.task {
		if t.taskNum == taskNum {
			t.taskState = TaskCompleted
		}
		taskList = append(taskList,t)
	}

	worker.task = taskList

	m.workerMap[workerID] = worker

	if len(m.mapFiles) == 0 && len(m.failedTasks) == 0 && m.masterState == MapInProgres {
		isMapCompleted := true
		for _, val := range m.workerMap {
			if val.workerState == WorkerFailed {
				continue
			}
			breakFlag := false
			for _, t := range val.task {
				if t.taskType == MapType && t.taskState != TaskCompleted {
					isMapCompleted = false
					breakFlag = true
					break
				}
			}
			if breakFlag {
				break
			}
		}
		if isMapCompleted {
			m.masterState = ReduceInProgress
		}
	}
	if len(m.reduceFiles) == 0 && len(m.failedTasks) == 0 {
		isReduceCompleted := true
		for _, val := range m.workerMap {
			if val.workerState == WorkerFailed {
				continue
			}
			breakFlag := false
			for _, t := range val.task {
				if t.taskType == ReduceType && t.taskState != TaskCompleted {
					isReduceCompleted = false
					breakFlag = true
					break
				}
			}
			if breakFlag {
				break
			}
		}
		if isReduceCompleted {
			m.masterState = MasterCompleted
		}
	}
	response.MasterState = m.masterState
	mutex.Unlock()
	return nil
}

func addWorker(m *Master, workerID string, task Task) {
	if val, ok := m.workerMap[workerID]; ok {
		val.workerState = WorkerBusy
		val.timestamp = time.Now().Second()
		val.task = append(val.task, task)
		m.workerMap[workerID] = val
		return
	}

	worker := workerMachine{
		workerID:    workerID,
		workerState: WorkerBusy,
		timestamp:   time.Now().Second(),
		task:        []Task{},
	}
	worker.task = append(worker.task, task)
	m.workerMap[workerID] = worker
}
/*
func isFailedTaskPresent(failedTasks []Task,task Task) bool{
	for _,val := range failedTasks {
		if val.taskNum == task.taskNum {
			return true
		}
	}
	return false
}
*/
func checkWorkers(m *Master) {
	for {
		mutex.Lock()
		if m.masterState == MasterCompleted {
			mutex.Unlock()
			break
		}
		failedWorkers := []string{}
		for key, worker := range m.workerMap {
			currentTime := time.Now().Second()
			if worker.workerState != WorkerFailed && currentTime-worker.timestamp > 10 {
				failedWorkers = append(failedWorkers, key)
			}
		}
		for _, wID := range failedWorkers {
			worker := m.workerMap[wID]
			worker.workerState = WorkerFailed
			workerTask := worker.task
			for _, t := range workerTask {
				if (t.taskType == MapType && m.masterState == MapInProgres && t.taskState != TaskIdle) ||
                   (t.taskType == ReduceType && m.masterState == ReduceInProgress && t.taskState == TaskRunning){
					t.taskState = TaskIdle
					m.failedTasks = append(m.failedTasks, t)
				}
				
			}
			worker.task = []Task{}
			m.workerMap[wID] = worker
		}
		mutex.Unlock()
		time.Sleep(10 * time.Second)
	}
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//Done method to check whether the master has completed or not
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	if m.masterState == MasterCompleted {
		fmt.Println("master is done completing the map reduce task")
		ret = true
	}
	return ret
}

// MakeMaster -> create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	numInitialMapFile = len(files)
	numInitialReduceFiles = nReduce
	m := Master{
		masterState: MapInProgres,
		failedTasks: []Task{},
		mapFiles:    files,
		reduceFiles: [][]string{},
		workerMap:   make(map[string]workerMachine),
	}

	for i := 0; i < nReduce; i++ {
		m.reduceFiles = append(m.reduceFiles, []string{})
	}

	m.server()
	defer checkWorkers(&m)
	return &m
}
