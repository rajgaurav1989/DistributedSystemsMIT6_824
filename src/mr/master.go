package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

const (
	//MapType represent tasks doing the mapping
	MapType = iota
	//ReduceType represent tasks doing the reduction
	ReduceType
)

const (
	//TaskIdle represent task in idle state
	TaskIdle = iota
	//InProgress represent task is running
	InProgress
	//Completed means task is done
	Completed
)

const (
	//WorkerIdle means worker in idle state
	WorkerIdle = iota
	//WorkerFailure means worker has failed
	WorkerFailure
	//WorkerBusy means worker is running
	WorkerBusy
	//WorkerSuccess means worker has successfully executed
	WorkerSuccess
)

const (
	//MasterIdle -> Master in idle state
	MasterIdle = iota
	//MapInProgess -> Master running map jobs now
	MapInProgess
	//ReduceInProgress -> Master running reduce jobs now
	ReduceInProgress
	//MasterCompleted -> Master has successfully completed the map reduce task
	MasterCompleted
)

//Task define the jobs running on worker machines
type Task struct {
	taskType          int
	taskState         int
	machineID         string
	intmFileLocations []string
}

//WorkerMachine -> to describe workers
type WorkerMachine struct {
	machineID   string
	workerState int
}

//Master -> description of the Master machine
type Master struct {
	taskList    []Task
	masterState int
	workers     []WorkerMachine
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	m.server()
	return &m
}
