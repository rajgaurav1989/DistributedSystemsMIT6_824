package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// AssignTaskRequest -> request for go rpc call AssignTask
type AssignTaskRequest struct {
	WorkerID string
}

// AssignTaskResponse -> response for go rpc call AssignTask
type AssignTaskResponse struct {
	TaskNum     int
	TaskType    int
	Files       []string
	MasterState int
	NumReducers int
}

//WorkerHeartBeatRequest ...
type WorkerHeartBeatRequest struct {
	WorkerID string
}

//WorkerHeartBeatResponse ....
type WorkerHeartBeatResponse struct {
	MasterState int
}

//SuccessRequest ...
type SuccessRequest struct {
	WorkerID string
	Files    []string
	TaskNum  int
}

//SuccessResponse ...
type SuccessResponse struct {
	MasterState int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
