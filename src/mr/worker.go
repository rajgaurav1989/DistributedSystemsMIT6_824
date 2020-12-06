package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	successStr      = "ASSSSFGGHHHS_SUCCESS_HHGHJGJHBHJINVA"
	fileDeleteError = "FILE_DELETE_ERROR"
)

// KeyValue ...
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//ByKey ...
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker ...
//how to assign map task to worker
//how to assign reduce task to worker
//how to notify workers to start reducing
// Worker ...

var isProblem bool = false

//Worker ...
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	uuid, _ := exec.Command("uuidgen").Output()
	workerID := string(uuid)
	isStarted := false
	for {
		response, error := CallAssignTask(workerID)
		if response.Files == nil || len(response.Files) == 0 {
			continue
		}
		
		if error != nil  {
			return
		}
		if response.MasterState == MasterCompleted || isProblem {
			return
		}
		fmt.Printf("assigned task files %v %v\n",response.Files,response.MasterState)
		if response.TaskType == MapType {
			_, err := performMapTask(workerID, response.Files[0], response.TaskNum, response.NumReducers, mapf)
			if err != nil {
				return
			}
		} else {
			_, err := performReduceTask(workerID, response.Files, response.TaskNum, reducef)
			if err != nil {
				return
			}
		}
		if !isStarted {
			isStarted = true
			go sendHeartBeat(workerID)
		}
		time.Sleep(10 * time.Second)
	}
}

func sendHeartBeat(workerID string) {
	for {
		heartBeatRequest := WorkerHeartBeatRequest{
			WorkerID: workerID,
		}

		heartBeatResponse := WorkerHeartBeatResponse{}

		success := call("Master.WorkerHeartBeat", &heartBeatRequest, &heartBeatResponse)
		if !success {
			isProblem = true

		}
		if heartBeatResponse.MasterState == MasterCompleted {
			break
		}
		time.Sleep(10 * time.Second)
	}
}

func performReduceTask(workerID string, files []string, taskNum int, reducef func(string, []string) string) (int, error) {
	intermediate := []KeyValue{}
	resFile := "mr-out-" + strconv.Itoa(taskNum)
	if _, err := os.Stat(resFile); err != nil {
		if os.IsExist(err) {
			os.Remove(resFile)
		}
	}
	for _, fileItr := range files {
		fileOpener, err := os.Open(fileItr)
		if err != nil {
			return -1, fmt.Errorf("some serious error on worker")
		}
		dec := json.NewDecoder(fileOpener)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		intermediate = intermediate[:len(intermediate)-1]
	}
	sort.Sort(ByKey(intermediate))
	outFile, err := os.Create(resFile)
	if err != nil {
		return -1, fmt.Errorf("some serious error on worker")
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	outFile.Close()

	successRequest := SuccessRequest{
		WorkerID: workerID,
		TaskNum: taskNum,
	}

	successResponse := SuccessResponse{}
	success := call("Master.SuccessFromWorker", &successRequest, &successResponse)
	if !success {
		return -1, fmt.Errorf("some serious error on worker %v", workerID)
	}

	return successResponse.MasterState, nil
}

func performMapTask(workerID string, filename string, taskNum int,
	numReducers int, mapf func(string, string) []KeyValue) (int, error) {
	intmContent := [][]string{}

	for i := 0; i < numReducers; i++ {
		intmContent = append(intmContent, []string{})
	}

	intmFileMap := make(map[string]*json.Encoder)
	existingFileMap := make(map[string]string)
	mapFileNames := []string{}
	for i := 0; i < numReducers; i++ {
		intmFileName := "mr-" + strconv.Itoa(taskNum) + "-" + strconv.Itoa(i)
		mapFileNames = append(mapFileNames,intmFileName)
		//key := strconv.Itoa(taskNum) + "-" + strconv.Itoa(i)
		ok, err := fileExists(intmFileName)
		if !ok && err != nil {
			return -1, fmt.Errorf("some serious error on worker %v", workerID)
		} else if ok {
			existingFileMap[intmFileName] = intmFileName
		} else {
			f, err := os.Create(intmFileName)
			if err != nil {
				return -1, fmt.Errorf("some serious error on worker %v", workerID)
			}
			intmFileMap[intmFileName] = json.NewEncoder(f)
		}
	}

	filewords := getFileContent(filename)
	if strings.EqualFold(filewords, "") {
		return -1, fmt.Errorf("some serious error on worker %v", workerID)
	}
	kva := mapf(filename, filewords)

	for _, intm := range kva {
		fileNamekey :=  "mr-" + strconv.Itoa(taskNum) + "-" + strconv.Itoa(ihash(intm.Key)%numReducers)
		value := KeyValue{
			Key:   intm.Key,
			Value: "1",
		}
		if _, ok := existingFileMap[fileNamekey]; ok == true {
			continue
		}
		intmFileMap[fileNamekey].Encode(&value)
	}

	for _, val := range intmFileMap {
		lastPair := KeyValue{
			Key:   successStr,
			Value: "1",
		}
		val.Encode(&lastPair)
	}

	successRequest := SuccessRequest{
		WorkerID: workerID,
		Files : mapFileNames,
		TaskNum : taskNum,
	}

	successResponse := SuccessResponse{}
	success := call("Master.SuccessFromWorker", &successRequest, &successResponse)
	if !success {
		return -1, fmt.Errorf("some serious error on worker %v", workerID)
	}
	return successResponse.MasterState, nil
}

func fileExists(filename string) (bool, error) {
	if _, err := os.Stat(filename); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
	}

	file, errOpen := os.Open(filename)
	defer file.Close()
	if errOpen != nil {
		err := os.Remove(filename)
		if err != nil {
			return false, fmt.Errorf("error in worker")
		}
		return false, nil
	}

	dec := json.NewDecoder(file)
	isValidFile := false
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		if strings.Contains(kv.Key, successStr) {
			isValidFile = true
		}
	}
	if !isValidFile {
		err := os.Remove(filename)
		if err != nil {
			return false, fmt.Errorf("error in worker")
		}
		return false, nil
	}

	return true, nil
}

func getFileContent(filename string) string {
	file, errOpen := os.Open(filename)
	if errOpen != nil {
		err := os.Remove(filename)
		if err != nil {
			return ""
		}
	}

	contents, errRead := ioutil.ReadAll(file)
	if errRead != nil {
		err := os.Remove(filename)
		if err != nil {
			return ""
		}
	}
	file.Close()

	return string(contents)
}

//CallAssignTask -> to ask the master for task
func CallAssignTask(workerID string) (AssignTaskResponse, error) {

	// declare an argument structure.
	args := AssignTaskRequest{
		WorkerID: workerID,
	}

	reply := AssignTaskResponse{}

	// send the RPC request, wait for the reply.
	success := call("Master.AssignTask", &args, &reply)
	if !success {
		return AssignTaskResponse{}, fmt.Errorf("error in contacting master")
	}
	return reply, nil

}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
