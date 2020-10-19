package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var m1 sync.Mutex
var workerId = 1

type Master struct {
	// Your definitions here.
	FileDone map[string]int
	// 0 not begin 1 not finish 2 finish
	FileToWorker map[string]int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) AskForFileRpc(args *MRArgs, reply *MRReply) error {

	if args.AskForFile == 1 {
		m1.Lock()
		reply.WorkerId = workerId
		workerId++
		for k, v := range m.FileDone {
			if v == 0 {
				m.FileToWorker[k] = workerId
				reply.FileName = k
				v = 1
				break
			}
		}
		m1.Unlock()
	}
	return nil
}
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
	ret := true
	for _, v := range m.FileDone {
		if v == 0 || v == 1 {
			return false
		}
	}
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
	m.FileToWorker = make(map[string]int)
	m.FileDone = make(map[string]int)

	for _, filename := range files {
		m.FileDone[filename] = 0
	}
	fmt.Print(m.FileDone)
	// Your code here.
	m.server()
	return &m
}
