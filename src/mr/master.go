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
	// 0 not begin 1 not finish 2 finish
	MapFileDone     map[string]int
	MapFileToWorker map[string]int

	ReduceFileDone     map[string]int
	ReduceFileToWorker map[string]int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//___________________________________________________________-
func (m *Master) PartMapDone(args *MapDoneRpcArgs, reply *MapDoneRpcReply) error {
	m.MapFileDone[args.FileName] = 2
	temp := args.FileName
	temp = temp + "temp"
	m.ReduceFileDone[temp] = 0
	reply.Ack = true
	return nil
}
func (m *Master) PartReduceDone(args *ReduceDoneRpcArgs, reply *ReduceDoneRpcReply) error {
	m.ReduceFileDone[args.ReduceDoneFileName] = 2
	reply.Ack = true
	return nil
}
func (m *Master) ReduceRpc(args *ReduceRpcArgs, reply *ReduceRpcReply) error {
	if args.AskForReduceFile == 1 {
		m1.Lock()
		reply.WorkerId = workerId
		workerId++
		for k, v := range m.ReduceFileDone {
			if v == 0 {
				m.ReduceFileToWorker[k] = workerId
				reply.NeedToReduceFileName = k
				v = 1
				break
			}
		}
		m1.Unlock()
	}
	if m.ReduceDone() == false {
		reply.ReduceFinish = 0
	}
	return nil

}
func (m *Master) MapRpc(args *MapAskArgs, reply *MapAskReply) error {

	if args.AskForFile == 1 {
		m1.Lock()
		reply.WorkerId = workerId
		workerId++
		for k, v := range m.MapFileDone {
			if v == 0 {
				m.MapFileToWorker[k] = workerId
				reply.FileName = k
				v = 1
				break
			}
		}
		reply.MapFinish = 0
		if m.MapDone() == true {
			reply.MapFinish = 1
		}
		m1.Unlock()
	}
	fmt.Print("\n")
	fmt.Print(m.MapFileDone)
	fmt.Print("\n")
	return nil
}

//__________________________________________
func (m *Master) MapDone() bool {
	res := true
	for _, v := range m.MapFileDone {
		if v == 0 || v == 1 {
			return false
		}
	}
	return res
}
func (m *Master) ReduceDone() bool {
	res := true
	for _, v := range m.ReduceFileDone {
		if v == 0 || v == 1 {
			return false
		}
	}
	return res

}
func (m *Master) Done() bool {
	return m.MapDone() && m.ReduceDone()
}

//_________________Not Need to Modify___________________________
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

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.MapFileToWorker = make(map[string]int)
	m.MapFileDone = make(map[string]int)
	m.ReduceFileDone = make(map[string]int)
	m.ReduceFileToWorker = make(map[string]int)

	for _, filename := range files {
		m.MapFileDone[filename] = 0
	}

	// Your code here.
	m.server()
	return &m
}

//_________________Not Need to Modify___________________________
