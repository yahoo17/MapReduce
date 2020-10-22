package mr

import (
	"container/list"
	"fmt"
	"io"
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var m1 sync.Mutex

type Master struct {
	MapTask    map[int]int // key: MapTaskid value: state
	ReduceTask map[int]int //

	MapWorkQueue    list.List
	ReduceWorkQueue list.List

	m_mapdone    bool
	m_reducedone bool

	m_nReduce int
	m_nMap    int
}

func (m *Master) SayHelloRpc(hello *SayHello, reply *SayHelloReply) error {
	reply.NReduce = m.m_nReduce
	reply.NMap = m.m_nMap
	temp := "mr-inter-" + strconv.Itoa(hello.ID) + ".txt"
	reply.OriginFileName = tempToorigin[temp]
	return nil

}
func (m *Master) HandleTimeOut(TaskType int, ID int) {
	go m.AddToQueue(TaskType, ID)
}
func (m *Master) AddToQueue(TaskType int, ID int) {
	time.Sleep(time.Second * 10)
	if TaskType == 1 {
		if m.MapTask[ID] != 2 {
			m.MapWorkQueue.PushBack(ID)
		}
	} else if TaskType == 2 {
		if m.ReduceTask[ID] != 2 {
			m.ReduceWorkQueue.PushBack(ID)
		}
	}

}
func (m *Master) AssignTask() (TYPE int, ID int) {
	if m.m_mapdone == false {

		if m.MapWorkQueue.Len() == 0 {
			//fmt.Printf("the mapQueue empty ,please wait for map done \n")
			return 0, 0
		}
		m1.Lock()
		task := m.MapWorkQueue.Front().Value
		m.MapWorkQueue.Remove(m.MapWorkQueue.Front())
		m.MapTask[task.(int)] = 1
		TYPE = 1
		ID = task.(int)
		m.HandleTimeOut(TYPE, ID)
		m1.Unlock()
		return TYPE, ID

	} else if m.m_mapdone == true && m.m_reducedone == false {
		if m.ReduceWorkQueue.Len() == 0 {
			//fmt.Printf("the reduceQueue empty ,please wait for reduce done\n")
			return 0, 0
		}
		m1.Lock()
		task := m.ReduceWorkQueue.Front().Value
		m.ReduceWorkQueue.Remove(m.ReduceWorkQueue.Front())
		//fmt.Printf("task is: %v\n",task )
		//fmt.Printf("task.(int) is: %v\n",task.(int) )
		//fmt.Printf("m.ReduceTask is %v\n",m.ReduceTask[0] )
		m.ReduceTask[task.(int)] = 1
		TYPE = 2
		ID = task.(int)
		m.HandleTimeOut(TYPE, ID)
		m1.Unlock()
		//fmt.Printf("return value is %v,%v",TYPE,ID)
		return TYPE, ID

	} else {
		return 7, 0
	}

}

func (m *Master) HeartBeatRpc(ping *HeartBeatPing, pong *HeartBeatPong) error {

	if ping.TYPE == 1 {
		m.MapTask[ping.ID] = 2
		//fmt.Printf("TYPE %v ID %v Done \n", ping.TYPE, ping.ID)
	} else if ping.TYPE == 2 {
		//fmt.Printf("TYPE %v ID %v Done \n", ping.TYPE, ping.ID)
		m.ReduceTask[ping.ID] = 2
	}
	//________________-Handle ASSIGN TASK_____________________
	pong.TYPE, pong.ID = m.AssignTask()
	return nil
}

func (m *Master) Daemon() {
	for {
		//fmt.Printf("MapQueue is:\n")
		//fmt.Print(m.MapWorkQueue)
		//fmt.Printf("Map Task is:\n")
		//fmt.Print(m.MapTask)
		//fmt.Print("\n")
		if m.m_mapdone == false {
			if m.MapDone() {
				m.m_mapdone = true
				fmt.Print("Map Done (by daemon) \n")

			}
		}
		if m.m_reducedone == false {
			if m.ReduceDone() {
				m.m_reducedone = true
				fmt.Print("Reduce Done (by daemon) \n")
			}
		}
		time.Sleep(time.Second)
	}

}

//___________________________________________Not Need to Modify____________________________________________________________
func (m *Master) MapDone() bool {
	res := true
	for _, v := range m.MapTask {
		if v == 0 || v == 1 {
			return false
		}
	}
	return res
}
func (m *Master) ReduceDone() bool {
	res := true
	for _, v := range m.ReduceTask {
		if v == 0 || v == 1 {
			return false
		}
	}
	return res

}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//

func (m *Master) Done() bool {

	return m.MapDone() && m.ReduceDone()
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
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func (m *Master) MapReduceSplit(TaskType int, n int) {
	if TaskType == 1 {
		m.MapTask = make(map[int]int)
		m.m_mapdone = false
		for i := 0; i < n; i++ {
			m.MapWorkQueue.PushBack(i)

			m.MapTask[i] = 0

		}
		fmt.Printf("MapWorkQueue len is %v\n", m.MapWorkQueue.Len())

	} else if TaskType == 2 {
		m.ReduceTask = make(map[int]int)
		m.m_reducedone = false
		for i := 0; i < n; i++ {
			m.ReduceWorkQueue.PushBack(i)

			m.ReduceTask[i] = 0
		}
	} else {
		fmt.Printf("MapReduceSplit error\n")
	}
}

var tempToorigin map[string]string

func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// Your code here.
	tempToorigin = make(map[string]string)
	for i, v := range files {

		filename := "mr-inter-" + strconv.Itoa(i) + ".txt"
		tempToorigin[filename] = v
		//fmt.Printf("the file name is %v  %v\n", filename,v)
		ofile, err := os.Create(filename)
		if err != nil {
			log.Fatal("open file error\n")

		}
		srcfile, _ := os.Open(v)

		io.Copy(ofile, srcfile)

	}
	nMap := len(files)
	m.m_nMap = nMap
	m.m_nReduce = nReduce
	m.MapReduceSplit(1, nMap)
	m.MapReduceSplit(2, nReduce)
	m.server()
	go m.Daemon()
	return &m
}

//_________________Not Need to Modify___________________________
