package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var m_Nmap int
var m_Nreduce int
var workerID int
var intermediate []KeyValue

var cout int

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var ping HeartBeatPing

//
// main/mrworker.go calls this function.
//@

func HandleTask(TYPE int, ID int) {
	//fmt.Printf("Get the task to Handle %v %v\n", TYPE, ID)
	if TYPE == 1 {
		//map task
		filename := "mr-inter-" + strconv.Itoa(ID) + ".txt"
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal("open file %v", err)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatal("read file error %v", err)

		}
		originFile := tempToorigin[filename]
		fmt.Printf("the originFileName is %v\n", originFile)
		kv := mapf(originFile, string(content))
		intermediate = []KeyValue{}
		intermediate = append(intermediate, kv...)

		tasks := make([][]KeyValue, m_Nreduce)
		for i := 0; i < m_Nmap; i++ {
			tasks[i] = []KeyValue{}
		}

		for _, kv := range intermediate {
			kkk := kv.Key
			tasks[ihash(kkk)%m_Nreduce] = append(tasks[ihash(kkk)%m_Nreduce], kv)
		}
		for i, v := range tasks {
			Encoder(v, ID, i)
		}

	} else if TYPE == 2 {
		// reduce task
		intermediate = []KeyValue{}

		for i := 0; i < 8; i++ {
			filename := "mr-inter-" + strconv.Itoa(i) + "-" + strconv.Itoa(ID) + ".txt"
			Decode(filename)
		}
		sort.Sort(ByKey(intermediate))
		DoOutput(ID)

	}
}
func Encoder(iii []KeyValue, mapid int, reduceid int) string {
	oname := "mr-inter-" + strconv.Itoa(mapid) + "-" + strconv.Itoa(reduceid) + ".txt"
	ofile, _ := os.Create(oname)
	enc := json.NewEncoder(ofile)
	for _, v := range iii {
		err := enc.Encode(&v)
		if err != nil {
			fmt.Print("\n,Encode error\n")
		}
	}
	return oname
}
func SendAck(t_TYPE int, t_ID int) HeartBeatPing {
	ping := HeartBeatPing{t_TYPE, t_ID}
	return ping
}

var mapf func(string, string) []KeyValue
var reducef func(string, []string) string

func Worker(t_mapf func(string, string) []KeyValue,
	t_reducef func(string, []string) string) {
	//hasHulue := false
	arg := SayHello{}
	reply := SayHelloReply{}
	call("Master.SayHelloRpc", &arg, &reply)
	m_Nmap = reply.NMap
	m_Nreduce = reply.NReduce
	fmt.Printf("the map and reduce is %v %v\n", m_Nmap, m_Nreduce)
	mapf = t_mapf
	reducef = t_reducef

	for {
		pong := HeartBeatPong{}

		call("Master.HeartBeatRpc", &ping, &pong)
		if pong.TYPE == 7 {
			os.Exit(1)
		}
		HandleTask(pong.TYPE, pong.ID)

		ping = SendAck(pong.TYPE, pong.ID)

		time.Sleep(time.Millisecond * 400)
		// handle pong

	}

}

func Decode(fileName string) {

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal("open intermediate file fail")
	}
	//fmt.Print("******* worker.go / 75 line DoReduce *******")
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		intermediate = append(intermediate, kv)
	}
	sort.Sort(ByKey(intermediate))
}

func DoOutput(reduceId int) {
	oname := "mr-out-" + strconv.Itoa(reduceId)
	ofile, _ := os.Create(oname)

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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

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
