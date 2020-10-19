package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
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

var workerID int
var intermediate []KeyValue

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Encoder(intermediate []KeyValue) {
	oname := "mr-out-"
	oname = oname + string(workerID)
	ofile, _ := os.Create(oname)

	enc := json.NewEncoder(ofile)
	for _, v := range intermediate {
		enc.Encode(&v)
	}

}
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	CallExample()
	fileName := AskForFile()
	if fileName != "" {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal("open file %v", err)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatal("read file error %v", err)

		}
		kv := mapf(fileName, string(content))
		intermediate = append(intermediate, kv...)

	}
	Encoder(intermediate)
	//fmt.Print(intermediate)

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func AskForFile() string {
	args := MRArgs{}

	args.AskForFile = 1

	reply := MRReply{}

	call("Master.AskForFileRpc", &args, &reply)
	workerID = reply.WorkerId

	fmt.Printf("reply.filename :%v\n, reply.workerID:%v", reply.FileName, reply.WorkerId)

	return reply.FileName

}
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
