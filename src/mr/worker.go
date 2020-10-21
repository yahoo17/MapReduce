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
	fmt.Printf("Get the task to Handle %v %v\n", TYPE, ID)
}
func SendAck(t_TYPE int, t_ID int) HeartBeatPing {
	ping := HeartBeatPing{t_TYPE, t_ID}
	return ping
}
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	hasHulue := false
	for {
		pong := HeartBeatPong{}
		if ping.ID == 5 && hasHulue == false {
			hasHulue = true
			ping = HeartBeatPing{}
		}
		call("Master.HeartBeatRpc", &ping, &pong)
		if pong.TYPE == 7 {
			os.Exit(1)
		}
		HandleTask(pong.TYPE, pong.ID)

		ping = SendAck(pong.TYPE, pong.ID)

		time.Sleep(time.Millisecond * 400)
		// handle pong

	}
	// Your worker implementation here.
	// uncomment to send the Example RPC to the master.
	//DoMap(mapf)
	//intermediate = []KeyValue{}
	//fmt.Print(intermediate)
	//DoReduce(reducef)

}
func AskForFile() (string, int) {
	args := MapAskArgs{AskForFile: 1}
	reply := MapAskReply{}
	call("Master.MapRpc", &args, &reply)
	workerID = reply.WorkerId
	fmt.Printf("\nMapAskreply.filename :%v, reply.workerID:%v\n", reply.FileName, reply.WorkerId)
	return reply.FileName, reply.MapFinish

}
func DoMap(mapf func(string, string) []KeyValue) {
	fileName, mapfinish := AskForFile()
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
		tasks := make([][]KeyValue, 10)
		for i := 0; i < 10; i++ {
			tasks[i] = []KeyValue{}
		}
		for _, kv := range intermediate {
			kkk := kv.Key
			tasks[ihash(kkk)%10] = append(tasks[ihash(kkk)%10], kv)
		}

		for _, v := range tasks {
			Encoder(v, fileName)
		}
		//Encoder(tasks[0], fileName)
		args := MapDoneRpcArgs{FileName: fileName}
		reply := MapDoneRpcReply{}
		call("Master.PartMapDone", &args, &reply)

	}

	if mapfinish == 0 {
		DoMap(mapf)
	}
}

var gggg = 1

func Encoder(iii []KeyValue, originFileName string) string {
	oname := "mr-temp-" + strconv.Itoa(gggg)
	gggg++
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
func Decode(fileName string) {
	intermediate = []KeyValue{}
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

func AskForReduceFile() (string, int) {
	args := ReduceRpcArgs{AskForReduceFile: 1}
	reply := ReduceRpcReply{}
	call("Master.ReduceRpc", &args, &reply)
	return reply.NeedToReduceFileName, reply.ReduceFinish
}
func DoReduce(reducef func(string, []string) string) {
	for i := 1; i < gggg; i++ {
		name := "mr-temp-" + strconv.Itoa(i)
		Decode(name)
		DoOutput(reducef)
	}
	//fmt.Print("\n Now Begin Do Reduce \n")
	//fileName, reduceFinish := AskForReduceFile()
	//fmt.Print("\n Now Begin Do Reduce \n")
	//if fileName != "" {
	//	fmt.Print("\n Now Begin Do Decode \n")
	//	Decode(fileName)
	//	fmt.Print("\n Now Begin Do DoOutPut \n")
	//	DoOutput(reducef)
	//	fmt.Print("\n Now Begin Do RPC \n")
	//	args := ReduceDoneRpcArgs{ReduceDoneFileName: fileName}
	//	reply := ReduceDoneRpcReply{}
	//	call("Master.PartReduceDone", &args, &reply)
	//}
	//fmt.Print("no file")
	//if reduceFinish == 0 {
	//	DoReduce(reducef)
	//}

}
func DoOutput(reducef func(string, []string) string) {
	oname := "mr-out-" + string(cout)
	cout++
	ofile, _ := os.Create(oname)
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//

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
