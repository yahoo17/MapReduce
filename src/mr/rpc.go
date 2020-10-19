package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type ReduceDoneRpcArgs struct {
	ReduceDoneFileName string
}
type ReduceDoneRpcReply struct {
	Ack bool
}

//rpcname+Args
//__________________________
type ReduceRpcArgs struct {
	AskForReduceFile int
}
type ReduceRpcReply struct {
	NeedToReduceFileName string
	WorkerId             int
	ReduceFinish         int
}

//___________________________________

//____________________________________
type MapDoneRpcArgs struct {
	FileName string
}
type MapDoneRpcReply struct {
	Ack bool
}

//___________________________

//___________________________
type MapAskArgs struct {
	AskForFile int
}
type MapAskReply struct {
	FileName  string //MapAsk return file name
	WorkerId  int
	MapFinish int
}

//____________________________

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
