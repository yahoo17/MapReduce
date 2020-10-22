package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	m_state         int // 0 follower 1 candidate 2 leader
	electionTimeout time.Duration
	currentTerm     int
	m_nRaft         int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) getRandomElectionTimeout() {
	rf.electionTimeout = time.Millisecond * time.Duration(rand.Int()%150+150)
	//rf.electionTimeout = time.Millisecond *100
	//fmt.Printf("this raft server electionTimeout is %v\n",rf.electionTimeout)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	return rf.currentTerm, rf.m_state == 2

	//var term int
	//var isleader bool
	//// Your code here (2A).
	//
	//return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

type AppendEntryArgs struct {
	CurrentTerm int
}
type AppendEntryReply struct {
	RejectOrAccept int // 0 || 1
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	CurrentTerm int

	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	RejectOrAccept int // 0 reject 1 accept
	// Your data here (2A).
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//fmt.Printf("I am  %v, and args is %v ,and the reply is %v\n",rf.me,args,reply)
	fmt.Printf("%v get the rpc, and the arg is %v,and my is %v\n", rf.me, args.CurrentTerm, rf.currentTerm)

	if args.CurrentTerm > rf.currentTerm {
		reply.RejectOrAccept = 1
		//fmt.Printf("%v accept the leader\n",rf.me)

	} else {
		reply.RejectOrAccept = 0
	}
	// Your code here (2A, 2B).
}
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {

	if args.CurrentTerm >= rf.currentTerm {
		reply.RejectOrAccept = 1
		rf.currentTerm = args.CurrentTerm
	} else {
		reply.RejectOrAccept = 0
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//fmt.Printf("arg is %v\n",args.CurrentTerm)
	temp := args
	ok := rf.peers[server].Call("Raft.RequestVote", temp, reply)
	return ok
}
func (rf *Raft) sendApplyEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}
func (rf *Raft) atomicSetState(state int) {
	rf.mu.Lock()
	rf.m_state = state
	rf.mu.Unlock()
}
func (rf *Raft) beginVote() bool {
	count := 0

	for i := 0; i != rf.me && i < rf.m_nRaft; i++ {

		arg := RequestVoteArgs{CurrentTerm: rf.currentTerm + 1}
		reply := RequestVoteReply{}
		rf.sendRequestVote(i, &arg, &reply)
		if reply.RejectOrAccept == 1 {
			count++
		}
	}
	//fmt.Printf("count is %v, and rf.m_ is %v\n",count,(rf.m_nRaft+1)/2)
	//fmt.Printf("%v get %v vote\n",rf.me,count)
	return count >= (rf.m_nRaft+1)/2

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func (rf *Raft) TimeoutToElection() {
	rf.getRandomElectionTimeout()
	for {
		time.Sleep(rf.electionTimeout)
		if rf.m_state == 0 || rf.m_state == 1 {
			if rf.beginVote() == true {
				rf.atomicSetState(2)
				rf.currentTerm++
				rf.HeartBeatPing()
				fmt.Printf("%v has been the leader\n", rf.me)
				//______________
				for i := 0; i < rf.m_nRaft && i != rf.me; i++ {
					args := AppendEntryArgs{rf.currentTerm}
					reply := AppendEntryReply{}
					rf.sendApplyEntry(i, &args, &reply)

				}
				//________________-
			}
		}

	}

}
func (rf *Raft) HeartBeatPing() {
	for rf.m_state == 2 {
		time.Sleep(time.Millisecond * 300)
		count := 0
		for i := 0; i < rf.m_nRaft && i != rf.me; i++ {
			args := AppendEntryArgs{rf.currentTerm}
			reply := AppendEntryReply{}
			rf.sendApplyEntry(i, &args, &reply)
			count += reply.RejectOrAccept

		}
		if count < (rf.m_nRaft+1)/2 {
			rf.atomicSetState(0)
		}

	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.m_nRaft = len(peers)
	rf.currentTerm = 0
	go rf.TimeoutToElection()
	go rf.PrintSomething()
	//fmt.Print("call the Make func\n")
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
func (rf *Raft) PrintSomething() {
	for {
		time.Sleep(time.Millisecond * 500)
		fmt.Printf("%v state : term:%v is leader :%v\n", rf.me, rf.currentTerm, rf.m_state == 2)
	}
}
