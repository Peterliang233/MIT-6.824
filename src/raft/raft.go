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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}


type LogEntry struct {
	Term int
	Command interface{}
}
const (
	Follow = 1
	Candidate = 2
	Leader = 3
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state int

	currentTerm int
	voteFor int
	getVoteNum int

	timerElectionChan chan bool
	timerHeartbeatChan chan bool
	lastResetElectionTimer int64
	lastResetHeartbeatTimer int64
	timeoutHeartbeat int64
	timeoutElection int64
}


// 重置选举超时时间，150ms～300ms
func (rf *Raft) timerElection() {

	for {
		rf.mu.Lock()

		if rf.state != Leader {
			timeElapsed := (time.Now().UnixNano() - rf.lastResetElectionTimer) / time.Hour.Milliseconds()
			if timeElapsed > rf.timeoutElection {
				// 超时执行选举
				DPrintf("server %v timeout, start to elect, state: %v, currentTerm: %v\n", rf.me, rf.state, rf.currentTerm)
				rf.timerElectionChan <- true
			}
		}

		rf.mu.Unlock()

		// 为了保证这些RPC协程都执行完成
		time.Sleep(time.Millisecond * 10)
	}
}

func (rf *Raft) timerHeartbeat() {
	for {
		rf.mu.Lock()

		if rf.state == Leader {
			timeElapsed := (time.Now().UnixNano() - rf.lastResetHeartbeatTimer) / time.Hour.Milliseconds()
			if timeElapsed > rf.timeoutHeartbeat {
				DPrintf("[timerHeartbeat] raft %v heartbeat timeout, start broadcast\n", rf.me)
				rf.timerHeartbeatChan <- true
			}
		}

		rf.mu.Unlock()

		time.Sleep(time.Millisecond * 10)
	}
}

func (rf *Raft) resetTimerElection() {
	rand.Seed(time.Now().UnixNano())
	rf.timeoutElection = rf.timeoutHeartbeat * 5 + rand.Int63n(150)
	rf.lastResetElectionTimer = time.Now().UnixNano()
}


func (rf *Raft) resetTimerHeartbeat() {
	rf.lastResetHeartbeatTimer = time.Now().UnixNano()
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	//rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	}else{
		isleader = false
	}
	//rf.mu.Unlock()
	return term, isleader
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


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}


type AppendEntriesReply struct {
	Term int
	Success bool
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("[AppendEntries] server %v reject %v append\n", rf.me, args.LeaderId)
		return
	}


	rf.resetTimerElection()

	if args.Term > rf.currentTerm || rf.state != Follow {
		rf.convertTo(Follow)
		rf.currentTerm = args.Term
		DPrintf("[AppendEntries] server %v accpet %v append\n", rf.me, args.LeaderId)
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(serverId int,args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok := rf.peers[serverId].Call("Raft.AppendEntries", args, reply); !ok {
		return
	}


	if rf.state != Leader {
		DPrintf("[broadcastHeartbeat] lost leadership: state:%v, term:%v\n", rf.state, rf.currentTerm)
		return
	}

	if rf.currentTerm != args.Term {
		DPrintf("[broadcatHeartbeat] raft %v node term inconsistency: currentTerm:%v, args.Term:%v, state: %v\n",rf.me, rf.currentTerm,
		args.Term, rf.state)
		return
	}


	if reply.Success {
		DPrintf("[appendEntriesAsync] success, server %v received %v entries\n", serverId, rf.me)
		return
	}else{
		DPrintf("[appendEntriesAsync] fail, state: %v, term: %v\n", rf.state, rf.currentTerm)

		if reply.Term > rf.currentTerm {
			rf.convertTo(Follow)
			rf.currentTerm = reply.Term
		}
	}

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int // 当前这个节点的任期Id
	VoteGranted bool  // 为true说明获得了票数
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		// 如果请求的term大于自身的term,说明自身慢于发起请求的这个节点，需要改变为follow节点
		rf.convertTo(Follow)
		rf.currentTerm = args.Term
	}
	// Your code here (2A, 2B).
	if rf.currentTerm > args.Term || (rf.voteFor != -1 && rf.voteFor != args.CandidateId) {
		DPrintf("[RequestVote] raft %v reject vote for %v, state: %v, currentTerm: %v, args.Term:%v\n", rf.me, args.CandidateId,
		rf.state, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}



	// 这个地方需要保证发起请求的term大于自己的term，或者在term相等的情况下，日志的最大索引大于自身的日志的最大索引。
	// 这里需要注意在同一个term下面可能出现多条日志。
	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) {
		reply.VoteGranted = true
		reply.Term = args.Term
		rf.voteFor = args.CandidateId
		rf.persist()
		rf.resetTimerElection()
		// 给自己的channel发送一个信息提示投票了
		DPrintf("[RequestVote] server %v success vote for %v\n", rf.me, args.CandidateId)
	}
}

func (rf *Raft) convertTo(state int){
	switch state {
	case Follow:
		rf.voteFor = -1
		rf.state = Follow
	case Candidate:
		rf.state = Candidate
		rf.currentTerm ++ 
		rf.voteFor = rf.me
		rf.resetTimerElection()
	case Leader:
		rf.state = Leader
		rf.resetTimerHeartbeat()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, onceLeader sync.Once) {
	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); !ok {
		return
	}

	// 说明请求接收方的term大于自己的term，自己的term不是最新的term,需要逐步同步
	if reply.Term > rf.currentTerm {
		rf.convertTo(Follow)
		rf.currentTerm = reply.Term
		return
	}

	if !reply.VoteGranted {
		return
	}

	rf.getVoteNum++

	// 注意这个地方需要保证还是candidate身份
	if rf.getVoteNum > len(rf.peers)/2 && rf.currentTerm == args.Term && rf.state == Candidate {
		// 只执行一次
		onceLeader.Do(func() {
			rf.convertTo(Leader)
			DPrintf("[sendRequestVote] server %v become leader\n", rf.me)
			rf.mu.Lock()
			rf.broadcastHeartbeat()
			rf.mu.Unlock()
		})
	}

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


func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	state := rf.state
	if state != Leader {
		rf.mu.Unlock()
		return
	}

	rf.resetTimerElection()
	rf.mu.Unlock()

	args := &AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
	}

	reply := &AppendEntriesReply{}

	// 给其他每个节点发送一个心跳,证明Leader自己是还存活的
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendAppendEntries(i, args,reply)
		}
	}
}


func (rf *Raft) resetChannel() {
	rf.timerElectionChan = make(chan bool)
	rf.timerHeartbeatChan = make(chan bool)
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.convertTo(Candidate)
	rf.persist()

	rf.getVoteNum = 1

	DPrintf("[startElection] raft server %v start election,state: %v, currentTerm:%v\n", rf.me, rf.state, rf.currentTerm)

	args := &RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
	}

	rf.mu.Unlock()

	reply := &RequestVoteReply{}

	var onceLeader sync.Once

	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(i, args, reply, onceLeader)
		}
	}
	
}

func (rf *Raft) broadcastRequestVote() {
	if rf.state != Candidate {
		return
	}

	args := &RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
	}

	reply := &RequestVoteReply{}

	var onceLeader sync.Once
	
	for i := range rf.peers {
		if i != rf.me {
			// 向其他节点发起投票请求
			go rf.sendRequestVote(i, args, reply, onceLeader)
		}
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	state := rf.state
	rf.mu.Unlock()
	if state != Leader {
		return
	}

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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// 如果这个节点没有停止，那么就一直监听这个节点
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <- rf.timerHeartbeatChan:
			rf.broadcastHeartbeat()
		case <- rf.timerElectionChan:
			rf.startElection()
		}
	}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follow
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.resetTimerElection()
	rf.resetChannel()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// 开始一个定时器开始进行选举
	go rf.ticker()
	go rf.timerElection()
	go rf.timerHeartbeat()

	return rf
}


