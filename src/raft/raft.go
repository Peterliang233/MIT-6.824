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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
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
	Term    int
	Command interface{}
}

const (
	Follow    = 1
	Candidate = 2
	Leader    = 3
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
	voteFor     int
	getVoteNum  int

	timerElectionChan       chan bool
	timerHeartbeatChan      chan bool
	lastResetElectionTimer  int64
	lastResetHeartbeatTimer int64
	timeoutHeartbeat        int64
	timeoutElection         int64

	nextIndex   []int
	matchIndex  []int
	log         []LogEntry
	commitIndex int
	lastApplied int

	applyCh chan ApplyMsg
}

// timeout start to election
func (rf *Raft) timerElection() {
	for {
		rf.mu.Lock()

		if rf.state != Leader && !rf.killed() {
			timeElapsed := (time.Now().UnixNano() - rf.lastResetElectionTimer) / time.Hour.Milliseconds()
			if timeElapsed > rf.timeoutElection {
				// 超时执行选举
				DPrintf("[timerElection] server %v timeout, start to elect, state: %v, currentTerm: %v\n", rf.me, rf.state, rf.currentTerm)
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
		if rf.state == Leader && !rf.killed() {
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
	rf.timeoutElection = rf.timeoutHeartbeat*5 + rand.Int63n(150)
	rf.lastResetElectionTimer = time.Now().UnixNano()
	DPrintf("[resetTimerElection] reset raft %v Election time: %vms", rf.me, rf.timeoutElection)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil {
		DPrintf("[readPersist] decode the raft %v persist data error\n", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.log = logs
	}
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

func (rf *Raft) applyEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}

		rf.applyCh <- applyMsg

		rf.lastApplied += 1

		DPrintf("[applyEntries] raft %v applied entry, lastApplied: %v, commitIndex: %v\n", rf.me, rf.lastApplied, rf.commitIndex)
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[AppendEntries] raft %v received raft %v appendEntries\n", rf.me, args.LeaderId)
	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("[AppendEntries] server %v reject %v append\n", rf.me, args.LeaderId)
		return
	}

	rf.resetTimerElection()

	if args.Term > rf.currentTerm {
		rf.convertTo(Follow)
		rf.currentTerm = args.Term
		rf.persist()
		DPrintf("[AppendEntries] raft %v term is less than raft %v, args.Term: %v, rf.currentTerm: %v\n",
			rf.me, args.LeaderId, args.Term, rf.currentTerm)
	} else if rf.state == Candidate {
		rf.state = Follow
		DPrintf("[AppendEntries] raft %v convert from Candidate to Follow\n", rf.me)
	}

	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	// 需要比较前一条日志的索引和term，需要保证有这个索引和term都可以对应上，这样保证了所有的日志都是同步了
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[AppendEntries] server %v reject %v append,prevLogIndex and prevLogTerm conflict,"+
			" len(rf.log): %v, args.PrevLogIndex: %v, args.PrevLogTerm: %v\n",
			rf.me, args.LeaderId, len(rf.log), args.PrevLogIndex, args.PrevLogTerm)
		reply.Term = rf.currentTerm
		reply.Success = false

		// 以下操作是为了尽可能尽快找到矛盾点
		if len(rf.log) <= args.PrevLogIndex {
			reply.ConflictIndex = len(rf.log)
		} else {
			// 无需一步一步的递减索引，可以直接找到当前term的最开始的索引进行重试，虽然不一定是刚好的矛盾点，但是却可以大幅度缩短重试次数
			for i := args.PrevLogIndex; i >= 0; i-- {
				if rf.log[i].Term != rf.log[i-1].Term {
					reply.ConflictIndex = i
					break
				}
			}
		}

		return
	}

	// If an existing entry conflict with a new one(same index but different terms), delete the existing entry and all that follow it
	// 找到最开始矛盾的点的位置
	isMatch := true
	nextIndex := args.PrevLogIndex + 1
	conflictIndex := 0
	logLen := len(rf.log)
	entryLen := len(args.Entries)
	for i := 0; isMatch && i < entryLen; i++ {
		if ((logLen - 1) < (nextIndex + i)) || rf.log[nextIndex+i].Term != args.Entries[i].Term {
			isMatch = false
			conflictIndex = i
			break
		}
	}
	// Append any new entries not already in the log.
	if !isMatch {
		// [0, nextIndex + conflictIndex) + [conflictIndex,len(entries)-1)
		// 前面不矛盾的加上后面矛盾的就是当前的最新的日志
		rf.log = append(rf.log[:nextIndex+conflictIndex], args.Entries[conflictIndex:]...)
		rf.persist()
		DPrintf("[AppendEntries] raft %v append entries from leader\n", rf.me)
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	lastNewEntryIndex := args.PrevLogIndex + entryLen
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, lastNewEntryIndex)
		go rf.applyEntries()
	}

	if args.Term > rf.currentTerm || rf.state != Follow {
		rf.convertTo(Follow)
		rf.currentTerm = args.Term
		rf.persist()
		DPrintf("[AppendEntries] server %v accept %v append\n", rf.me, args.LeaderId)
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(serverId int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("[sendAppendEntries] raft %v send appendEntries to raft %v\n", rf.me, serverId)
	return rf.peers[serverId].Call("Raft.AppendEntries", args, reply)
}

// checkN check have a half of peers
func (rf *Raft) checkN() {
	for N := len(rf.log) - 1; N > rf.commitIndex && rf.log[N].Term == rf.currentTerm; N-- {
		nReplicated := 0
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= N && rf.log[N].Term == rf.currentTerm {
				nReplicated += 1
			}
			if nReplicated > len(rf.peers)/2 {
				rf.commitIndex = N
				DPrintf("[CheckN] raft %v start to checkN, rf.commitIndex: %v\n", rf.me, rf.commitIndex)
				go rf.applyEntries()
				break
			}
		}
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前这个节点的任期Id
	VoteGranted bool // 为true说明获得了票数
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, onceLeader *sync.Once) {
	DPrintf("[sendRequestVote] raft %v start sendRequestVote to raft %v\n", rf.me, server)
	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); !ok {
		DPrintf("[sendRequestVote] raft %v call raft %v fail\n", rf.me, server)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 说明请求接收方的term大于自己的term，自己的term不是最新的term,需要逐步同步
	if reply.Term > rf.currentTerm {
		rf.convertTo(Follow)
		rf.currentTerm = reply.Term
		rf.persist()
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
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = 0
			}
			rf.mu.Unlock()
			go rf.broadcastHeartbeat()
			rf.mu.Lock()
		})
	}

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
		rf.persist()
	}
	// Your code here (2A, 2B).
	if rf.currentTerm > args.Term || (rf.voteFor != -1 && rf.voteFor != args.CandidateId) {
		DPrintf("[RequestVote] raft %v reject vote for %v, state: %v, currentTerm: %v, args.Term:%v\n", rf.me, args.CandidateId,
			rf.state, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	lastLogIndex := len(rf.log) - 1
	// 拒绝投票的情况如下
	// 1、最新的term大于请求方的最新的term（小于的情况在这之前讨论过了）
	// 2、在term相等的情况下，日志的最大索引大于请求方的最大的日志索引
	// 3、以上，说明所有的情况都讨论完成了
	isLatest := rf.log[lastLogIndex].Term > args.LastLogTerm ||
		(rf.log[lastLogIndex].Term == args.LastLogTerm && args.LastLogIndex < lastLogIndex)

	// 如果自身的状态比请求的节点的日志更新，那么就拒绝投票
	if isLatest {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 这个地方需要保证发起请求的term大于自己的term，或者在term相等的情况下，日志的最大索引大于自身的日志的最大索引。
	// 这里需要注意在同一个term下面可能出现多条日志。
	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.voteFor = args.CandidateId
		rf.persist()
		rf.resetTimerElection()
		// 给自己的channel发送一个信息提示投票了
		DPrintf("[RequestVote] server %v success vote for %v\n", rf.me, args.CandidateId)
	}
}

func (rf *Raft) convertTo(state int) {
	switch state {
	case Follow:
		rf.voteFor = -1
		rf.state = Follow
		rf.persist()
	case Candidate:
		rf.state = Candidate
		rf.currentTerm++
		rf.voteFor = rf.me
		rf.persist()
		rf.resetTimerElection()
	case Leader:
		rf.state = Leader
		rf.resetTimerHeartbeat()
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
	if term, isLeader = rf.GetState(); isLeader {
		rf.mu.Lock()
		rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
		rf.persist()
		rf.matchIndex[rf.me] = rf.nextIndex[rf.me] + len(rf.log) - 1
		index = len(rf.log) - 1
		DPrintf("[Start] raft %v replicate log,state: %v,currentTerm:%v,index:%v\n", rf.me, rf.state, rf.currentTerm, index)
		rf.mu.Unlock()
	}

	return index, term, isLeader
}

func (rf *Raft) broadcastHeartbeat() {
	DPrintf("[broadcastHeartbeat] server %v start to broadcastHeartbeat\n", rf.me)
	rf.mu.Lock()
	state := rf.state
	if state != Leader {
		DPrintf("[broadcastHeartbeat] server %v not leader,cannot broadcast heartBeat.\n", rf.me)
		rf.mu.Unlock()
		return
	}

	rf.resetTimerHeartbeat()
	rf.mu.Unlock()

	// 给其他每个节点发送一个心跳,证明Leader自己是还存活的,以及更新日志
	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
			retry:
				rf.mu.Lock()
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
					Entries:      rf.log[rf.nextIndex[i]:],
					LeaderCommit: rf.commitIndex,
				}
				reply := &AppendEntriesReply{}
				rf.mu.Unlock()
				if rf.sendAppendEntries(i, args, reply) {
					rf.mu.Lock()
					if rf.state != Leader {
						DPrintf("[broadcastHeartbeat] lost leadership: state:%v, term:%v\n", rf.state, rf.currentTerm)
						rf.mu.Unlock()
						return
					}
					if rf.currentTerm != args.Term {
						DPrintf("[broadcastHeartbeat] raft %v node term inconsistency: currentTerm:%v, args.Term:%v, state: %v\n", rf.me, rf.currentTerm,
							args.Term, rf.state)
						rf.mu.Unlock()
						return
					}
					if reply.Success {
						DPrintf("[broadcastHeartbeat] success, server %v received %v entries\n", i, rf.me)
						rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[i] = rf.matchIndex[i] + 1
						rf.checkN()
					} else {
						DPrintf("[broadcastHeartbeat] fail, state: %v, term: %v\n", rf.state, rf.currentTerm)
						if reply.Term > rf.currentTerm {
							rf.convertTo(Follow)
							rf.currentTerm = reply.Term
							rf.persist()
							rf.mu.Unlock()
							return
						}

						rf.nextIndex[i] = reply.ConflictIndex
						DPrintf("[broadcastHeartbeat] raft %v append entries to %v reject: decrement nextIndex and retry\n", rf.me, i)
						rf.mu.Unlock()
						goto retry
					}
					rf.mu.Unlock()
				} else {
					DPrintf("[broadcastHeartbeat] raft %v call raft %v Raft.AppendEntries failed\n", rf.me, i)
				}
			}(i)
		}
	}
}

func (rf *Raft) resetChannel() {
	rf.timerElectionChan = make(chan bool)
	rf.timerHeartbeatChan = make(chan bool)
	rf.applyCh = make(chan ApplyMsg)
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.convertTo(Candidate)

	rf.getVoteNum = 1

	DPrintf("[startElection] raft server %v start election,state: %v, currentTerm:%v\n", rf.me, rf.state, rf.currentTerm)

	rf.mu.Unlock()

	var onceLeader = &sync.Once{}

	for i := range rf.peers {
		if i != rf.me {
			rf.mu.Lock()
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			reply := &RequestVoteReply{}
			rf.mu.Unlock()
			go rf.sendRequestVote(i, args, reply, onceLeader)
		}
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
	for {
		if rf.killed() {
			DPrintf("[ticker] raft %v got a stop signal.\n", rf.me)
			break
		}
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.timerHeartbeatChan:
			rf.broadcastHeartbeat()
		case <-rf.timerElectionChan:
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
	rf.timeoutHeartbeat = 100
	rf.resetTimerElection()
	rf.resetTimerHeartbeat()
	rf.resetChannel()
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("Starting a raft: %v\n", rf.me)
	// start ticker goroutine to start elections
	// 开始一个定时器开始进行选举
	go rf.ticker()

	// Start two goroutine listen election's time & heartbeat's time
	go rf.timerElection()
	go rf.timerHeartbeat()

	return rf
}
