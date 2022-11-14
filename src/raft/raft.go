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

type LogType struct {
	Logs []LogEntry // Logs[0] is the LastIncludeLog,so Logs[0].Term is LastIncludeTerm
	Base int        // Base is LastIncludeIndex
}

func (l *LogType) lastIndex() int {
	return l.Base + len(l.Logs) - 1
}

func (l *LogType) index(index int) LogEntry {
	if index > l.lastIndex() {
		panic("index is more than lastIndex.")
	} else if index < l.Base {
		panic("index is not more than Base.")
	} else if index == l.Base {
		return LogEntry{Term: l.Logs[0].Term, Command: nil}
	}

	return l.Logs[index-l.Base]
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
	log         LogType
	commitIndex int
	lastApplied int

	applyCh  chan ApplyMsg
	snapshot []byte
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
	var logs LogType
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

func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()

	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

func (rf *Raft) applyEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 在这个过程中可能会出现锁被抢占，导致部分日志被快照处理了，
	// 那么可能会出现索引找不到对应的日志的问题
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log.index(i).Command,
			CommandIndex: i,
		}

		rf.mu.Unlock()
		// 由于上层的service会调用snapshot会抢占锁，所以这里我们需要释放锁，避免造成死锁问题
		rf.applyCh <- applyMsg
		rf.mu.Lock()

		rf.lastApplied += 1

		DPrintf("[applyEntries] raft %v applied entry, lastApplied: %v, commitIndex: %v command: %v\n", rf.me, rf.lastApplied, rf.commitIndex, applyMsg.Command)
	}
}

// checkN check have a half of peers
func (rf *Raft) checkN() {
	for N := rf.log.lastIndex(); N > rf.commitIndex && rf.log.Logs[N-rf.log.Base].Term == rf.currentTerm; N-- {
		nReplicated := 0
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= N && rf.log.Logs[N-rf.log.Base].Term == rf.currentTerm {
				nReplicated += 1
			}
			if nReplicated > len(rf.peers)/2 {
				rf.commitIndex = N
				DPrintf("[CheckN] raft %v start to checkN, rf.commitIndex: %v, rf.lastApplied: %v\n", rf.me, rf.commitIndex, rf.lastApplied)
				go rf.applyEntries()
				break
			}
		}
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
		rf.log.Logs = append(rf.log.Logs, LogEntry{rf.currentTerm, command})
		index = rf.log.lastIndex()
		rf.persist()
		rf.matchIndex[rf.me] = rf.log.lastIndex()
		DPrintf("[Start] raft %v replicate log,state: %v,currentTerm:%v,index:%v,command: %v, logs: %v\n",
			rf.me, rf.state, rf.currentTerm, index, command, rf.log.Logs)
		rf.mu.Unlock()
	}

	return index, term, isLeader
}

func (rf *Raft) resetChannel() {
	rf.timerElectionChan = make(chan bool)
	rf.timerHeartbeatChan = make(chan bool)
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

	// initialize from state persisted before a crash
	rf.log.Logs = append(rf.log.Logs, LogEntry{Term: 0, Command: nil})
	rf.log.Base = 0
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follow
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.timeoutHeartbeat = 100
	rf.resetTimerElection()
	rf.resetTimerHeartbeat()
	rf.resetChannel()
	rf.commitIndex = 0
	rf.lastApplied = rf.log.Base
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	DPrintf("Starting a raft: %v\n", rf.me)
	// start ticker goroutine to start elections
	// 开始一个定时器开始进行选举
	go rf.ticker()

	// Start two goroutine listen election's time & heartbeat's time
	go rf.timerElection()
	go rf.timerHeartbeat()

	return rf
}
