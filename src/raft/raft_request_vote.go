package raft

import "sync"

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
				rf.nextIndex[i] = rf.log.lastIndex() + 1
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

	lastLogIndex := rf.log.lastIndex()
	// 拒绝投票的情况如下
	// 1、最新的term大于请求方的最新的term（小于的情况在这之前讨论过了）
	// 2、在term相等的情况下，日志的最大索引大于请求方的最大的日志索引
	// 3、以上，说明所有的情况都讨论完成了
	isLatest := rf.log.index(lastLogIndex).Term > args.LastLogTerm ||
		(rf.log.index(lastLogIndex).Term == args.LastLogTerm && args.LastLogIndex < lastLogIndex)

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
				LastLogIndex: rf.log.lastIndex(),
				LastLogTerm:  rf.log.lastTerm(),
			}
			reply := &RequestVoteReply{}
			rf.mu.Unlock()
			go rf.sendRequestVote(i, args, reply, onceLeader)
		}
	}

}
