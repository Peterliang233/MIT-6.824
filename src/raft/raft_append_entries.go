package raft

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
	DPrintf("[AppendEntries] raft %v debug entries: %v, log: %v, rf.lastIncludeIndex:%v, rf.lastIncludeTerm: %v",
		rf.me, args.Entries, rf.log.Logs, rf.log.Base, rf.log.Logs[0].Term)
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
	if args.PrevLogIndex > rf.log.lastIndex() || rf.log.index(args.PrevLogIndex).Term != args.PrevLogTerm {
		DPrintf("[AppendEntries] server %v reject %v append,prevLogIndex and prevLogTerm conflict,"+
			" rf.lastIndex: %v, args.PrevLogIndex: %v, args.PrevLogTerm: %v\n",
			rf.me, args.LeaderId, rf.log.lastIndex(), args.PrevLogIndex, args.PrevLogTerm)
		reply.Term = rf.currentTerm
		reply.Success = false

		// 以下操作是为了尽可能尽快找到矛盾点
		if rf.log.lastIndex()+1 <= args.PrevLogIndex {
			reply.ConflictIndex = rf.log.lastIndex() + 1
		} else {
			// 无需一步一步的递减索引，可以直接找到当前term的最开始的索引进行重试，虽然不一定是刚好的矛盾点，但是却可以大幅度缩短重试次数
			succ := false
			for i := args.PrevLogIndex; i > rf.log.Base; i-- {
				if rf.log.index(i).Term != rf.log.index(i-1).Term {
					succ = true
					reply.ConflictIndex = i
					break
				}
			}
			if !succ {
				reply.ConflictIndex = rf.log.Base
			}
		}

		return
	}

	// If an existing entry conflict with a new one(same index but different terms), delete the existing entry and all that follow it
	// 找到最开始矛盾的点的位置
	isMatch := true
	nextIndex := args.PrevLogIndex + 1
	conflictIndex := 0
	// 当前节点的所有的日志的长度
	lastIndex := rf.log.lastIndex()
	DPrintf("[Debug] nextIndex: %v, lastIndex: %v", nextIndex, lastIndex)
	entryLen := len(args.Entries)
	for i := 0; isMatch && i < entryLen; i++ {
		// 1、索引下标超过了follow日志的最大索引
		// 2、在相同索引地方的选举周期不一致
		if (lastIndex < (nextIndex + i)) || rf.log.index(nextIndex+i).Term != args.Entries[i].Term {
			isMatch = false
			conflictIndex = i
			break
		}
	}
	// Append any new entries not already in the log.
	if !isMatch {
		// [0, nextIndex + conflictIndex) + [conflictIndex,len(entries)-1)
		// 前面不矛盾的加上后面矛盾的就是当前的最新的日志
		DPrintf("[AppendEntries] raft %v, nextIndex: %v, conflictIndex: %v", rf.me, nextIndex, conflictIndex)
		// 新的日志为[0,index)+[conflictIndex....]
		rf.log.Logs = append(rf.log.Logs[:(nextIndex+conflictIndex-rf.log.Base)], args.Entries[conflictIndex:]...)
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
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	DPrintf("[AppendEntries] server %v accept %v append, now log: %v, lastIndex: %v\n",
		rf.me, args.LeaderId, rf.log.Logs, rf.log.lastIndex())
}

func (rf *Raft) sendAppendEntries(serverId int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("[sendAppendEntries] raft %v send appendEntries to raft %v\n", rf.me, serverId)
	ok := rf.peers[serverId].Call("Raft.AppendEntries", args, reply)
	DPrintf("Debug: raft %v sendAppendEntries %v result: %v\n", rf.me, serverId, ok)
	return ok
}

func (rf *Raft) ReplicationEntries(i int) bool {
	rf.mu.Lock()
	// 2D test, snapshot,if PrevLogIndex < rf.log.LastIncludeIndex
	prevLogIndex := rf.nextIndex[i] - 1
	DPrintf("[ReplicationEntries] snapshot check raft %v, prevLogIndex:%v, lastIncludeIndex: %v", i, prevLogIndex, rf.log.Base)
	rf.mu.Unlock()
	if prevLogIndex < rf.log.Base {
		rf.mu.Lock()
		args := InstallSnapshotArgs{
			Term:             rf.currentTerm,
			LeaderId:         rf.me,
			LastIncludeIndex: rf.log.Base,
			LastIncludedTerm: rf.log.Logs[0].Term,
			Data:             rf.snapshot,
		}
		rf.mu.Unlock()
		reply := InstallSnapshotReply{}
		rf.sendInstallSnapshot(i, &args, &reply)
		return true
	}

	rf.mu.Lock()
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[i] - 1,
		PrevLogTerm:  rf.log.index(rf.nextIndex[i] - 1).Term,
		Entries:      rf.log.Logs[(rf.nextIndex[i] - rf.log.Base):],
		LeaderCommit: rf.commitIndex,
	}
	reply := &AppendEntriesReply{}
	rf.mu.Unlock()
	if rf.sendAppendEntries(i, args, reply) {
		rf.mu.Lock()
		if rf.state != Leader {
			DPrintf("[ReplicationEntries] lost leadership: state:%v, term:%v\n", rf.state, rf.currentTerm)
			rf.mu.Unlock()
			return true
		}
		if rf.currentTerm != args.Term {
			DPrintf("[ReplicationEntries] raft %v node term inconsistency: currentTerm:%v, args.Term:%v, state: %v\n", rf.me, rf.currentTerm,
				args.Term, rf.state)
			rf.mu.Unlock()
			return true
		}
		if reply.Success {
			DPrintf("[ReplicationEntries] success, server %v received %v entries\n", i, rf.me)
			rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[i] = rf.matchIndex[i] + 1
			rf.checkN()
			rf.mu.Unlock()
			return true
		} else {
			DPrintf("[ReplicationEntries] fail, state: %v, term: %v\n", rf.state, rf.currentTerm)
			if reply.Term > rf.currentTerm {
				rf.convertTo(Follow)
				rf.currentTerm = reply.Term
				rf.persist()
				rf.mu.Unlock()
				return true
			}
			rf.nextIndex[i] = reply.ConflictIndex
			DPrintf("[ReplicationEntries] raft %v append entries to %v reject: decrement nextIndex and retry\n", rf.me, i)
			rf.mu.Unlock()
			return false
		}
	} else {
		DPrintf("[ReplicationEntries] raft %v call raft %v Raft.AppendEntries failed\n", rf.me, i)
		return true
	}
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
				for !rf.ReplicationEntries(i) {
				}
			}(i)
		}
	}
}
