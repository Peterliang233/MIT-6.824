package raft

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludedTerm int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	DPrintf("[InstallSnapshot] raft %v start install Snapshot,term: %v, lastIncludeIndex:%v, lastIncludeTerm: %v",
		rf.me, args.Term, args.LastIncludeIndex, args.LastIncludedTerm)

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		DPrintf("[InstallSnapshot] raft %v installsnapshot error, args.Term: %v, rf.currentTerm: %v\n", rf.me, args.Term, rf.currentTerm)
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertTo(Follow)
		rf.currentTerm = args.Term
		rf.persist()
	}

	if args.Term == rf.currentTerm {
		if rf.state == Leader {
			DPrintf("[InstallSnapshot] raft %v now is leader,cannot accept InstallSnapshot", rf.me)
		} else if rf.state == Candidate {
			rf.state = Follow
		}
	}

	if args.LastIncludeIndex <= rf.log.LastIncludeIndex {
		DPrintf("[InstallSnapshot] raft %v fail, args.LastIncludeIndex: %v, rf.log.LastIncludeIndex: %v\n",
			rf.me, args.LastIncludeIndex, rf.log.LastIncludeIndex)
		rf.mu.Unlock()
		return
	}

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludeIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}

	rf.mu.Unlock()

	rf.applyCh <- msg

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply); !ok {
		DPrintf("[sendInstallSnapshot] raft %v failed to send InstallSnapShot to raft %v\n", rf.me, server)
		return
	}

	DPrintf("[sendInstallSnapshot] raft %v sendInstallSnapshot raft %v success", rf.me, server)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		DPrintf("[sendInstallSnapshot] raft %v state is not leader\n", rf.me)
		return
	}

	if reply.Term > rf.currentTerm {
		rf.convertTo(Follow)
		rf.currentTerm = reply.Term
		rf.persist()
		rf.resetTimerElection()
		return
	}

	rf.nextIndex[server] = args.LastIncludeIndex + 1
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	DPrintf("[CondInstallSnapshot] call raft %v CondInstallSnapshot, lastIncludeIndexTerm: %v, lastIncludeIndex: %v\n",
		rf.me, lastIncludedTerm, lastIncludedIndex)

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("[CondInstallSnapshot] raft %v lastIncludedIndex: %v, rf.commitIndex: %v\n", rf.me, lastIncludedIndex, rf.commitIndex)
		return false
	}

	defer func() {
		rf.log.LastIncludeIndex = lastIncludedIndex
		rf.log.LastIncludeTerm = lastIncludedTerm
		rf.snapshot = snapshot
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
		rf.persistStateAndSnapshot(snapshot)
	}()
	index1 := 0
	if rf.log.LastIncludeIndex == 0 {
		index1 = lastIncludedIndex - rf.log.LastIncludeIndex + 1
	} else {
		index1 = lastIncludedIndex - rf.log.LastIncludeIndex
	}

	if lastIncludedIndex <= rf.log.lastIndex() && rf.log.index(lastIncludedIndex).Term == lastIncludedTerm {
		rf.log.Logs = append([]LogEntry(nil), rf.log.Logs[index1:]...)
		rf.mu.Unlock()
		return true
	}

	rf.log.Logs = make([]LogEntry, 0)

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	DPrintf("[Snapshot] call raft %v the Snapshot, index: %v\n", rf.me, index)
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果这个下标小于之前已经快照的下标，说明不能被提交
	// 如果下标超过了这个节点的最后向service的提交下标，同样不能被快照
	if index <= rf.log.LastIncludeIndex || index > rf.lastApplied {
		DPrintf("[Snapshot] raft %v index: %v,LastIncludeIndex: %v\n", rf.me, index, rf.log.LastIncludeIndex)
		return
	}

	index1 := 0
	if rf.log.LastIncludeIndex == 0 {
		index1 = index - rf.log.LastIncludeIndex + 1
	} else {
		index1 = index - rf.log.LastIncludeIndex
	}
	rf.log.LastIncludeTerm = rf.log.index(index).Term
	rf.log.LastIncludeIndex = index
	rf.log.Logs = append([]LogEntry(nil), rf.log.Logs[index1:]...)
	rf.snapshot = snapshot
	rf.persist()
	rf.persistStateAndSnapshot(snapshot)
	DPrintf("[Snapshot] after raft %v snapshot, log: %v", rf.me, rf.log.Logs)
}
