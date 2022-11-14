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
		DPrintf("[InstallSnapshot] raft %v InstallSnapshot error, args.Term: %v, rf.currentTerm: %v\n", rf.me, args.Term, rf.currentTerm)
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

	if args.LastIncludeIndex <= rf.log.Base {
		DPrintf("[InstallSnapshot] raft %v fail, args.LastIncludeIndex: %v, rf.log.LastIncludeIndex: %v\n",
			rf.me, args.LastIncludeIndex, rf.log.Base)
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

	go func() {
		rf.applyCh <- msg
	}()

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
	DPrintf("[CondInstallSnapshot] call raft %v CondInstallSnapshot, lastIncludeIndexTerm: %v, lastIncludeIndex: %v, logs: %v\n",
		rf.me, lastIncludedTerm, lastIncludedIndex, rf.log.Logs)

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("[CondInstallSnapshot] raft %v lastIncludedIndex: %v, rf.commitIndex: %v\n", rf.me, lastIncludedIndex, rf.commitIndex)
		return false
	}

	if lastIncludedIndex <= rf.log.lastIndex() && rf.log.index(lastIncludedIndex).Term == lastIncludedTerm {
		rf.log.Logs = append([]LogEntry(nil), rf.log.Logs[(lastIncludedIndex-rf.log.Base):]...)
		rf.log.Logs = append([]LogEntry{{Term: lastIncludedTerm}}, rf.log.Logs...)
	} else {
		rf.log.Logs = append([]LogEntry(nil), LogEntry{Term: lastIncludedTerm})
	}

	rf.log.Base = lastIncludedIndex
	rf.log.Logs[0].Term = lastIncludedTerm
	rf.snapshot = snapshot
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex

	rf.persistStateAndSnapshot(snapshot)

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
	// 如果下标超过了这个节点的最后向service的提交下标，理应不能被快照
	if index <= rf.log.Base || index > rf.lastApplied {
		DPrintf("[Snapshot] raft %v index: %v,LastIncludeIndex: %v\n", rf.me, index, rf.log.Base)
		return
	}

	lastIncludeTerm := rf.log.index(index).Term
	lastIncludeIndex := index
	// (index,...],for GC
	rf.log.Logs = append([]LogEntry(nil), rf.log.Logs[(index-rf.log.Base+1):]...)
	// append a elem in front of the slice
	rf.log.Logs = append([]LogEntry{{Term: lastIncludeTerm}}, rf.log.Logs...)
	rf.log.Base = lastIncludeIndex
	rf.snapshot = snapshot

	rf.persist()
	rf.persistStateAndSnapshot(snapshot)
	DPrintf("[Snapshot] after raft %v snapshot, log: %v", rf.me, rf.log.Logs)
}
