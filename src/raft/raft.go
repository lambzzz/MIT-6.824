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
	"bytes"
	// "fmt"

	"sync"
	"sync/atomic"

	"math"
	"math/rand"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

func maxf(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func minf(a, b int) int {
	if a < b {
		return a
	}
	return b
}

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
		Follower = iota
		Candidate
		Leader
		PreCandidate
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
	heartbeats chan bool
	applyCh chan ApplyMsg

	// Persistent state on all servers
	currentTerm int
	votedFor int
	log []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex []int
	matchIndex []int

	lastsnapshotIndex int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
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

	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	rf.mu.Lock()
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)
	encoder.Encode(rf.lastsnapshotIndex)
	rf.mu.Unlock()
	data := buf.Bytes()
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

	buf := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buf)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastsnapshotIndex int
	if decoder.Decode(&currentTerm) != nil ||
	   decoder.Decode(&votedFor) != nil ||
	   decoder.Decode(&log) != nil || 
	   decoder.Decode(&lastsnapshotIndex) != nil {
		// error
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.commitIndex = lastsnapshotIndex
		rf.lastApplied = lastsnapshotIndex
		rf.lastsnapshotIndex = lastsnapshotIndex
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
	rf.mu.Lock()
	rf.Snapshotunlocked(index, -1, snapshot)
	rf.mu.Unlock()
}

func (rf *Raft) Snapshotunlocked(index int, term int, snapshot []byte) bool {

	if index <= rf.lastsnapshotIndex {
		return false
	} else if index < rf.lastsnapshotIndex+len(rf.log)-1 {
		snapshotoffset := index - rf.lastsnapshotIndex
		rf.lastsnapshotIndex = index
		rf.commitIndex = maxf(rf.commitIndex, index)
		rf.lastApplied = maxf(rf.lastApplied, index)
		rf.log = rf.log[snapshotoffset:]
	} else if term != -1{
		rf.lastsnapshotIndex = index
		rf.commitIndex = index
		rf.lastApplied = index
		rf.log = []LogEntry{{Term: term, Command: nil}}
	} else {
		return false
	}

	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)
	encoder.Encode(rf.lastsnapshotIndex)
	data := buf.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
	return true
}

type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Data []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	// 任期和状态处理
	if args.Term < rf.currentTerm {
		// 如果此次 RPC 中的任期号比自己小，拒绝这次的 RCP
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	} else if args.Term > rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	} else {
		reply.Term = rf.currentTerm
		rf.state = Follower
	}

	if rf.Snapshotunlocked(args.LastIncludedIndex, args.LastIncludedTerm, args.Data) {
		applymsg := ApplyMsg{
			CommandValid: false,
			SnapshotValid: true,
			Snapshot: args.Data,
			SnapshotTerm: args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		rf.mu.Unlock()
		rf.applyCh <- applymsg
		return
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			go rf.persist()
		} else {
			rf.nextIndex[server] = maxf(rf.nextIndex[server], args.LastIncludedIndex+1)
			rf.matchIndex[server] = maxf(rf.matchIndex[server], args.LastIncludedIndex)
		}
		rf.mu.Unlock()
	}
	return ok
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
	// NextIndex int
	Entriesterm []int
	Entriesindex []int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	persistFlag := false

	// 任期和状态处理
	if args.Term < rf.currentTerm {
		// 如果此次 RPC 中的任期号比自己小，拒绝这次的 RCP
		reply.Term = rf.currentTerm
		reply.Success = false
		// reply.NextIndex = len(rf.log)
		return
	} else if args.Term > rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		persistFlag = true
	} else {
		reply.Term = rf.currentTerm
		rf.state = Follower
	}

	
	if len(rf.log)+rf.lastsnapshotIndex-1 < args.PrevLogIndex {
		reply.Success = false
		// Follower 的日志比 Leader 的日志短，回退到任期号和 PrevLogIndex 匹配的日志条目
		// reply.NextIndex = len(rf.log)
		// for i := len(rf.log)-1; i >= 0; i-- {
		// 	if rf.log[i].Term <= args.PrevLogTerm {
		// 		reply.NextIndex = i + 1
		// 		break
		// 	}
		// }

		// 返回分块日志，在 Leader 端寻找匹配的日志条目
		begin := rf.commitIndex
		end := len(rf.log)+rf.lastsnapshotIndex-1
		length := int(math.Sqrt(float64(end-begin)))
		reply.Entriesterm = append(reply.Entriesterm, rf.log[0].Term)
		reply.Entriesindex = append(reply.Entriesindex, rf.lastsnapshotIndex)
		for i := begin; i < end; i += length {
			reply.Entriesterm = append(reply.Entriesterm, rf.log[i-rf.lastsnapshotIndex].Term)
			reply.Entriesindex = append(reply.Entriesindex, i)
		}
	} else if args.PrevLogIndex < rf.lastsnapshotIndex {
		// 跟随者快照比附加日志开始索引更加新，回退到快照的最后一条日志
		reply.Success = false
		reply.Entriesterm = append(reply.Entriesterm, rf.log[0].Term)
		reply.Entriesindex = append(reply.Entriesindex, rf.lastsnapshotIndex)

	} else if rf.log[args.PrevLogIndex-rf.lastsnapshotIndex].Term != args.PrevLogTerm {
		// 如果在 prevLogIndex 位置的日志条目的任期号和 prevLogTerm 不匹配，往前更新索引重新比较
		reply.Success = false
		// 当附加日志 RPC 的请求被拒绝的时候，跟随者可以(返回)冲突条目的任期号和该任期号对应的最小索引地址。
		// 借助这些信息，领导人可以减小 nextIndex 一次性越过该冲突任期的所有日志条目；这样就变成每个任期需要一次附加条目 RPC 而不是每个条目一次。
		// reply.NextIndex = args.PrevLogIndex
		// for i := args.PrevLogIndex-1; i >= 0; i-- {
		// 	if rf.log[i].Term != rf.log[args.PrevLogIndex].Term && rf.log[i].Term <= args.PrevLogTerm{
		// 		reply.NextIndex = i + 1
		// 		break
		// 	}
		// }

		// 返回分块日志，在 Leader 端寻找匹配的日志条目
		begin := rf.commitIndex
		end := args.PrevLogIndex
		length := int(math.Sqrt(float64(end-begin)))
		reply.Entriesterm = append(reply.Entriesterm, rf.log[0].Term)
		reply.Entriesindex = append(reply.Entriesindex, rf.lastsnapshotIndex)
		for i := begin; i < end; i += length {
			reply.Entriesterm = append(reply.Entriesterm, rf.log[i-rf.lastsnapshotIndex].Term)
			reply.Entriesindex = append(reply.Entriesindex, i)
		}
	} else {
		// 日志不进行直接覆盖，只有RPC日志与自身日志不匹配，才能将后续日志删除并更新，防止接收到旧的RPC导致日志缩短
		for i := range args.Entries {
			if len(rf.log)+rf.lastsnapshotIndex > args.PrevLogIndex+1+i && args.Entries[i].Term == rf.log[args.PrevLogIndex+1+i-rf.lastsnapshotIndex].Term {
				continue
			}
			if len(args.Entries[i:]) > 0 {
				rf.log = append(rf.log[:args.PrevLogIndex+1+i-rf.lastsnapshotIndex], args.Entries[i:]...)
			}
			break
		}

		reply.Success = true
		// reply.NextIndex = args.PrevLogIndex + len(args.Entries) + 1
		persistFlag = true
	}
	

	// 当 RPC 成功即领导者与跟随者的日志一致时，跟随者才会尝试更新 commitIndex
	if reply.Success && args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit <= args.PrevLogIndex + len(args.Entries) {
			rf.commitIndex = args.LeaderCommit
		} else if args.PrevLogIndex + len(args.Entries) > rf.commitIndex{
			rf.commitIndex = args.PrevLogIndex + len(args.Entries)
		}
	}

	// 持久化
	if persistFlag {
		go rf.persist()
	}

	// 重置心跳计时器
	select {
	case rf.heartbeats <- true:
	default:
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntriesReplyHandler(args *AppendEntriesArgs, reply *AppendEntriesReply, server int) {
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		go rf.persist()
	} else if reply.Success {
		NextIndex := args.PrevLogIndex + len(args.Entries) + 1
		if NextIndex > rf.nextIndex[server] {
			rf.nextIndex[server] = NextIndex
		}
		if NextIndex-1 > rf.matchIndex[server] {
			rf.matchIndex[server] = NextIndex - 1
		}
	} else {
		// if reply.NextIndex < rf.nextIndex[server] {
		// 	rf.nextIndex[server] = reply.NextIndex
		// }
		
		for i := len(reply.Entriesindex)-1; i >= 0; i-- {
			if reply.Entriesindex[i]-rf.lastsnapshotIndex < 0 {
				// 回溯到快照位置
				if rf.matchIndex[server] < rf.lastsnapshotIndex {
					args := &InstallSnapshotArgs{
						Term: rf.currentTerm,
						LeaderId: rf.me,
						LastIncludedIndex: rf.lastsnapshotIndex,
						LastIncludedTerm: rf.log[0].Term,
						Data: rf.persister.ReadSnapshot(),
					}
					reply := &InstallSnapshotReply{}
					go rf.sendInstallSnapshot(server, args, reply)
				}
				break
			}
			if reply.Entriesterm[i] == rf.log[reply.Entriesindex[i]-rf.lastsnapshotIndex].Term {
				rf.nextIndex[server] = reply.Entriesindex[i] + 1
				break
			}
		}
	}
	select {
	case rf.heartbeats <- true:
	default:
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
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	persistFlag := false

	if args.Term < rf.currentTerm {			// 如果此次 RPC 中的任期号比自己小，拒绝这次的投票。
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {	// 如果此次 RPC 中的任期号比自己大，承认对方的合法性进行投票并且改变为跟随者状态。
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		persistFlag = true
	}

	// 如果 votedFor 为空或者为 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他
	// 如果两份日志最后的条目的任期号不同，那么任期号大的日志更加新。如果两份日志最后的条目任期号相同，那么日志比较长的那个就更加新。
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && 
	(args.LastLogTerm > rf.log[len(rf.log)-1].Term || 
	(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1+rf.lastsnapshotIndex)) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		persistFlag = true

		select {
		case rf.heartbeats <- true:
		default:
		}
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}

	// 持久化
	if persistFlag {
		go rf.persist()
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
	rf.mu.Lock()
	index = len(rf.log)+rf.lastsnapshotIndex
	term = rf.currentTerm
	isLeader = rf.state == Leader

	if isLeader {
		rf.log = append(rf.log, LogEntry{Term: term, Command: command})
		go rf.persist()
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

// 同步日志，适用于 Leader
func (rf *Raft) syncLogsLoop() {
	rf.mu.Lock()
	LeaderTerm := rf.currentTerm
	for {
		if rf.state != Leader || rf.currentTerm != LeaderTerm || rf.killed() {
			rf.mu.Unlock()
			return
		}
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.syncLogs(i)
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
	}
}

// commitIndex 更新，适用于 Leader 
// 假设存在 N 满足`N > commitIndex`，使得大多数的 `matchIndex[i] ≥ N`以及`log[N].term == currentTerm` 成立，则令 `commitIndex = N`
func (rf *Raft) checkcommitIndexLoop() {
	rf.mu.Lock()
	LeaderTerm := rf.currentTerm
	for {
		if rf.state != Leader || rf.currentTerm != LeaderTerm || rf.killed() {
			rf.mu.Unlock()
			return
		}
		N := rf.commitIndex
		// 从后往前遍历
		for i := len(rf.log)-1; i > rf.commitIndex-rf.lastsnapshotIndex; i-- {
			count := 1
			for j := range rf.peers {
				if j == rf.me {
					continue
				}
				if rf.matchIndex[j] >= i+rf.lastsnapshotIndex {
					count++
				}
			}
			if count > len(rf.peers)/2 && rf.log[i].Term == rf.currentTerm {
				N = i+rf.lastsnapshotIndex
				break
			}
		}

		// 二分查找，Fig.8测试存在bug
		// var left, right, mid int = rf.commitIndex+1, len(rf.log)-1, 0
		// for left <= right {
		// 	mid = (left + right) / 2
		// 	count := 1
		// 	for j := range rf.peers {
		// 		if j == rf.me {
		// 			continue
		// 		}
		// 		if rf.matchIndex[j] >= mid {
		// 			count++
		// 		}
		// 	}
		// 	if count > len(rf.peers)/2 {
		// 		left = mid + 1
		// 	} else {
		// 		right = mid - 1
		// 	}
		// }
		// if right > rf.commitIndex && rf.log[right].Term == rf.currentTerm {
		// 	N = right
		// }

		if N > rf.commitIndex {
			rf.commitIndex = N
		}
		
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
	}
}

func (rf *Raft) syncLogs(server int) {
	rf.mu.Lock()

	if len(rf.log)-1+rf.lastsnapshotIndex < rf.nextIndex[server] || rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	lognextIndex := rf.nextIndex[server]-rf.lastsnapshotIndex
	if lognextIndex < 1 {
		args := &InstallSnapshotArgs{
			Term: rf.currentTerm,
			LeaderId: rf.me,
			LastIncludedIndex: rf.lastsnapshotIndex,
			LastIncludedTerm: rf.log[0].Term,
			Data: rf.persister.ReadSnapshot(),
		}
		reply := &InstallSnapshotReply{}
		go rf.sendInstallSnapshot(server, args, reply)
		rf.mu.Unlock()
		return
	}
	args := &AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		PrevLogIndex: lognextIndex+rf.lastsnapshotIndex-1,
		PrevLogTerm: rf.log[lognextIndex-1].Term,
		Entries: rf.log[lognextIndex:],
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	rf.mu.Lock()
	if rf.state != Leader || rf.currentTerm != args.Term {
		rf.mu.Unlock()
		return
	}
	if ok {
		rf.AppendEntriesReplyHandler(args, reply, server)
	}
	rf.mu.Unlock()
}

// 服务器应用日志，适用于所有 Server 
func (rf *Raft) applyLogsLoop() {
	rf.mu.Lock()
	for {
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			applymsg := ApplyMsg{
				CommandValid: true,
				Command: rf.log[rf.lastApplied-rf.lastsnapshotIndex].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- applymsg
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
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

func (rf *Raft) sendHeartbeat(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	lognextIndex := rf.nextIndex[server]-rf.lastsnapshotIndex
	if lognextIndex < 1 {
		args := &InstallSnapshotArgs{
			Term: rf.currentTerm,
			LeaderId: rf.me,
			LastIncludedIndex: rf.lastsnapshotIndex,
			LastIncludedTerm: rf.log[0].Term,
			Data: rf.persister.ReadSnapshot(),
		}
		reply := &InstallSnapshotReply{}
		go rf.sendInstallSnapshot(server, args, reply)
		rf.mu.Unlock()
		return
	}
	args := &AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		PrevLogIndex: rf.nextIndex[server]-1,
		PrevLogTerm: rf.log[lognextIndex-1].Term,
		Entries: make([]LogEntry, 0),
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	rf.mu.Lock()
	if rf.state != Leader || rf.currentTerm != args.Term || rf.killed() {
		rf.mu.Unlock()
		return
	}
	if ok {
		rf.AppendEntriesReplyHandler(args, reply, server)
	}
	rf.mu.Unlock()
}

// 广播心跳，适用于 Leader
func (rf *Raft) broadcastHeartbeatsLoop() {
	rf.mu.Lock()
	LeaderTerm := rf.currentTerm
	for {
		if rf.state != Leader || rf.currentTerm != LeaderTerm || rf.killed() {
			rf.mu.Unlock()
			return
		}
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.sendHeartbeat(i)
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
		rf.mu.Lock()
	}
}

func (rf *Raft) electleader() {
	rf.mu.Lock()
	args := &RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: len(rf.log)-1+rf.lastsnapshotIndex,
		LastLogTerm: rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

	votes := int32(1)
	var wg sync.WaitGroup
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(server int) {
			defer wg.Done()
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(server, args, reply) {
				return
			}
			rf.mu.Lock()
			if reply.VoteGranted && reply.Term == args.Term{
				atomic.AddInt32(&votes, 1)
				if atomic.LoadInt32(&votes) > int32(len(rf.peers)/2) && rf.state == Candidate && rf.currentTerm == args.Term{
					rf.state = Leader
					// nextIndex 初始值为领导人最后的日志条目的索引+1, matchIndex 初始值为 0
					for i := range rf.peers {
						rf.nextIndex[i] = len(rf.log)+rf.lastsnapshotIndex
						rf.matchIndex[i] = 0
					}
					go rf.broadcastHeartbeatsLoop()
					go rf.syncLogsLoop()
					go rf.checkcommitIndexLoop()

					select {
					case rf.heartbeats <- true:
					default:
					}
				}
			} else if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				go rf.persist()

				select {
				case rf.heartbeats <- true:
				default:
				}
			}
			rf.mu.Unlock()
		}(i)
	}
	wg.Wait()
}



// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		randnum := rand.Intn(300) + 300
		delay := time.Duration(randnum) * time.Millisecond
		time.Sleep(delay)

		rf.mu.Lock()
		select {
		case <-rf.heartbeats:
		default:
			switch rf.state {
			case Follower:
				rf.currentTerm++
				rf.state = Candidate
				rf.votedFor = rf.me
				
				go rf.electleader()
				go rf.persist()

			case PreCandidate:

			case Candidate:
				rf.currentTerm++
				rf.state = Candidate
				rf.votedFor = rf.me

				go rf.electleader()
				go rf.persist()

			case Leader:
				rf.state = Follower
			}
			
			select {
			case rf.heartbeats <- true:
			default:
			}
		}
		rf.mu.Unlock()
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
	rf := &Raft{
		state: Follower,
		heartbeats: make(chan bool, 1),
		applyCh: applyCh,
		currentTerm: 0,
		votedFor: -1,
		log: []LogEntry{{Term: 0, Command: nil}},
		commitIndex: 0,
		lastApplied: 0,
		nextIndex: make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
		lastsnapshotIndex: 0,
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLogsLoop()


	return rf
}
