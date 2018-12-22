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

import "sync"
import "labrpc"

// import "bytes"
// import "labgob"

import (
    "fmt"
    "time"
    "math/rand"
    "strconv"
)


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
	CommandValid    bool
	Command         interface{}
	CommandIndex    int
}

type LogEntry struct {
    Entry           string
    Term            int
}

type AppendEntryState struct {
//    leaderIsLeagal  bool
//    leaderFrom      int
    isApplyMsg      bool
    isHeartbeat     bool
    preCommitIndex  int
}

type VoteState struct {
    voteGrant       bool
}

type RpcState struct {
    aeState         AppendEntryState
    vState          VoteState
    isToFollower    bool
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

    /*Persistent state on all servers*/
    currentTerm     int
    votedFor        string             //candidateId that received vote in current term(or null if none)
    log             []LogEntry

    /*Volatile state on all servers*/
    commitIndex     int
    lastApplied     int

    /*Volatile state on leaders*/
    nextIndex       []int
    matchIndex      []int

    exitChan        chan bool
    role            int                //0: follower, 1: candidate, 2: leader
    rpcState        chan RpcState
    isHbChan        chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
    term = rf.currentTerm
    if rf.role == 2 {
        fmt.Printf("Get state, My role = %v, Id = %v, term = %v, I am a reader\n", rf.role, rf.me, rf.currentTerm)
        isleader = true
    } else {
        fmt.Printf("Get state, My role = %v, Id = %v, term = %v, I am not a reader\n", rf.role, rf.me, rf.currentTerm)
        isleader = false
    }
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    Term            int
    CandidateId     int
    LastLogIndex    int
    LastLogTerm     int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
    Term        int
    VoteGranted bool
}

type RpcCandidateReply struct {
    Reply       *RequestVoteReply
    ExecutedId  int
    Result      bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

    var isChangeRole bool
    fmt.Printf("My role = %v, my Id = %v, term = %v, args.Term = %v, receive a vote request from candidate = %v\n", rf.role, rf.me, rf.currentTerm, args.Term, args.CandidateId)

    if args.Term < rf.currentTerm {        //current term of this server is higher than args, deny vote
        reply.Term        = rf.currentTerm
        reply.VoteGranted = false
    } else if args.Term == rf.currentTerm {

        if rf.votedFor ==  strconv.Itoa(args.CandidateId) || rf.votedFor == "" {
            fmt.Printf("My role = %v, my Id = %v, rf.votedFor = %v, term = %v\n", rf.role, rf.me, rf.votedFor, rf.currentTerm)
            reply.Term  = args.Term
            rf.votedFor = strconv.Itoa(args.CandidateId)

            fmt.Printf("My role = %v, my Id = %v, rf.commitIndex = %v, rf.log[rf.commitIndex].Term = %v, args.LastLogTerm = %v\n", rf.role, rf.me, rf.commitIndex, rf.log[rf.commitIndex].Term, args.LastLogTerm)
            if rf.log[rf.commitIndex].Term < args.LastLogTerm {
                fmt.Printf("My role = %v, my Id = %v, vote grant, reply to candidate = %v, term = %v\n", rf.role, rf.me, args.CandidateId, rf.currentTerm)
                reply.VoteGranted = true
            } else if rf.log[rf.commitIndex].Term == args.LastLogTerm {
                if rf.commitIndex < args.LastLogIndex {
                    fmt.Printf("My role = %v, my Id = %v, term = %v, rf.commitIndex < args.LastLogIndex, return true\n", rf.role, rf.me, rf.currentTerm)
                    reply.VoteGranted = true
                } else if rf.commitIndex == args.LastLogIndex {
                    fmt.Printf("My role = %v, my Id = %v, term = %v, rf.commitIndex == args.LastLogIndex, return true\n", rf.role, rf.me, rf.currentTerm)
                    reply.VoteGranted = true
                } else {
                    fmt.Printf("My role = %v, my Id = %v, term = %v, rf.commitIndex > args.LastLogIndex, return false\n", rf.role, rf.me, rf.currentTerm)
                    reply.VoteGranted = false
                }
            } else {
                fmt.Printf("My role = %v, my Id = %v, vote deny, reply to candidate = %v, term = %v\n", rf.role, rf.me, args.CandidateId, rf.currentTerm)
                reply.VoteGranted = false
            }
        } else {
            reply.VoteGranted = false
            fmt.Printf("My role = %v, my Id = %v, current voter has voted to candidate, term = %v\n", rf.role, rf.me, rf.currentTerm)
        }
    } else {                                //current term of this server is lower than args
        fmt.Printf("My role = %v, my Id = %v, term = %v, rf.log[len(rf.log) - 1].Term = %v < args.LastLogTerm = %v, set new currentTerm\n", rf.role, rf.me, rf.currentTerm, rf.log[len(rf.log) - 1].Term, args.Term)
        rf.mu.Lock()
        rf.currentTerm = args.Term
        rf.mu.Unlock()

        if rf.log[len(rf.log) - 1].Term > args.LastLogTerm {
            fmt.Printf("My role = %v, my Id = %v, term = %v, rf.log[len(rf.log) - 1].Term = %v > args.LastLogTerm = %v\n", rf.role, rf.me, rf.currentTerm, rf.log[len(rf.log) - 1].Term, args.LastLogTerm)
            reply.VoteGranted = false
        } else if rf.log[len(rf.log) - 1].Term == args.LastLogTerm {
            fmt.Printf("My role = %v, my Id = %v, term = %v, rf.log[len(rf.log) - 1].Term = %v == args.LastLogTerm = %v\n", rf.role, rf.me, rf.currentTerm, rf.log[len(rf.log) - 1].Term, args.LastLogTerm)
            if len(rf.log) - 1 > args.LastLogIndex {
                fmt.Printf("My role = %v, my Id = %v, term = %v, len(rf.log) - 1 = %v > args.LastLogIndex = %v, vote deny\n", rf.role, rf.me, rf.currentTerm, len(rf.log) - 1, args.LastLogIndex)
                reply.VoteGranted = false
            } else {
                fmt.Printf("My role = %v, my Id = %v, term = %v, len(rf.log) - 1 = %v <= args.LastLogIndex = %v, vote grant\n", rf.role, rf.me, rf.currentTerm, len(rf.log) - 1, args.LastLogIndex)
                rf.mu.Lock()
                rf.votedFor = strconv.Itoa(args.CandidateId)
                rf.mu.Unlock()
                reply.VoteGranted = true
            }
        } else {
            rf.mu.Lock()
            rf.votedFor = strconv.Itoa(args.CandidateId)
            rf.mu.Unlock()

            reply.VoteGranted = true
        }

        if rf.role != 0 {                   //if the current term of candidate or leader is lower than args, than thansis to follower
            fmt.Printf("my role = %v, me = %v change to follower, term = %v\n", rf.role, rf.me, rf.currentTerm)
            isChangeRole = true
        }
    }

    /*pass rpc state to current server*/
    func(role, currentServer, candidateId int, isGrant, isChangeRole bool) {
        fmt.Printf("My role = %v, my Id = %v, voteDone, start send channel, candidate = %v\n", role, currentServer, candidateId)
        rf.rpcState <- RpcState { AppendEntryState{} , VoteState{isGrant}, isChangeRole}
        fmt.Printf("My role = %v, my Id = %v, voteDone, somewhere receive channel, candidate = %v\n", role, currentServer, candidateId)
    }(rf.role, rf.me, args.CandidateId, reply.VoteGranted, isChangeRole)
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


type AppendEntriesArgs struct {
    Term            int
    LeaderId        int
    PrevLogIndex    int
    PrevLogTerm     int
    Entries         []LogEntry
    LeaderCommit    int
    Heartbeat       bool
}

type AppendEntriesReply struct {
    Term        int
    Success     bool
}

type RpcLeaderReply struct {
    Reply       *AppendEntriesReply
    ExecutedId  int
    Result      bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    fmt.Printf("My role = %v, Id = %v, term = %v, get heartbeat from leader = %v\n", rf.role, rf.me, rf.currentTerm, args.LeaderId)

    var isApply      bool
    var isChangeRole bool
    var preIndex     int
//    var min          int
    fmt.Printf("My role = %v, Id = %v, args.Term = %v, rf.currentTerm = %v\n", rf.role, rf.me, args.Term, rf.currentTerm)
    if args.Term >= rf.currentTerm {
        fmt.Printf("My role = %v, Id = %v, term = %v, AppendEntries true\n", rf.role, rf.me, rf.currentTerm)
        fmt.Printf("My role = %v, Id = %v, args.Term = %v >= rf.currentTerm = %v, set new currentTerm\n", rf.role, rf.me, args.Term, rf.currentTerm)

        rf.mu.Lock()
        rf.currentTerm = args.Term
        rf.mu.Unlock()
        if rf.role != 0 {
            fmt.Printf("My role = %v, Id = %v, term = %v, AppendEntries, isChangeRole = true\n", rf.role, rf.me, rf.currentTerm)
            isChangeRole = true
        }

            fmt.Printf("My role = %v, Id = %v, term = %v, AppendEntries, len(rf.log) = %v, PrevLogIndex = %v\n", rf.role, rf.me, rf.currentTerm, len(rf.log), args.PrevLogIndex)
        if len(rf.log) < args.PrevLogIndex {
            fmt.Printf("My role = %v, Id = %v, term = %v, AppendEntries, len(rf.log) < PrevLogIndex, reply.Success = false\n", rf.role, rf.me, rf.currentTerm)
            reply.Success = false
        } else if len(rf.log) == args.PrevLogIndex {
            fmt.Printf("My role = %v, Id = %v, term = %v, AppendEntries, len(rf.log)=%v = PrevLogIndex=%v\n", rf.role, rf.me, rf.currentTerm, len(rf.log), args.PrevLogIndex)
            if len(rf.log) == 1 {
                if args.Heartbeat == false {
                    fmt.Printf("My role = %v, Id = %v, term = %v, AppendEntries, agrs.Heartbeat = false\n", rf.role, rf.me, rf.currentTerm)
                    isApply = true
                    rf.mu.Lock()
                    rf.log = append(rf.log[:args.PrevLogIndex], args.Entries...)
                    for i := 0; i < len(rf.log); i++ {
                        fmt.Printf("My role = %v, Id = %v, term = %v, AppendEntries, log[%v].Entry = %v\n", rf.role, rf.me, rf.currentTerm, i, rf.log[i].Entry)
                    }
//                    rf.commitIndex = len(rf.log) - 1
/*                    if args.LeaderCommit <= len(rf.log) - 1 {
                        min = args.LeaderCommit
                    } else {
                        min = len(rf.log) - 1
                    }
                    if args.LeaderCommit > rf.commitIndex {
                        rf.commitIndex = min
                    }
*/
                    rf.mu.Unlock()
                    preIndex = args.PrevLogIndex
                } else {
                    fmt.Printf("My role = %v, Id = %v, term = %v, AppendEntries, agrs.Heartbeat = true, do nothing\n", rf.role, rf.me, rf.currentTerm)
                }
                reply.Success = true
                fmt.Printf("My role = %v, Id = %v, term = %v, AppendEntries, len(rf.log) = PrevLogIndex, len(rf.log) = 1, success = true\n", rf.role, rf.me, rf.currentTerm)
            } else {
                reply.Success = false
                fmt.Printf("My role = %v, Id = %v, term = %v, AppendEntries, len(rf.log) = PrevLogIndex, len(rf.log) != 1, success = false\n", rf.role, rf.me, rf.currentTerm)
            }
        } else {
            fmt.Printf("My role = %v, Id = %v, term = %v, AppendEntries, len(rf.log) > PrevLogIndex\n", rf.role, rf.me, rf.currentTerm)

            if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
                fmt.Printf("My role = %v, Id = %v, term = %v, AppendEntries, rf.log[args.PrevLogIndex].Term != args.PrevLogTerm, reply.Success = false\n", rf.role, rf.me, rf.currentTerm)
                reply.Success = false
            } else {
                fmt.Printf("My role = %v, Id = %v, term = %v, AppendEntries, rf.log[args.PrevLogIndex].Term == args.PrevLogTerm, reply.Success = true\n", rf.role, rf.me, rf.currentTerm)

                if args.Heartbeat == true {
                    fmt.Printf("My role = %v, Id = %v, term = %v, AppendEntries, agrs.Heartbeat = true, do nothing\n", rf.role, rf.me, rf.currentTerm)
                } else {
                    fmt.Printf("My role = %v, Id = %v, term = %v, AppendEntries, agrs.Heartbeat = false\n", rf.role, rf.me, rf.currentTerm)
                    isApply = true
                    rf.mu.Lock()
                    rf.log = append(rf.log[:args.PrevLogIndex], args.Entries...)
                    for i := 0; i < len(rf.log); i++ {
                        fmt.Printf("My role = %v, Id = %v, term = %v, AppendEntries, log[%v].Entry = %v\n", rf.role, rf.me, rf.currentTerm, i, rf.log[i].Entry)
                    }
//                    rf.commitIndex = len(rf.log) - 1
                    rf.mu.Unlock()
                    preIndex = args.PrevLogIndex
                }
                reply.Success = true
            }
        }
    } else {
        fmt.Printf("My role = %v, Id = %v, currentTerm = %v > args.Term, AppendEntries failure\n", rf.role, rf.me, rf.currentTerm, args.Term)
        reply.Term = rf.currentTerm
        reply.Success = false
    }

    fmt.Printf("My role = %v, Id = %v, term = %v, send heartbeatChan start\n", rf.role, rf.me, rf.currentTerm)
    func(leader int, isChangeRole ,isApply bool, preIndex int) {
        rf.rpcState <- RpcState{AppendEntryState{isApply, true, preIndex}, VoteState{}, isChangeRole}
    }(args.LeaderId, isChangeRole, isApply, preIndex)
    fmt.Printf("My role = %v, Id = %v, term = %v, send heartbeatChan end\n", rf.role, rf.me, rf.currentTerm)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
    fmt.Printf("My role = %v, my Id = %v, term = %v, Start a command = %v\n", rf.role, rf.me, rf.currentTerm, fmt.Sprintf("%v", command))
    rf.mu.Lock()
    if rf.role == 2 {
        fmt.Printf("My role = %v, my Id = %v, term = %v, I am leader\n", rf.role, rf.me, rf.currentTerm)

        rf.log = append(rf.log, LogEntry{fmt.Sprintf("%v", command), rf.currentTerm})
        InitNextIndex(rf)

        isLeader = true
        index = len(rf.log) - 1
        term  = rf.currentTerm

        for i := 0; i < len(rf.log); i++ {
            fmt.Printf("My role = %v, my Id = %v, term = %v, rf.log[%v].Term = %v, rf.log[%v].Entry = %v\n", rf.role, rf.me, rf.currentTerm, i, rf.log[i].Term, i, rf.log[i].Entry)
        }

        go func(ch chan bool) {
            fmt.Printf("My role = %v, my Id = %v, term = %v, send isHbChan start\n", rf.role, rf.me, rf.currentTerm)
            ch <- false
            fmt.Printf("My role = %v, my Id = %v, term = %v, send isHbChan end\n", rf.role, rf.me, rf.currentTerm)
        } (rf.isHbChan)
    } else {
        fmt.Printf("My role = %v, my Id = %v, term = %v, I am not a leader, return false\n", rf.role, rf.me, rf.currentTerm)
        isLeader = false
    }
    rf.mu.Unlock()
    fmt.Printf("My role = %v, my Id = %v, term = %v, index = %v, term = %v, isLeader = %v, Start a command = %v end\n", rf.role, rf.me, rf.currentTerm, index, term, isLeader, fmt.Sprintf("%v", command))

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
    fmt.Printf("My role = %v, my Id = %v, term = %v, will be killed now\n", rf.role, rf.me, rf.currentTerm)
    go func(){
        fmt.Printf("My role = %v, my Id = %v, term = %v, send exitChan\n", rf.role, rf.me, rf.currentTerm)
        rf.exitChan <- true
        fmt.Printf("My role = %v, my Id = %v, term = %v, send exitChan over\n", rf.role, rf.me, rf.currentTerm)
    }()
    fmt.Printf("My role = %v, my Id = %v, term = %v, has been killed\n", rf.role, rf.me, rf.currentTerm)
}

type ServerState struct {
    rpcFailCount    int
    isHandout       bool
    isFailure       bool
    isDuplicated    bool
    isRpcReturn     bool
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

    rf.mu.Lock()
    rf.log         = make([]LogEntry, 1)
    rf.rpcState    = make(chan RpcState)
    rf.exitChan    = make(chan bool)
    rf.isHbChan    = make(chan bool)
    rf.nextIndex   = make([]int, len(rf.peers))
    rf.matchIndex  = make([]int, len(rf.peers))
    InitNextIndex(rf)
    rf.mu.Unlock()

    fmt.Printf("me = %v\n", me)
    fmt.Printf("server handler\n")
    go serverHandler(rf, applyCh)
    fmt.Printf("create goroutine, program continue\n")

	return rf
}

type Candidate struct {
    numberOfVotes   int
    flagRpcCalled   []bool
}

func serverHandler (rf *Raft, applyCh chan ApplyMsg) {

    ExitServer:
    for {

        switch rf.role {
            /*Current stage is follower*/
            case 0:
                fmt.Printf("Follower = %v, term = %v, stage start......\n", rf.me, rf.currentTerm)

                rf.mu.Lock()
                rf.votedFor = ""            //a follower will get a vote
                rf.mu.Unlock()

                timeout := GetRandamNumber(600, 800)
                fmt.Printf("follower = %v, term = %v, timeout = %v\n", rf.me, rf.currentTerm, timeout)
                followerTimer := time.NewTimer(time.Duration(timeout) * time.Millisecond)
                defer followerTimer.Stop()

                ExitFollower:
                for {

                    fmt.Printf("follower = %v, term = %v, running.........\n", rf.me, rf.currentTerm)
                    select {
                        case <- followerTimer.C:
                            fmt.Printf("follower = %v, get timer.C channel, time out\n", rf.me)
                            TransistToCandidate(rf)
                            break ExitFollower

                        case state := <-rf.rpcState:
                            isReset := false
                            fmt.Printf("follower = %v, get channel rf.rpcState\n", rf.me)
                            if state.vState.voteGrant == true {
                                isReset = true
                                fmt.Printf("follower = %v, vote to somebody, reset timer\n", rf.me)
                            } else {
                                isReset = false
                                fmt.Printf("follower = %v, vote deny, donot reset the timer\n", rf.me)
                            }
                            if state.aeState.isHeartbeat == true {
                                isReset = true
                            }
                            if state.aeState.isApplyMsg == true {
                                isReset = true
                                fmt.Printf("follower = %v, term = %v, preCommitIndex = %v, commitIndex = %v, len(rf.log) - 1 = %v, isApplyMsg = true\n", rf.me, rf.currentTerm, state.aeState.preCommitIndex, rf.commitIndex, len(rf.log) - 1)
                                fmt.Printf("follower = %v, term = %v, commitIndex = %v\n", rf.me, rf.currentTerm, rf.commitIndex)
                                rf.mu.Lock()
                                rf.commitIndex++
                                rf.mu.Unlock()
                                for j := rf.commitIndex; j < len(rf.log); j++ {
                                    cmd, _ := strconv.Atoi(rf.log[j].Entry)
                                    fmt.Printf("follower = %v, term = %v, j = %v, commitIndex = %v, cmd = %v, applyChan send\n", rf.me, rf.currentTerm, j, rf.commitIndex, cmd)
                                    applyCh <- ApplyMsg{true, cmd, j}
                                    fmt.Printf("follower = %v, term = %v, applyChan send end, commitIndex = %v\n", rf.me, rf.currentTerm, rf.commitIndex)
                                }
                                rf.mu.Lock()
                                rf.commitIndex = len(rf.log) - 1
                                rf.mu.Unlock()
                                fmt.Printf("follower = %v, term = %v, commitIndex = %v\n", rf.me, rf.currentTerm, rf.commitIndex)
                            }
                            if isReset == true {
                                fmt.Printf("follower = %v, term = %v, reset timer\n", rf.me, rf.currentTerm)
                                timeout =GetRandamNumber(800, 1000)
                                followerTimer = time.NewTimer(time.Duration(timeout) * time.Millisecond)
                            }
                        case <- rf.exitChan:
                            fmt.Printf("follower = %v, term = %v, get channel rf.exitChan, break ExitServer\n", rf.me, rf.currentTerm)
                            break ExitServer
                    }
                }
                fmt.Printf("Follower = %v, term = %v, stage end......\n", rf.me, rf.currentTerm)

            /*Current stage is candidate*/
            case 1:
                rf.mu.Lock()
                rf.currentTerm++
                rf.votedFor = strconv.Itoa(rf.me)
                rf.mu.Unlock()

                numberOfVotes   := 1     //the vote of candidate itself
                rpcSucceedCount := 1     //candidate itself needless to do rpc call

                fmt.Printf("Candidate = %v, term = %v, stage start......\n", rf.me, rf.currentTerm)

                serverState   := make([]ServerState, len(rf.peers))
                rpcCandidateReplyChan := make(chan RpcCandidateReply)

                electTime := GetRandamNumber(600, 800)
                fmt.Printf("electTimer = %v\n", electTime)
                candidateTimer := time.NewTimer(time.Duration(electTime) * time.Millisecond)
                defer candidateTimer.Stop()


                ExitCandidate:
                for i := 0; ; i++ {

                    select {
                        case state := <-rf.rpcState:
                            if state.aeState.isApplyMsg == true {
                                fmt.Printf("candidate = %v, term = %v, preCommitIndex = %v, commitIndex = %v, len(rf.log) - 1 = %v, isApplyMsg = true\n", rf.me, rf.currentTerm, state.aeState.preCommitIndex, rf.commitIndex, len(rf.log) - 1)
                                fmt.Printf("follower = %v, term = %v, commitIndex = %v\n", rf.me, rf.currentTerm, rf.commitIndex)
                                rf.mu.Lock()
                                rf.commitIndex++
                                rf.mu.Unlock()
                                for j := rf.commitIndex; j < len(rf.log); j++ {
                                    cmd, _ := strconv.Atoi(rf.log[j].Entry)
                                    fmt.Printf("candidate = %v, term = %v, j = %v, commitIndex = %v, cmd = %v, applyChan send\n", rf.me, rf.currentTerm, j, rf.commitIndex, cmd)
                                    applyCh <- ApplyMsg{true, cmd, j}
                                    rf.mu.Lock()
                                    rf.commitIndex++
                                    rf.mu.Unlock()
                                    fmt.Printf("candidate = %v, term = %v, applyChan send end, commitIndex = %v\n", rf.me, rf.currentTerm, rf.commitIndex)
                                }
                                rf.mu.Lock()
                                rf.commitIndex = len(rf.log) - 1
                                rf.mu.Unlock()
                                fmt.Printf("follower = %v, term = %v, commitIndex = %v\n", rf.me, rf.currentTerm, rf.commitIndex)
                            }
                            if state.isToFollower == true {
                                TransistToFollower(rf)
                                fmt.Printf("candidate = %v, term = %v, because leader is leagal, candidate transit to follower\n", rf.me, rf.currentTerm)

                                break ExitCandidate
                            }
                        case <-candidateTimer.C:
                            fmt.Printf("candidate = %v, term = %v, election time out, start a new election\n", rf.me, rf.currentTerm)
                            CalculateVotes(rf, serverState, numberOfVotes)

                            break ExitCandidate
                        case <- rf.exitChan:
                            fmt.Printf("candidate = %v, term = %v, get channel rf.exitChan, break ExitServer\n", rf.me, rf.currentTerm)

                            break ExitServer

                        default:
                    }

                    select {
                        case rpcResult := <-rpcCandidateReplyChan:
                            fmt.Printf("candidate = %v, term = %v, server = %v, rpcReturn\n", rf.me, rf.currentTerm, rpcResult.ExecutedId)
                            serverState[rpcResult.ExecutedId].isRpcReturn = true
                            fmt.Printf("candidate = %v, term = %v, server = %v, state[%v].isRpcReturn = %v\n", rf.me, rf.currentTerm, rpcResult.ExecutedId, rpcResult.ExecutedId, serverState[rpcResult.ExecutedId].isRpcReturn)
                            if rpcResult.Result == true {           //previous rpc call succeed
                                fmt.Printf("candidate = %v, term = %v, server = %v, rpc succeeded\n", rf.me, rf.currentTerm, rpcResult.ExecutedId)

                                serverState[rpcResult.ExecutedId].rpcFailCount = 0
                                serverState[rpcResult.ExecutedId].isFailure = false

                                fmt.Printf("candidate = %v, term = %v, reply.Term = %v\n", rf.me, rf.currentTerm, rpcResult.Reply.Term)
                                if rpcResult.Reply.Term > rf.currentTerm {
                                    fmt.Printf("candidate = %v, term = %v < reply.Term = %v, set new currentTerm\n", rf.me, rf.currentTerm, rpcResult.Reply.Term)
                                    rf.mu.Lock()
                                    rf.currentTerm = rpcResult.Reply.Term
                                    rf.mu.Unlock()
                                }

                                rpcSucceedCount++
                                if rpcResult.Reply.VoteGranted == true {
                                    fmt.Printf("candidate = %v, term = %v, server = %v voted a granted vote\n", rf.me, rf.currentTerm, rpcResult.ExecutedId)
                                    numberOfVotes++
                                } else {
                                    rf.mu.Lock()
                                    if rpcResult.Reply.Term > rf.currentTerm {
                                        fmt.Printf("candidate = %v, term = %v, reply.Term = %v, server = %v did not vote, because current candidate term is low\n", rf.me, rf.currentTerm, rpcResult.Reply.Term, rpcResult.ExecutedId)
                                        rf.currentTerm = rpcResult.Reply.Term
                                    }
                                    fmt.Printf("candidate = %v, term = %v, server = %v did not vote\n", rf.me, rf.currentTerm, rpcResult.ExecutedId)
                                    rf.mu.Unlock()
                                }

                                /*All server has reply the result of vote*/
                                fmt.Printf("candidate = %v, term = %v, server = %v, IsServerAllTrue = %v, ValidServerCount = %v, rpcCount = %v, IsRpcAllReturn = %v\n", rf.me, rf.currentTerm, rpcResult.ExecutedId, IsServerAllTrue(serverState), ValidServerCount(serverState), rpcSucceedCount, IsRpcAllReturn(serverState))
                                if IsServerAllTrue(serverState) == true && ValidServerCount(serverState) <= rpcSucceedCount && IsRpcAllReturn(serverState){

                                    ok := CalculateVotes(rf, serverState, numberOfVotes)
                                    if ok == true {
                                        fmt.Printf("candidate = %v, term = %v, rpc success, get majority votes, transist to leader\n", rf.me, rf.currentTerm)
                                        break ExitCandidate
                                    }
                                    fmt.Printf("candidate = %v, term = %v, rpc success get less votes, wait for election timeout\n", rf.me, rf.currentTerm)

                                }
                            } else {                            //previous rpc call failure
                                serverState[rpcResult.ExecutedId].rpcFailCount++
                                fmt.Printf("candidate = %v, term = %v, server = %v rpc failure, failCount = %v\n", rf.me, rf.currentTerm, rpcResult.ExecutedId, serverState[rpcResult.ExecutedId].rpcFailCount)
                                if serverState[rpcResult.ExecutedId].rpcFailCount >= 1 {
                                    fmt.Printf("candidate = %v, term = %v, server = %v failure times more than 5, set failure flag to it\n", rf.me, rf.currentTerm, rpcResult.ExecutedId)
                                    serverState[rpcResult.ExecutedId].isFailure = true
                                }

                                fmt.Printf("candidate = %v, term = %v, server = %v, IsServerAllTrue = %v, ValidServerCount = %v, rpcCount = %v, IsRpcAllReturn = %v\n", rf.me, rf.currentTerm, rpcResult.ExecutedId, IsServerAllTrue(serverState), ValidServerCount(serverState), rpcSucceedCount, IsRpcAllReturn(serverState))
                                if IsServerAllTrue(serverState) == true && ValidServerCount(serverState) <= rpcSucceedCount && IsRpcAllReturn(serverState) {

                                    ok := CalculateVotes(rf, serverState, numberOfVotes)
                                    if ok == true {
                                        fmt.Printf("candidate = %v, term = %v, rpc fails, get majority votes, transist to leader\n", rf.me, rf.currentTerm)
                                        break ExitCandidate
                                    }
                                    fmt.Printf("candidate = %v, term = %v, rpc fails, get less votes, wait for election timeout\n", rf.me, rf.currentTerm)
                                } else {
                                    serverState[rpcResult.ExecutedId].isHandout = false         //re-transmit rpc call to the server
                                }

                                fmt.Printf("candidate = %v, term = %v, server = %v, rpc failed\n", rf.me, rf.currentTerm, rpcResult.ExecutedId)
                            }
                        default:
                    }

                    if i >= len(rf.peers) {
                        i = 0
                    }

                    if i == rf.me {
                        serverState[i].isHandout   = true
                        serverState[i].isRpcReturn = true
                        continue
                    }


                    if serverState[i].isHandout == true || serverState[i].isFailure == true {
                        continue
                    } else {
                        serverState[i].isHandout = true
                    }
                    var args  RequestVoteArgs
                    var reply RequestVoteReply

                    rf.mu.Lock()
                    args.Term = rf.currentTerm
                    args.CandidateId = rf.me
                    args.LastLogIndex = len(rf.log) - 1
                    args.LastLogTerm = rf.log[args.LastLogIndex].Term
                    fmt.Printf("candidate = %v, term = %v, LastLogIndex = %v, LastLogTerm = %v\n", rf.me, rf.currentTerm, args.LastLogIndex, args.LastLogTerm)
                    rf.mu.Unlock()
                    serverState[i].isRpcReturn = false

                    go func(server int, args *RequestVoteArgs, reply *RequestVoteReply, rpcCandidateReplyChan chan RpcCandidateReply) {

                        rpcChan := make(chan RpcCandidateReply)
                        rpcTimeout := 300
                        rpcTimer   := time.NewTimer(time.Duration(rpcTimeout) * time.Millisecond)
                        defer rpcTimer.Stop()
                        fmt.Printf("candidate = %v, rpc executedId = %v, term = %v, set rpcTimeout = %v\n", args.CandidateId, server, args.Term, rpcTimeout)

                        go func(rpcChan chan RpcCandidateReply) {
                            fmt.Printf("candidate = %v, rpc executedId = %v, term = %v, send rpc request start......\n", args.CandidateId, server, args.Term)
                            err := rf.sendRequestVote(server, args, reply)
                            fmt.Printf("candidate = %v, rpc executedId = %v, term = %v, send rpc request end, result = %v......\n", args.CandidateId, server, args.Term, err)

                            if IsRpcCandidateChanClosed(rpcChan) == true {
                                fmt.Printf("candidate = %v, term = %v, rpcChan has been closed\n", args.CandidateId, args.Term, server)
                            } else {
                                fmt.Printf("candidate = %v, term = %v, rpcChan has not been closed\n", args.CandidateId, args.Term, server)
                                fmt.Printf("candidate = %v, term = %v, rpc executedId = %v, rpcReplyChan sned\n", args.CandidateId, args.Term, server)
                                rpcChan <- RpcCandidateReply{reply, server, err}
                                fmt.Printf("candidate = %v, term = %v, rpc executedId = %v, rpcReplyChan end\n", args.CandidateId, args.Term, server)
                            }
                        }(rpcChan)

                        select {
                            case <-rpcTimer.C:
                                fmt.Printf("candidate = %v, term = %v, rpc executedId = %v, rpcChan timeout, send rpcReplyChan\n", args.CandidateId, args.Term, server)
                                close(rpcChan)
                                rpcCandidateReplyChan <- RpcCandidateReply{reply, server, false}
                                fmt.Printf("candidate = %v, term = %v, rpc executedId = %v, rpcChan timeout, send rpcReplyChan end\n", args.CandidateId, args.Term, server)

                            case rpc := <-rpcChan:
                                fmt.Printf("candidate = %v, term = %v, rpc executedId = %v, rpcReplyChan sned\n", args.CandidateId, args.Term, server)
                                rpcCandidateReplyChan <- rpc
                                fmt.Printf("candidate = %v, term = %v, rpc executedId = %v, rpcReplyChan sned over\n", args.CandidateId, args.Term, server)
                        }
                    }(i, &args, &reply, rpcCandidateReplyChan)
                }
                fmt.Printf("Candidate = %v, term = %v, stage end......\n", rf.me, rf.currentTerm)

            /*Current server is leader*/
            case 2:
                fmt.Printf("leader = %v, term = %v, I am a leader now\n", rf.me, rf.currentTerm)


                ExitLeader:
                for {
                    fmt.Printf("leader = %v, term = %v, start a heartbeatTimer\n", rf.me, rf.currentTerm)
                    heartbeatTime := 100
                    leaderTimer   := time.NewTimer(time.Duration(heartbeatTime) * time.Millisecond)
                    defer leaderTimer.Stop()
                    serverState   := make([]ServerState, len(rf.peers))
                    rpcSucceedCount    := 1
                    appendEntryCount   := 1
                    rpcLeaderReplyChan := make(chan RpcLeaderReply)
                    isHeartbeat        := true

                    select {
                       case ch := <-rf.isHbChan:
                           isHeartbeat = ch
                           fmt.Printf("leader = %v, term = %v, get channel rf.isHbChan = %v\n", rf.me, rf.currentTerm, isHeartbeat)
                       default:
                    }

                    select {
                        case <- rf.exitChan:
                            fmt.Printf("leader = %v, term = %v, get channel rf.exitChan, break ExitServer\n", rf.me, rf.currentTerm)
                            break ExitServer

                        case <- leaderTimer.C:
                            fmt.Printf("leader = %v, term = %v, heartbeat timerout\n", rf.me, rf.currentTerm)

                            ExitHeartbeat:
                            for i := 0; ; i++ {

                                var args  AppendEntriesArgs
                                var reply AppendEntriesReply

                                select {
                                    case rpcResult := <-rpcLeaderReplyChan:
                                        fmt.Printf("leader = %v, term = %v, server = %v, rpcReturn\n", rf.me, rf.currentTerm, rpcResult.ExecutedId)
                                        serverState[rpcResult.ExecutedId].isRpcReturn = true
                                        if rpcResult.Result == true {
                                            fmt.Printf("leader = %v, term = %v, follower = %v, rpc succeed\n", rf.me, rf.currentTerm, rpcResult.ExecutedId)
                                            rpcSucceedCount++

                                            serverState[rpcResult.ExecutedId].rpcFailCount = 0
                                            serverState[rpcResult.ExecutedId].isFailure    = false

                                            if rpcResult.Reply.Success == true {

                                                appendEntryCount++
                                                rf.mu.Lock()
                                                rf.nextIndex[rpcResult.ExecutedId]  = len(rf.log)
                                                rf.matchIndex[rpcResult.ExecutedId] = len(rf.log) - 1
                                                rf.mu.Unlock()

                                                fmt.Printf("leader = %v, term = %v, isServerAllTrue = %v, ValidServerCount = %v, appendEntryCount = %v, IsRpcAllReturn = %v\n", rf.me, rf.currentTerm, IsServerAllTrue(serverState), ValidServerCount(serverState), appendEntryCount, IsRpcAllReturn(serverState))
//                                                if IsServerAllTrue(serverState) == true && ValidServerCount(serverState) <= appendEntryCount {
                                                if IsServerAllTrue(serverState) == true && ValidServerCount(serverState) <= appendEntryCount && IsRpcAllReturn(serverState) == true {
                                                    fmt.Printf("leader = %v, term = %v, ValidServerCount = %v, appendEntryCount = %v, a heartbeat round is end\n", rf.me, rf.currentTerm, ValidServerCount(serverState), appendEntryCount)
                                                    if CalculateDuplicated(rf, serverState, appendEntryCount) == true {
                                                        rf.mu.Lock()
                                                        fmt.Printf("leader = %v, term = %v, isHeartbeat = %v\n", rf.me, rf.currentTerm, isHeartbeat)
                                                        if isHeartbeat == false {

                                                            fmt.Printf("leader = %v, term = %v, majority server has duplicated, commitIndex = %v, exit appendEntries\n", rf.me, rf.currentTerm, rf.commitIndex)
                                                            go func(ch chan ApplyMsg) {
                                                                fmt.Printf("leader = %v, term = %v, commitIndex = %v, len(rf.log) = %v\n", rf.me, rf.currentTerm, rf.commitIndex, len(rf.log))
                                                                rf.mu.Lock()
                                                                rf.commitIndex++
                                                                rf.mu.Unlock()
                                                                for j := rf.commitIndex; j < len(rf.log); j++ {
                                                                    cmd, _ := strconv.Atoi(rf.log[j].Entry)
                                                                    fmt.Printf("leader = %v, term = %v, len(rf.log) - 1 = %v, commitIndex = %v, cmd = %v, send applyMsg\n", rf.me, rf.currentTerm, len(rf.log) - 1, rf.commitIndex, cmd)
                                                                    ch <- ApplyMsg{true, cmd, j}
                                                                    fmt.Printf("leader = %v, term = %v, len(rf.log) - 1 = %v, commitIndex = %v, send applyMsg end\n", rf.me, rf.currentTerm, len(rf.log) - 1, rf.commitIndex)
                                                                }
                                                                rf.mu.Lock()
                                                                rf.commitIndex = len(rf.log) - 1
                                                                rf.mu.Unlock()
                                                                fmt.Printf("leader = %v, term = %v, commitIndex = %v, len(rf.log) = %v\n", rf.me, rf.currentTerm, rf.commitIndex, len(rf.log))
                                                            } (applyCh)
                                                            isHeartbeat = true
                                                        }
                                                        rf.mu.Unlock()
                                                        break ExitHeartbeat
                                                    } else {
                                                        if rf.role == 0 {
                                                            fmt.Printf("leader = %v, term = %v, has no quorum, transist to follower\n", rf.me, rf.currentTerm, rpcResult.ExecutedId)
                                                            break ExitLeader
                                                        }
                                                        fmt.Printf("leader = %v, term = %v, less server has duplicated, start new appendEntries\n", rf.me, rf.currentTerm)
                                                        break ExitHeartbeat
                                                    }
                                                }
                                            } else {
                                                fmt.Printf("appendEntries failure......\n")

                                                if IsReachQuorum(serverState) == false {
                                                    fmt.Printf("leader = %v, term = %v, transist to follower\n", rf.me, rf.currentTerm)
                                                    TransistToFollower(rf)
                                                    break ExitLeader
                                                } else {
                                                    fmt.Printf("leader = %v, term = %v, there is quorum\n", rf.me, rf.currentTerm)
                                                }
                                                if rpcResult.Reply.Term > rf.currentTerm {
                                                   fmt.Printf("leader = %v, term = %v, rpc executedId = %v, reply.Term = %v > currentTerm = %v, set new Term, transist to follower\n", rf.me, rf.currentTerm, rpcResult.ExecutedId, rpcResult.Reply.Term, rf.currentTerm)
                                                   rf.mu.Lock()
                                                   rf.currentTerm = rpcResult.Reply.Term
                                                   rf.mu.Unlock()
                                                   TransistToFollower(rf)
                                                   break ExitLeader
                                                }
                                                rf.mu.Lock()
                                                rf.nextIndex[rpcResult.ExecutedId]--
                                                rf.mu.Unlock()

                                                serverState[rpcResult.ExecutedId].isHandout = false
                                            }
                                       } else {
                                            fmt.Printf("leader = %v, term = %v, isServerAllTrue = %v, ValidServerCount = %v, rpcSucceedCount = %v, rpc failure\n", rf.me, rf.currentTerm, IsServerAllTrue(serverState), ValidServerCount(serverState), rpcSucceedCount)

                                            serverState[rpcResult.ExecutedId].rpcFailCount++

                                            if serverState[rpcResult.ExecutedId].rpcFailCount >= 1 {
                                                fmt.Printf("leader = %v, term = %v, server = %v failure times more than 1, set failure flag to it\n", rf.me, rf.currentTerm, rpcResult.ExecutedId)
                                                serverState[rpcResult.ExecutedId].isFailure = true
                                            }

                                            fmt.Printf("leader = %v, term = %v, isServerAllTrue = %v, ValidServerCount = %v, appendEntryCount = %v, IsRpcAllReturn = %v\n", rf.me, rf.currentTerm, IsServerAllTrue(serverState), ValidServerCount(serverState), appendEntryCount, IsRpcAllReturn(serverState))
                                            if IsServerAllTrue(serverState) == true && ValidServerCount(serverState) <= appendEntryCount && IsRpcAllReturn(serverState) == true {
                                                if IsReachQuorum(serverState) == true {
                                                    if CalculateDuplicated(rf, serverState, appendEntryCount) == true {
                                                        rf.mu.Lock()
                                                        fmt.Printf("leader = %v, term = %v, isHeartbeat = %v\n", rf.me, rf.currentTerm, isHeartbeat)
                                                        if isHeartbeat == false {

                                                            fmt.Printf("leader = %v, term = %v, majority server has duplicated, commitIndex = %v, exit appendEntries\n", rf.me, rf.currentTerm, rf.commitIndex)
                                                            go func(ch chan ApplyMsg) {
                                                                rf.mu.Lock()
                                                                rf.commitIndex++
                                                                rf.mu.Unlock()
                                                                for j := rf.commitIndex; j < len(rf.log); j++ {
                                                                    cmd, _ := strconv.Atoi(rf.log[j].Entry)
                                                                    fmt.Printf("leader = %v, term = %v, len(rf.log) - 1 = %v, commitIndex = %v, j = %v,  cmd = %v, send applyMsg\n", rf.me, rf.currentTerm, len(rf.log) - 1, rf.commitIndex, j, cmd)
                                                                    ch <- ApplyMsg{true, cmd, rf.commitIndex}
                                                                    fmt.Printf("leader = %v, term = %v, len(rf.log) - 1 = %v, commitIndex = %v, j = %v, send applyMsg end\n", rf.me, rf.currentTerm, len(rf.log) - 1, rf.commitIndex, j)
                                                                }
                                                                rf.mu.Lock()
                                                                rf.commitIndex = len(rf.log) - 1
                                                                rf.mu.Unlock()
                                                                fmt.Printf("leader = %v, term = %v, commitIndex = %v, len(rf.log) = %v\n", rf.me, rf.currentTerm, rf.commitIndex, len(rf.log))

                                                            } (applyCh)
                                                            isHeartbeat = true
                                                        }
                                                        rf.mu.Unlock()
                                                        break ExitHeartbeat
                                                    } else {
                                                        if rf.role == 0 {
                                                            fmt.Printf("leader = %v, term = %v, has no quorum, transist to follower\n", rf.me, rf.currentTerm, rpcResult.ExecutedId)
                                                            break ExitLeader
                                                        }
                                                        fmt.Printf("leader = %v, term = %v, less server has duplicated, start new appendEntries\n", rf.me, rf.currentTerm)
                                                        break ExitHeartbeat
                                                    }
                                                } else {
                                                    fmt.Printf("leader = %v, term = %v, transist to follower\n", rf.me, rf.currentTerm, rpcResult.ExecutedId)
                                                    TransistToFollower(rf)
                                                    break ExitLeader
                                                }
                                            }
                                            fmt.Printf("leader = %v, term = %v, follower = %v, rpc failure\n", rf.me, rf.currentTerm, rpcResult.ExecutedId)
                                            serverState[rpcResult.ExecutedId].isHandout = false
                                        }

                                    case state := <-rf.rpcState:
                                        fmt.Printf("leader = %v , term = %v, get rpc state, isToFollower = %v\n", rf.me, rf.currentTerm, state.isToFollower)
                                        if state.isToFollower == true {
                                            TransistToFollower(rf)
                                            fmt.Printf("candidate = %v, term = %v, because leader is leagal, leader transit to follower\n", rf.me, rf.currentTerm)

                                            break ExitLeader
                                        }
                                    default:
                                }
                                if i >= len(rf.peers) {
                                    i = 0
                                }
                                if i == rf.me {
                                    serverState[i].isHandout = true
                                    serverState[i].isRpcReturn = true
                                    continue
                                }

                                if serverState[i].isHandout == true || serverState[i].isFailure == true {
                                    continue
                                } else {
                                    serverState[i].isHandout = true
                                }

                                rf.mu.Lock()
                                for j := 0; j < len(rf.log); j++ {
                                    fmt.Printf("leader = %v, term = %v, log[%v].Entry = %v\n", rf.me, rf.currentTerm, j, rf.log[j].Entry)
                                }
                                fmt.Printf("leader = %v, term = %v, rf.nextIndex[%v] = %v\n", rf.me, rf.currentTerm, i, rf.nextIndex[i])
                                fmt.Printf("leader = %v, term = %v, len(rf.log) = %v\n", rf.me, rf.currentTerm, len(rf.log))
                                args.Term         = rf.currentTerm
                                args.LeaderId     = rf.me
                                args.PrevLogIndex = rf.nextIndex[i] - 1
                                args.PrevLogTerm  = rf.log[args.PrevLogIndex].Term
                                args.LeaderCommit = rf.commitIndex
                                if isHeartbeat == false {
                                    args.Entries   = make([]LogEntry, len(rf.log) - args.PrevLogIndex)
                                    args.Entries   = append(rf.log[args.PrevLogIndex:])
                                    args.Heartbeat = false
                                    for j := 0; j < len(args.Entries); j++ {
                                        fmt.Printf("leader = %v, term = %v, entries[%v] = %v\n", rf.me, rf.currentTerm, j, args.Entries[j])
                                    }
                                } else {
                                    args.Heartbeat = true
                                    args.Entries = make([]LogEntry, 1)
                                }
                                rf.mu.Unlock()
                                serverState[i].isRpcReturn = false

                                go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, rpcLeaderReplyChan chan RpcLeaderReply) {

                                    rpcChan := make(chan RpcLeaderReply)
                                    rpcTimeout := 300
                                    rpcTimer   := time.NewTimer(time.Duration(rpcTimeout) * time.Millisecond)
                                    defer rpcTimer.Stop()
                                    fmt.Printf("leader = %v, rpc executedId = %v, term = %v, set rpcTimeout = %v\n", args.LeaderId, server, args.Term, rpcTimeout)

                                    go func(rpcChan chan RpcLeaderReply) {
                                        fmt.Printf("leader = %v, term = %v, rpc executedId = %v, send rpc request start......\n", args.LeaderId, args.Term, server)
                                        err := rf.sendAppendEntries(server, args, reply)
                                        fmt.Printf("leader = %v, term = %v, rpc executedId = %v, send rpc request end, result = %v......\n", args.LeaderId, args.Term, server, err)

                                        if IsRpcLeaderChanClosed(rpcChan) == true {
                                            fmt.Printf("leader = %v, term = %v, rpc executedId = %v, rpcChan has been closed, donot send rpcChan\n", args.LeaderId, args.Term, server)
                                        } else {
                                            fmt.Printf("leader = %v, term = %v, rpc executedId = %v, rpcChan has not been closed\n", args.LeaderId, args.Term, server)
                                            fmt.Printf("leader = %v, term = %v, rpc executedId = %v, rpcChan sned\n", args.LeaderId, args.Term, server)
                                            rpcChan <- RpcLeaderReply{reply, server, err}
                                            fmt.Printf("leader = %v, term = %v, rpc executedId = %v, rpcChan end\n", args.LeaderId, args.Term, server)
                                        }
                                    }(rpcChan)

                                    select {
                                        case <-rpcTimer.C:
                                            close(rpcChan)
                                            fmt.Printf("leader = %v, term = %v, rpc executedId = %v, rpcLeaderReplyChan timeout, send rpcReplyChan\n", args.LeaderId, args.Term, server)
                                            rpcLeaderReplyChan <- RpcLeaderReply{reply, server, false}
                                            fmt.Printf("leader = %v, term = %v, rpc executedId = %v, rpcLeaderReplyChan timeout, send rpcReplyChan end\n", args.LeaderId, args.Term, server)

                                        case rpc := <-rpcChan:
                                            fmt.Printf("leader = %v, term = %v, rpc executedId = %v, rpcReplyChan sned\n", args.LeaderId, args.Term, server)
                                            rpcLeaderReplyChan <- rpc
                                            fmt.Printf("leader = %v, term = %v, rpc executedId = %v, rpcReplyChan sned over\n", args.LeaderId, args.Term, server)
                                    }

                                }(i, &args, &reply, rpcLeaderReplyChan)
                            }
                            fmt.Printf("leader = %v, term = %v, heartbeat operation end, reset the leaderTimer\n", rf.me, rf.currentTerm)
                    }//select
                    fmt.Printf("leader = %v, term = %v, stage of leader end\n", rf.me, rf.currentTerm)
                }//for
        }//switch
    }//for
}//serverHandler

func GetRandamNumber(min, max int) int {
    rand.Seed(time.Now().UnixNano())
    randNum := rand.Intn(max - min) + min
    return randNum
}

func IsReachQuorum(server []ServerState) bool {
    count := ValidServerCount(server)
    if count * 2 <= len(server) {
        fmt.Printf("There is no quorum\n")
        return false
    } else {
        fmt.Printf("There is quorum\n")
        return true
    }
}

func IsRpcAllReturn(server []ServerState) bool {
    for i := 0; i < len(server); i++ {
        if server[i].isRpcReturn == false {
            return false
        }
    }
    return true
}

func TransistToFollower(rf *Raft) {
    rf.mu.Lock()
    rf.role = 0
    rf.mu.Unlock()
}

func TransistToCandidate(rf *Raft) {
    rf.mu.Lock()
    rf.role = 1
    rf.mu.Unlock()
}

func TransistToLeader(rf *Raft) {
    rf.mu.Lock()
    rf.role = 2
    rf.mu.Unlock()
}

func IsServerAllTrue(server []ServerState) bool {
    for i := 0; i < len(server); i++ {
        if server[i].isHandout == false {
            if server[i].isFailure == true {
                continue
            }
            return false
        }
    }
    return true
}
func IsGetMajority(number, total int) bool {

    if number * 2 > total {
        return true
    } else {
        return false
    }

}

func IsRpcCandidateChanClosed(ch <-chan RpcCandidateReply) bool {
    select {
        case <-ch:
            return true
        default:
    }
    return false
}
func IsRpcLeaderChanClosed(ch <-chan RpcLeaderReply) bool {
    select {
        case <-ch:
            return true
        default:
    }
    return false
}
func ValidServerCount(server []ServerState) int {
    count := 0
    for i := 0; i < len(server); i++ {
        if server[i].isFailure == true {
            continue
        }
        count++
    }
    return count
}
func ServerDuplicatedCount(server []ServerState) int {
    count := 0
    for i := 0; i < len(server); i++ {
        if server[i].isDuplicated == true {
            continue
        }
        count++
    }
    return count
}
func CalculateVotes(rf *Raft, server []ServerState, numberOfVotes int) bool {

    fmt.Printf("candidate = %v, term = %v, numberOfVotes = %v, ValidServerCount %v, calculateVoutes start\n", rf.me, rf.currentTerm, numberOfVotes, ValidServerCount(server))

    if IsReachQuorum(server) == false {
        fmt.Printf("candidate = %v, term = %v, there is no quorum, candidate transist to follower\n", rf.me, rf.currentTerm)
        TransistToFollower(rf)
        return false
    }

    if IsGetMajority(numberOfVotes, ValidServerCount(server)) {
        fmt.Printf("candidate = %v, term = %v, get majority votes\n", rf.me, rf.currentTerm)
        TransistToLeader(rf)
        return true
    } else {
        fmt.Printf("candidate = %v, term = %v, get less votes\n", rf.me, rf.currentTerm)
        TransistToCandidate(rf)
        return false
    }
    return false
}

func CalculateDuplicated(rf *Raft, server []ServerState, numberOfDuplicated int) bool {
    if IsReachQuorum(server) == false {
        TransistToFollower(rf)
        return false
    }

    if IsGetMajority(numberOfDuplicated, ServerDuplicatedCount(server)) {
        return true
    } else {
        return false
    }

}

func InitNextIndex(rf *Raft) {
    for i := 0; i < len(rf.nextIndex); i++ {
        fmt.Printf("server = %v, nextIndex[%v] = %v\n", i, i, len(rf.log))
        rf.nextIndex[i] = len(rf.log)
    }
}
