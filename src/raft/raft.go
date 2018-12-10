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
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
    Entry           string
    TermEntry       int
}

type HeartbeatState struct {
    leaderIsLeagal  bool
    leaderFrom      int
}

type VoteState struct {
    voteDone        bool
}

type RpcState struct {
    hbState         HeartbeatState
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

    voteDoneChan    chan bool
    role                int                //0: follower, 1: candidate, 2: leader
    rpcState        chan RpcState
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
    fmt.Printf("My role = %v, my Id = %v, term = %v, receive a vote request from candidate = %v\n", rf.role, rf.me, rf.currentTerm, args.CandidateId)
    fmt.Printf("My role = %v, my Id = %v, rf.currentTerm = %v, args.Term = %v\n", rf.role, rf.me, rf.currentTerm, args.Term)

    if args.Term < rf.currentTerm {        //current term of this server is higher than args, deny vote
        reply.Term        = rf.currentTerm
        reply.VoteGranted = false
    } else if args.Term == rf.currentTerm {

        if rf.votedFor ==  strconv.Itoa(args.CandidateId) || rf.votedFor == "" {
            fmt.Printf("My role = %v, my Id = %v, rf.votedFor = %v, term = %v\n", rf.role, rf.me, rf.votedFor, rf.currentTerm)
            reply.Term  = args.Term
            rf.votedFor = strconv.Itoa(args.CandidateId)

            if rf.commitIndex <= args.LastLogIndex {
                fmt.Printf("My role = %v, my Id = %v, vote grant, reply to candidate = %v, term = %v\n", rf.role, rf.me, args.CandidateId, rf.currentTerm)
                reply.VoteGranted = true
            } else {
                fmt.Printf("My role = %v, my Id = %v, rf.commitIndex > args.LastLogIndex, return false, term = %v\n", rf.role, rf.me, rf.currentTerm)
                reply.VoteGranted = false
            }
        } else {
            reply.VoteGranted = false
            fmt.Printf("My role = %v, my Id = %v, current voter has voted to candidate, term = %v\n", rf.role, rf.me, rf.currentTerm)
        }
    } else {                                //current term of this server is lower than args
        rf.mu.Lock()
        rf.currentTerm = args.Term
        rf.votedFor = strconv.Itoa(args.CandidateId)
        rf.mu.Unlock()
        if rf.role != 0 {                   //if the current term of candidate or leader is lower than args, than thansis to follower
            fmt.Printf("my role = %v, me = %v change to follower, term = %v\n", rf.role, rf.me, rf.currentTerm)
            isChangeRole = true
        }
        reply.VoteGranted = true
    }

    /*pass rpc state to current server*/
//    go func(role, currentServer, candidateId int, isGrant, isChangeRole bool) {
    func(role, currentServer, candidateId int, isGrant, isChangeRole bool) {
        fmt.Printf("My role = %v, my Id = %v, voteDone, start send channel, candidate = %v\n", role, currentServer, candidateId)
        rf.rpcState <- RpcState { HeartbeatState{} , VoteState{isGrant}, isChangeRole}
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

    var isChangeRole bool
    fmt.Printf("My role = %v, Id = %v, args.Term = %v, rf.currentTerm = %v\n", rf.role, rf.me, args.Term, rf.currentTerm)
    if args.Term >= rf.currentTerm {
        fmt.Printf("My role = %v, Id = %v, term = %v, AppendEntries true\n", rf.role, rf.me, rf.currentTerm)

        rf.mu.Lock()
        rf.currentTerm = args.Term
        rf.mu.Unlock()
        if rf.role != 0 {
            isChangeRole = true
        }

        reply.Success = true
    } else {
        fmt.Printf("My role = %v, Id = %v, AppendEntries failure\n", rf.role, rf.me)
        reply.Term = rf.currentTerm
        reply.Success = false
    }

    fmt.Printf("My role = %v, Id = %v, term = %v, send heartbeatChan start\n", rf.role, rf.me, rf.currentTerm)
//    go func(leader int, isChangeRole bool) {
    func(leader int, isChangeRole bool) {
        rf.rpcState <- RpcState{ HeartbeatState{}, VoteState{}, isChangeRole}
    }(args.LeaderId, isChangeRole)
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
}

type ServerState struct {
    id              int
    rpcFailCount    int
    isHandout       bool
    isFailure       bool
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
    rf.voteDoneChan = make(chan bool)
    rf.rpcState = make(chan RpcState)
    rf.currentTerm = 0
    rf.mu.Unlock()

    fmt.Printf("me = %v\n", me)
    fmt.Printf("server handler\n")
    go serverHandler(rf)
    fmt.Printf("create goroutine, program continue\n")

	return rf
}

type Candidate struct {
    numberOfVotes   int
    flagRpcCalled   []bool
}


func serverHandler (rf *Raft) {

    for {

        switch rf.role {
            /*Current server is follower*/
            case 0:
                fmt.Printf("Follower = %v, term = %v, stage start......\n", rf.me, rf.currentTerm)

                rf.mu.Lock()
                rf.votedFor = ""
                rf.mu.Unlock()

                ExitFollower:
                for {

                    timeout := GetRandamNumber(800, 1000)
                    fmt.Printf("timeout = %v\n", timeout)
                    followerTimer := time.NewTimer(time.Duration(timeout) * time.Millisecond)
                    defer followerTimer.Stop()

                    select {
                        case <- followerTimer.C:
                            fmt.Printf("follower = %v, get timer.C channel, time out\n", rf.me)
                            rf.mu.Lock()
                            rf.role = 1
                            rf.mu.Unlock()

                            break ExitFollower

                        case state := <-rf.rpcState:
                            fmt.Printf("follower = %v, get channel rf.rpcState\n", rf.me)
                            /*current follower get a vote request, reset the timer*/
                            if state.vState.voteDone == true {
                                fmt.Printf("follower = %v, vote to somebody\n", rf.me)
                            }
//                            followerTimer.Reset(time.Duration(timeout) * time.Millisecond)
                    }
                }
                fmt.Printf("Follower = %v, term = %v, stage end......\n", rf.me, rf.currentTerm)

            /*Current server is candidate*/
            case 1:
//                var wg sync.WaitGroup
                var numberOfVotes   int
                var rpcSucceedCount int

                rf.mu.Lock()
                rf.currentTerm++
                rf.votedFor = strconv.Itoa(rf.me)
                rf.mu.Unlock()
                numberOfVotes++     //the vote of candidate itself
                rpcSucceedCount++   //candidate itself needless to do rpc call

                fmt.Printf("Candidate = %v, term = %v, stage start......\n", rf.me, rf.currentTerm)

                rpcCandidateReplyChan := make(chan RpcCandidateReply)
                serverState   := make([]ServerState, len(rf.peers))

                electTime := GetRandamNumber(400, 500)
                fmt.Printf("electTimer = %v\n", electTime)
                candidateTimer := time.NewTimer(time.Duration(electTime) * time.Millisecond)
                defer candidateTimer.Stop()


                ExitCandidate:
                for i := 0; ; i++ {

                    select {
                        case state := <-rf.rpcState:

                            if state.isToFollower == true {
                                rf.mu.Lock()
                                rf.role = 0
                                rf.mu.Unlock()
                                fmt.Printf("candidate = %v, term = %v, because leader is leagal, candidate transit to follower\n", rf.me, rf.currentTerm)

                                break ExitCandidate
                            }
                        case <-candidateTimer.C:
                            fmt.Printf("candidate = %v, term = %v, election time out, start a new election\n", rf.me, rf.currentTerm)
                            CalculateVotes(rf, serverState, numberOfVotes)
                            break ExitCandidate
                        default:
                    }

                    select {
                        case rpcResult := <-rpcCandidateReplyChan:
                            if rpcResult.Result == true {           //previous rpc call succeed
                                fmt.Printf("candidate = %v, term = %v, follower = %v, rpc succeeded\n", rf.me, rf.currentTerm, rpcResult.ExecutedId)

                                serverState[rpcResult.ExecutedId].rpcFailCount = 0
                                serverState[rpcResult.ExecutedId].isFailure = false

                                rpcSucceedCount++
                                if rpcResult.Reply.VoteGranted == true {
                                    fmt.Printf("candidate = %v, term = %v, follower = %v voted a granted vote\n", rf.me, rf.currentTerm, rpcResult.ExecutedId)
                                    numberOfVotes++
                                } else {
                                    fmt.Printf("candidate = %v, term = %v, follower = %v did not vote\n", rf.me, rf.currentTerm, rpcResult.ExecutedId)
                                }

                                /*All server has reply the result of vote*/
                                fmt.Printf("candidate = %v, term = %v, server = %v, IsServerAllTrue = %v, ValidServerCount = %v, rpcCount = %v\n", rf.me, rf.currentTerm, rpcResult.ExecutedId, IsServerAllTrue(serverState), ValidServerCount(serverState), rpcSucceedCount)
                                if IsServerAllTrue(serverState) == true && ValidServerCount(serverState) <= rpcSucceedCount{

                                    CalculateVotes(rf, serverState, numberOfVotes)
                                    break ExitCandidate
                                }
                            } else {                            //previous rpc call failure
                                serverState[rpcResult.ExecutedId].rpcFailCount++
                                fmt.Printf("candidate = %v, term = %v, server = %v rpc failure, failCount = %v\n", rf.me, rf.currentTerm, rpcResult.ExecutedId, serverState[rpcResult.ExecutedId].rpcFailCount)
                                if serverState[rpcResult.ExecutedId].rpcFailCount >= 1 {
                                    fmt.Printf("candidate = %v, term = %v, server = %v failure times more than 5, set failure flag to it\n", rf.me, rf.currentTerm, rpcResult.ExecutedId)
                                    serverState[rpcResult.ExecutedId].isFailure = true
                                }

                                if IsServerAllTrue(serverState) == true && ValidServerCount(serverState) <= rpcSucceedCount{

                                    CalculateVotes(rf, serverState, numberOfVotes)
                                    break ExitCandidate
                                }
                                serverState[rpcResult.ExecutedId].isHandout = false         //re-transmit rpc call to the server

                                fmt.Printf("candidate = %v, term = %v, server = %v, rpc failed\n", rf.me, rf.currentTerm, rpcResult.ExecutedId)
                            }
                        default:
                    }

                    if i >= len(rf.peers) {
                        i = 0
                    }

                    if i == rf.me {
                        serverState[i].isHandout = true
                        continue
                    }


                    if serverState[i].isHandout == true || serverState[i].isFailure == true {
                        continue
                    } else {
                        serverState[i].isHandout = true
                    }
                    var args  RequestVoteArgs
                    var reply RequestVoteReply

                    args.Term = rf.currentTerm
                    args.CandidateId = rf.me
                    args.LastLogIndex = rf.commitIndex

//                    wg.Add(1)
                    go func(server int, args *RequestVoteArgs, reply *RequestVoteReply, rpcCandidateReplyChan chan RpcCandidateReply){

//                        defer wg.Done()
//                        rpcTimer = time.NewTimer(time.Duration(50) * time.Millisecond)
                        fmt.Printf("candidate = %v, rpc executedId = %v, term = %v, send rpc request start......\n", args.CandidateId, server, args.Term)
                        err := rf.sendRequestVote(server, args, reply)
                        fmt.Printf("candidate = %v, rpc executedId = %v, term = %v, send rpc request end, result = %v......\n", args.CandidateId, server, args.Term, err)

                        fmt.Printf("candidate = %v, rpc executedId = %v, term = %v, rpcReplyChan sned\n", args.CandidateId, server, args.Term)
                        rpcCandidateReplyChan <- RpcCandidateReply{reply, server, err}
                        fmt.Printf("candidate = %v, rpc executedId = %v, term = %v, rpcReplyChan sned over\n", args.CandidateId, server, args.Term)

                    }(i, &args, &reply, rpcCandidateReplyChan)
                }
//                wg.Wait()
                numberOfVotes   = 0
                rpcSucceedCount = 0
                fmt.Printf("Candidate = %v, term = %v, stage end......\n", rf.me, rf.currentTerm)


            /*Current server is leader*/
            case 2:
                fmt.Printf("leader = %v, term = %v, I am a leader now\n", rf.me, rf.currentTerm)

                var rpcSucceedCount int
                rpcLeaderReplyChan  := make(chan RpcLeaderReply)
                serverState         := make([]ServerState, len(rf.peers))

                rpcSucceedCount++

                ExitLeader:
                for {

                    fmt.Printf("leader = %v, term = %v, start a heartbeatTimer\n", rf.me, rf.currentTerm)
                    heartbeatTime := 200
                    leaderTimer   := time.NewTimer(time.Duration(heartbeatTime) * time.Millisecond)
                    defer leaderTimer.Stop()

                    select {
                        case <- leaderTimer.C:
                            fmt.Printf("leader = %v, term = %v, heartbeat timerout\n", rf.me, rf.currentTerm)

                            ExitHeartbeat:
                            for i := 0; ; i++ {

                                select {
                                    case rpcResult := <-rpcLeaderReplyChan:
                                        if rpcResult.Result == true {
                                            fmt.Printf("leader = %v, term = %v, follower = %v, rpc succeed\n", rf.me, rf.currentTerm, rpcResult.ExecutedId)
                                            rpcSucceedCount++

                                            serverState[rpcResult.ExecutedId].rpcFailCount = 0
                                            serverState[rpcResult.ExecutedId].isFailure    = false


                                            if rpcResult.Reply.Success == true {
                                                fmt.Printf("heartbeat success......\n")
                                            } else {
                                                fmt.Printf("heartbeat failure......\n")
                                            }

                                            fmt.Printf("leader = %v, term = %v, isServerAllTrue = %v, ValidServerCount = %v, rpcSucceedCount = %v\n", rf.me, rf.currentTerm, IsServerAllTrue(serverState), ValidServerCount(serverState), rpcSucceedCount)
//                                            if IsFlagAllTrue(rf.me, flagLeaderRpcCalled) == true && len(flagLeaderRpcCalled) <= rpcSucceedCount {
                                            if IsServerAllTrue(serverState) == true && ValidServerCount(serverState) <= rpcSucceedCount {
                                                fmt.Printf("leader = %v, term = %v, ValidServerCount = %v, rpcSucceedCount = %v, a heartbeat round is end, exit heartbeat\n", rf.me, rf.currentTerm, ValidServerCount(serverState), rpcSucceedCount)
                                                break ExitHeartbeat
                                            }
                                        } else {
                                            fmt.Printf("leader = %v, term = %v, isServerAllTrue = %v, ValidServerCount = %v, rpcSucceedCount = %v, rpc failure\n", rf.me, rf.currentTerm, IsServerAllTrue(serverState), ValidServerCount(serverState), rpcSucceedCount)

                                            serverState[rpcResult.ExecutedId].rpcFailCount++

                                            if serverState[rpcResult.ExecutedId].rpcFailCount >= 1 {
                                                serverState[rpcResult.ExecutedId].isFailure = true
                                            }

                                            if IsServerAllTrue(serverState) == true && ValidServerCount(serverState) <= rpcSucceedCount {
                                                break ExitHeartbeat
                                            }
                                            fmt.Printf("leader = %v, term = %v, follower = %v, rpc failure\n", rf.me, rf.currentTerm, rpcResult.ExecutedId)
                                            serverState[rpcResult.ExecutedId].isHandout = false
                                        }

                                    case state := <-rf.rpcState:
                                        fmt.Printf("leader = %v , term = %v, get rpc state, isToFollower = %v\n", rf.me, rf.currentTerm, state.isToFollower)
                                        if state.isToFollower == true {
                                            rf.mu.Lock()
                                            rf.role = 0
                                            rf.mu.Unlock()
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
                                    continue
                                }

                                if serverState[i].isHandout == true || serverState[i].isFailure == true {
                                    continue
                                } else {
                                    serverState[i].isHandout = true
                                }
                                var args  AppendEntriesArgs
                                var reply AppendEntriesReply

                                args.Term     = rf.currentTerm
                                args.LeaderId = rf.me

                                go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, rpcLeaderReplyChan chan RpcLeaderReply) {

                                    fmt.Printf("leader = %v, term = %v, rpc executedId = %v, send rpc request start......\n", args.LeaderId, args.Term, server)
                                    err := rf.sendAppendEntries(server, args, reply)
                                    fmt.Printf("leader = %v, term = %v, rpc executedId = %v, send rpc request end, result = %v......\n", args.LeaderId, args.Term, server, err)

                                     fmt.Printf("leader = %v, term = %v, rpc executedId = %v, rpcReplyChan sned\n", args.LeaderId, args.Term, server)
                                     rpcLeaderReplyChan <- RpcLeaderReply{reply, server, err}
                                     fmt.Printf("leader = %v, term = %v, rpc executedId = %v, rpcReplyChan end\n", args.LeaderId, args.Term, server)
                                }(i, &args, &reply, rpcLeaderReplyChan)
                            }
                            fmt.Printf("leader = %v, term = %v, heartbeat operation end, reset the leaderTimer\n", rf.me, rf.currentTerm)
                            leaderTimer.Reset(time.Duration(heartbeatTime) * time.Millisecond)
                    }//select
                }//for
        }//switch
    }//for
}//serverHandler

func GetRandamNumber(min, max int) int {

    rand.Seed(time.Now().UnixNano())
    randNum := rand.Intn(max - min) + min
    return randNum
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
func IsGetMajorVote(numberOfVote, total int) bool {
    if numberOfVote*2 >= total {
        return true
    } else {
        return false
    }

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
func CalculateVotes(rf *Raft, server []ServerState, numberOfVotes int) bool {

    fmt.Printf("candidate = %v, term = %v, numberOfVotes = %v, ValidServerCount %v, calculateVoutes start\n", rf.me, rf.currentTerm, numberOfVotes, ValidServerCount(server))
    if IsGetMajorVote(numberOfVotes, ValidServerCount(server)) {
        fmt.Printf("candidate = %v, term = %v, get majority votes\n", rf.me, rf.currentTerm)
        rf.mu.Lock()
        rf.role = 2
        rf.mu.Unlock()
        return true
    } else {
        fmt.Printf("candidate = %v, term = %v, get less votes\n", rf.me, rf.currentTerm)
        rf.mu.Lock()
        rf.role = 0
        rf.mu.Unlock()
        return false
    }
    fmt.Printf("candidate = %v, term = %v, numberOfVotes = %v, validServerCount = %v, calculateVoutes end\n", rf.me, rf.currentTerm, numberOfVotes, ValidServerCount(server))
    return false
}
