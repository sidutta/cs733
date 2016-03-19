package main

import (
	// "log"
	"encoding/json"
	// "fmt"
	"github.com/cs733-iitb/log"
	"github.com/syndtr/goleveldb/leveldb"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"time"
)

type Alarm struct {
	//sender int
	Delay float64 // delay in milliseconds
}

type Send struct {
	//sender int
	PeerID int // server to send to
	Event  interface{}
}

type Commit struct {
	//sender int
	Index int
	Data  []byte
	Err   error
}

type AppendEntry struct {
	Command []byte
}

type Timeout struct {
}

type SaveTerm struct {
	Term int
}

type SaveVotedFor struct {
	VotedFor int
}

type LogEntry struct {
	Data []byte
	Term int
}

type AppendEntriesReq struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	CommitIndex  int
}

type AppendEntriesResp struct {
	From       int
	Term       int
	MatchIndex int
	Success    bool
}

type VoteReq struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type VoteResp struct {
	ServerID    int
	Term        int
	VoteGranted bool
}

type LogTransfer struct {
	Index  int
	LEntry LogEntry
}

func (sm *StateMachine) LogStore(Index int, Entry []byte, EntryTerm int) {
	//fmt.Println("logstore called", sm.ServerID)
	for i := 0; i < Index-len(sm.Log)+1; i++ {
		sm.Log = append(sm.Log, LogEntry{nil, 0})
	}
	sm.Log[Index].Data = Entry
	sm.Log[Index].Term = EntryTerm
	// fmt.Println("lt calling", sm.ServerID, Index, sm.Log[Index])

	sm.actionCh <- LogTransfer{Index, sm.Log[Index]}
	// fmt.Println("lt success")
}

type StateMachine struct {
	CurrentTerm       int
	LastLogIndex      int
	LastLogTerm       int
	LeaderID          int
	ServerID          int
	State             string
	VoteGranted       map[int]bool
	VotedFor          int
	NextIndex         map[int]int
	MatchIndex        map[int]int
	Log               []LogEntry
	updateCh          chan interface{}
	netCh             chan interface{}
	actionCh          chan interface{}
	Timer             *time.Timer
	Mutex             sync.RWMutex
	PeerIds           []int
	CommitIndex       int
	HEARTBEAT_TIMEOUT float64
	ELECTION_TIMEOUT  float64
}

// const PATH = "/Users/Siddhartha/Documents/Academics/8thSem/cs733/assignment3"

// const PATH = "/Users/Siddhartha/Documents/Academics/8thSem/go/src/github.com/sidutta/cs733/assignment3"

const PATH = "./" //github.com/sidutta/cs733/assignment3

var NUMBER_OF_NODES int

func NewStateMachine(id int, peerIds []int, electionTimeout float64, heartbeatTimeout float64, lg *log.Log) *StateMachine {
	sm := StateMachine{
		ServerID:     id,
		State:        "follower",
		VoteGranted:  make(map[int]bool),
		NextIndex:    make(map[int]int),
		MatchIndex:   make(map[int]int),
		updateCh:     make(chan interface{}, 250000),
		netCh:        make(chan interface{}, 250000),
		actionCh:     make(chan interface{}, 250000),
		LastLogIndex: -1,
		CommitIndex:  -1,
		PeerIds:      peerIds,
		Log:          make([]LogEntry, 0), //see
	}

	sm.ELECTION_TIMEOUT = electionTimeout
	sm.HEARTBEAT_TIMEOUT = heartbeatTimeout
	NUMBER_OF_NODES = len(peerIds)

	CurrentTermDB, _ := leveldb.OpenFile(PATH+"/currentTerm", nil)
	defer CurrentTermDB.Close()

	termStr, err := CurrentTermDB.Get([]byte(strconv.FormatInt(int64(sm.ServerID), 10)), nil)
	if err == nil {
		sm.CurrentTerm, _ = strconv.Atoi(string(termStr))
	} else {
		sm.CurrentTerm = int(0)
	}

	VotedForDB, _ := leveldb.OpenFile(PATH+"/votedFor", nil)
	defer VotedForDB.Close()

	votedForStr, err := VotedForDB.Get([]byte(strconv.Itoa(sm.ServerID)), nil)
	if err == nil {
		sm.VotedFor, _ = strconv.Atoi(string(votedForStr))
	} else {
		sm.VotedFor = int(0)
	}

	lastIndex := int(lg.GetLastIndex())
	log.Println("last index is ", lastIndex)
	if lastIndex != -1 {
		var i int
		for i = 0; i <= lastIndex; i++ {
			b, _ := lg.Get(int64(i)) //see
			// entry := b.([]byte).(LogEntry)
			var entry LogEntry
			json.Unmarshal(b.([]byte), &entry)
			sm.Log = append(sm.Log, entry) //see
		}
		sm.LastLogIndex = lastIndex
		sm.LastLogTerm = sm.getLogTerm(lastIndex)
	}

	return &sm
}

func aggregate(VoteGranted map[int]bool) int {
	total := 0
	for _, vote := range VoteGranted {
		if vote {
			total = total + 1
		}
	}
	return total
}

func (sm *StateMachine) getLogTerm(i int) int {
	if i >= 0 {
		return sm.Log[i].Term
	} else {
		return 0
	}

}

func min(x int, y int) int {
	var c int
	if c = y; c > x {
		c = x
	}
	return c
}

func max(x int, y int) int {
	var c int
	if c = y; c < x {
		c = x
	}
	return c
}

func (sm *StateMachine) VoteReq(msg VoteReq) {
	// //fmt.Println(sm.ServerID, "reached votereq")
	if sm.CurrentTerm < msg.Term {
		sm.State = "follower"
		sm.CurrentTerm = msg.Term
		sm.VotedFor = 0
		sm.updateCh <- SaveVotedFor{sm.VotedFor}
		sm.actionCh <- Alarm{Delay: (float64(1.0) + rand.Float64()) * sm.ELECTION_TIMEOUT}
	}

	switch sm.State {
	case "follower", "candidate", "leader":
		if sm.CurrentTerm == msg.Term &&
			(sm.VotedFor == 0 ||
				sm.VotedFor == msg.CandidateId) &&
			(msg.LastLogTerm > sm.getLogTerm(len(sm.Log)-1) ||
				(msg.LastLogTerm == sm.getLogTerm(len(sm.Log)-1) &&
					msg.LastLogIndex >= len(sm.Log)-1)) {
			sm.CurrentTerm = msg.Term
			sm.VotedFor = msg.CandidateId
			sm.updateCh <- SaveVotedFor{sm.VotedFor}
			sm.actionCh <- Send{msg.CandidateId, VoteResp{sm.ServerID, sm.CurrentTerm, true}}
			sm.actionCh <- Alarm{(float64(1.0) + rand.Float64()) * sm.ELECTION_TIMEOUT}
			//fmt.Println(sm.ServerID, "+ voted for", sm.VotedFor, "when candidate", msg.CandidateId)
		} else {
			sm.actionCh <- Send{msg.CandidateId, VoteResp{sm.ServerID, sm.CurrentTerm, false}}
			//fmt.Println(sm.ServerID, "- voted for", sm.VotedFor, "when candidate", msg.CandidateId)
			//fmt.Println(msg.LastLogTerm, sm.getLogTerm(len(sm.Log)-1),
			//msg.LastLogTerm, sm.getLogTerm(len(sm.Log)-1),
			//msg.LastLogIndex, len(sm.Log)-1)
		}

	}

}

func (sm *StateMachine) ProcessEvent() {
	// for {
	// var ev interface{}
	select {
	case ev := <-sm.netCh:
		// //fmt.Println("came 0")
		switch ev.(type) {
		case VoteReq:
			//fmt.Println("came 1")
			sm.VoteReq(ev.(VoteReq))
		case VoteResp:
			// //fmt.Println("came 2")
			sm.VoteResp(ev.(VoteResp))
		case AppendEntriesReq:
			sm.AppendEntriesReq(ev.(AppendEntriesReq))
		case AppendEntriesResp:
			sm.AppendEntriesResp(ev.(AppendEntriesResp))
		case AppendEntry:
			sm.Append(ev.(AppendEntry))
		case Timeout:
			sm.Timeout()
		}
	}
	// }
}

func (sm *StateMachine) VoteResp(msg VoteResp) {
	if sm.CurrentTerm < msg.Term {
		sm.State = "follower"
		sm.CurrentTerm = msg.Term
		sm.VotedFor = 0
		sm.updateCh <- SaveVotedFor{sm.VotedFor}

	}
	//fmt.Println("not yet")
	switch sm.State {

	case "follower":

	case "candidate":

		if sm.CurrentTerm == msg.Term {
			sm.VoteGranted[msg.ServerID] = msg.VoteGranted
			if aggregate(sm.VoteGranted) > NUMBER_OF_NODES/2 {
				sm.State = "leader"
				//fmt.Println("i m neta bruah term:", sm.CurrentTerm)
				sm.LeaderID = sm.ServerID
				for _, peerId := range sm.PeerIds {
					if peerId != sm.ServerID {
						sm.NextIndex[peerId] = len(sm.Log)
						sm.MatchIndex[peerId] = -1

						sm.actionCh <- Send{peerId, AppendEntriesReq{sm.CurrentTerm, sm.ServerID, sm.LastLogIndex, sm.LastLogTerm, nil, sm.CommitIndex}}
					}
				}
				sm.actionCh <- Alarm{(float64(1.0) + rand.Float64()) * sm.HEARTBEAT_TIMEOUT}
			}
		} else {
			//fmt.Println("not yet")
		}
	case "leader":

	}
}

func (sm *StateMachine) AppendEntriesReq(msg AppendEntriesReq) {
	if sm.CurrentTerm < msg.Term {
		sm.State = "follower"
		sm.CurrentTerm = msg.Term
		sm.VotedFor = 0
		sm.updateCh <- SaveVotedFor{sm.VotedFor}

		sm.actionCh <- Alarm{(float64(1.0) + rand.Float64()) * sm.ELECTION_TIMEOUT}
	}
	switch sm.State {
	case "follower", "candidate", "leader":
		if sm.CurrentTerm > msg.Term {
			sm.actionCh <- Send{msg.LeaderId, AppendEntriesResp{sm.ServerID, sm.CurrentTerm, -1, false}}
		} else {
			sm.LeaderID = msg.LeaderId
			sm.State = "follower"
			success := (msg.PrevLogIndex == -1 || (msg.PrevLogIndex <= len(sm.Log)-1 && sm.getLogTerm(msg.PrevLogIndex) == msg.PrevLogTerm))
			index := -1
			//fmt.Println("was here", sm.CommitIndex, msg.CommitIndex)
			// if msg.PrevLogIndex <= len(sm.Log)-1 {
			// 				fmt.Println(msg.PrevLogIndex, msg.PrevLogIndex, len(sm.Log)-1, sm.getLogTerm(msg.PrevLogIndex), msg.PrevLogTerm, success, sm.ServerID, msg.LeaderId)
			// 			} else {
			// 				fmt.Println(msg.PrevLogIndex, msg.PrevLogIndex, len(sm.Log)-1, msg.PrevLogTerm, success, sm.ServerID, msg.LeaderId)
			// 			}
			// //fmt.Println(msg.PrevLogIndex, msg.PrevLogIndex, len(sm.Log)-1, sm.getLogTerm(msg.PrevLogIndex), msg.PrevLogTerm)
			if success {
				index = msg.PrevLogIndex
				for j := 0; j < len(msg.Entries); j++ {
					index++
					//fmt.Println("lscall", msg.Entries)
					sm.LogStore(index, msg.Entries[j].Data, msg.Term)
					// sm.actionCh <- LogStore{index, msg.Entries[j].Data}
				}
				sm.LastLogIndex = index
				sm.LastLogTerm = sm.CurrentTerm
				// fmt.Println("commit", sm.CommitIndex, msg.CommitIndex, index, sm.ServerID)
				if sm.CommitIndex < msg.CommitIndex {
					sm.CommitIndex = min(msg.CommitIndex, index)

					if sm.CommitIndex != -1 {
						// fmt.Println("aaaaaaa1", sm.CommitIndex, sm.Log)
						sm.actionCh <- Commit{sm.CommitIndex, sm.Log[sm.CommitIndex].Data, nil}
					} else {
						//fmt.Println("aaaaaaa2")
						sm.actionCh <- Commit{sm.CommitIndex, nil, nil}
					}
				}

			} else {
				index = -1
			}
			sm.actionCh <- Send{msg.LeaderId, AppendEntriesResp{sm.ServerID, sm.CurrentTerm, index, success}}
			sm.actionCh <- Alarm{(1.0 + rand.Float64()) * sm.ELECTION_TIMEOUT}
		}

	}
}

func (sm *StateMachine) AppendEntriesResp(msg AppendEntriesResp) {
	if sm.CurrentTerm < msg.Term {
		sm.State = "follower"
		sm.CurrentTerm = msg.Term
		sm.VotedFor = 0
		sm.updateCh <- SaveVotedFor{sm.VotedFor}

		sm.actionCh <- Alarm{Delay: (1.0 + rand.Float64()) * sm.ELECTION_TIMEOUT}
	}

	switch sm.State {
	case "follower", "candidate":

	case "leader":
		if sm.CurrentTerm == msg.Term {
			if !msg.Success {

				sm.NextIndex[msg.From] = max(0, sm.NextIndex[msg.From]-1)
				//fmt.Println("sending2", sm.Log[sm.NextIndex[msg.From]:sm.LastLogIndex+1])
				sm.actionCh <- Send{msg.From, AppendEntriesReq{sm.CurrentTerm, sm.ServerID, sm.NextIndex[msg.From] - 1, sm.getLogTerm(sm.NextIndex[msg.From] - 1), sm.Log[sm.NextIndex[msg.From] : sm.LastLogIndex+1], sm.CommitIndex}}
			} else {
				sm.MatchIndex[msg.From] = msg.MatchIndex
				sm.NextIndex[msg.From] = msg.MatchIndex + 1

				if sm.MatchIndex[msg.From] < len(sm.Log)-1 {
					//fmt.Println("sending", sm.Log[sm.NextIndex[msg.From]:len(sm.Log)], sm.NextIndex[msg.From], len(sm.Log), sm.Log[len(sm.Log)-1])
					sm.actionCh <- Send{msg.From, AppendEntriesReq{sm.CurrentTerm, sm.ServerID, sm.MatchIndex[msg.From], sm.getLogTerm(sm.MatchIndex[msg.From]), sm.Log[sm.NextIndex[msg.From]:len(sm.Log)], sm.CommitIndex}}
				}

				var indices []int
				indices = append(indices, len(sm.Log)-1)
				for _, peerId := range sm.PeerIds {
					indices = append(indices, sm.MatchIndex[peerId])
				}
				sort.Sort(sort.IntSlice(indices))
				n := indices[NUMBER_OF_NODES/2]
				if sm.CommitIndex < n && sm.getLogTerm(n) == sm.CurrentTerm {
					sm.CommitIndex = n
					if sm.CommitIndex != -1 {
						//fmt.Println("aaaaaaa3", sm.CommitIndex, sm.Log)
						sm.actionCh <- Commit{sm.CommitIndex, sm.Log[sm.CommitIndex].Data, nil}
					} else {
						//fmt.Println("aaaaaaa4")
						sm.actionCh <- Commit{sm.CommitIndex, nil, nil}
					}
				}
			}
		}
	}
}

func (sm *StateMachine) Append(msg AppendEntry) {
	switch sm.State {
	case "follower":
		sm.actionCh <- Send{sm.LeaderID, msg}
	case "candidate":
		sm.actionCh <- Send{sm.LeaderID, msg}
	case "leader":
		sm.LastLogIndex++
		sm.LastLogTerm = sm.CurrentTerm
		//fmt.Println("zxc", msg)
		sm.LogStore(len(sm.Log), msg.Command, sm.CurrentTerm)
	}
}

func (sm *StateMachine) Timeout() {
	switch sm.State {
	case "follower":
		sm.LeaderID = 0
		sm.State = "candidate"
		sm.CurrentTerm++
		// fmt.Println("called by", sm.ServerID, sm.CurrentTerm)

		sm.updateCh <- SaveTerm{sm.CurrentTerm}
		sm.VotedFor = sm.ServerID
		sm.updateCh <- SaveVotedFor{sm.VotedFor}

		sm.VoteGranted[sm.ServerID] = true
		for _, peerID := range sm.PeerIds {
			if peerID != sm.ServerID {
				sm.VoteGranted[sm.ServerID] = false
				sm.actionCh <- Send{peerID, VoteReq{sm.CurrentTerm, sm.ServerID, sm.LastLogIndex, sm.LastLogTerm}}
				//fmt.Println(sm.ServerID, "sending", peerID)
			}
		}
		sm.actionCh <- Alarm{Delay: (1.0 + rand.Float64()) * sm.ELECTION_TIMEOUT}

	case "candidate":
		sm.LeaderID = 0
		sm.CurrentTerm++
		// fmt.Println("called by", sm.ServerID, "as cand", sm.CurrentTerm)
		sm.updateCh <- SaveTerm{sm.CurrentTerm}
		sm.VotedFor = sm.ServerID
		sm.updateCh <- SaveVotedFor{sm.VotedFor}

		sm.VoteGranted[sm.ServerID] = true
		for _, peerID := range sm.PeerIds {
			if peerID != sm.ServerID {
				sm.VoteGranted[sm.ServerID] = false
				sm.actionCh <- Send{peerID, VoteReq{sm.CurrentTerm, sm.ServerID, sm.LastLogIndex, sm.LastLogTerm}}
			}
		}
		sm.actionCh <- Alarm{(1.0 + rand.Float64()) * sm.ELECTION_TIMEOUT}

	case "leader":
		for _, peerID := range sm.PeerIds {
			if peerID != sm.ServerID {
				sm.actionCh <- Send{peerID, AppendEntriesReq{sm.CurrentTerm, sm.ServerID, sm.LastLogIndex, sm.LastLogTerm, nil, sm.CommitIndex}}
			}
		}
		// fmt.Println("sending heartbeat")
		sm.actionCh <- Alarm{(1.0 + rand.Float64()) * sm.HEARTBEAT_TIMEOUT}

	}
}

func main() {

}
