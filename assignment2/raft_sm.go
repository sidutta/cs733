package main

import (
	// "log"
	"math/rand"
	"sort"
	"sync"
	"time"
)

type Alarm struct {
	//sender int
	delay float64 // delay in milliseconds
}

type Send struct {
	//sender int
	peerID int // server to send to
	event  interface{}
}

type Commit struct {
	//sender int
	index int
	data  LogEntry
	err   string
}

type LogStore struct {
	//sender  int
	index int
	data  []byte
}

type AppendEntry struct {
	command []byte
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

type StateMachine struct {
	CurrentTerm  int
	LastLogIndex int
	LastLogTerm  int
	LeaderID     int
	ServerID     int
	State        string
	VoteGranted  map[int]bool
	VotedFor     int
	NextIndex    map[int]int
	MatchIndex   map[int]int
	Log          []LogEntry
	updateCh     chan interface{}
	netCh        chan interface{}
	actionCh     chan interface{}
	Timer        *time.Timer
	Mutex        sync.RWMutex
	PeerIds      []int
	CommitIndex  int
}

const HEARTBEAT_TIMEOUT = 50
const ELECTION_TIMEOUT = 150
const NUMBER_OF_NODES = 5

func NewStateMachine(id int) *StateMachine {
	sm := StateMachine{
		ServerID:     id,
		State:        "follower",
		VoteGranted:  make(map[int]bool),
		NextIndex:    make(map[int]int),
		MatchIndex:   make(map[int]int),
		updateCh:     make(chan interface{}, 10),
		netCh:        make(chan interface{}, 10),
		actionCh:     make(chan interface{}, 10),
		LastLogIndex: -1,
		CommitIndex:  -1,
		// Timer:        e,
	}
	return &sm
}

func (sm *StateMachine) AddPeer(id int) {
	sm.PeerIds = append(sm.PeerIds, id)
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
		return -1
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

	if sm.CurrentTerm < msg.Term {
		sm.State = "follower"
		sm.CurrentTerm = msg.Term
		sm.VotedFor = 0
		// sm.updateCh <- SaveVotedFor{sm.VotedFor}
		sm.actionCh <- Alarm{delay: (1.0 + rand.Float64()) * ELECTION_TIMEOUT}
	}

	switch sm.State {
	case "follower", "candidate", "leader":
		if sm.CurrentTerm == msg.Term && (sm.VotedFor == 0 || sm.VotedFor == msg.CandidateId) && (msg.LastLogTerm > sm.getLogTerm(len(sm.Log)-1) || (msg.LastLogTerm == sm.getLogTerm(len(sm.Log)-1) && msg.LastLogIndex >= len(sm.Log)-1)) {
			sm.CurrentTerm = msg.Term
			sm.VotedFor = msg.CandidateId
			// sm.updateCh <- SaveVotedFor{sm.VotedFor}
			sm.actionCh <- Send{msg.CandidateId, VoteResp{sm.ServerID, sm.CurrentTerm, true}}
			sm.actionCh <- Alarm{delay: (1.0 + rand.Float64()) * ELECTION_TIMEOUT}

		} else {
			sm.actionCh <- Send{msg.CandidateId, VoteResp{sm.ServerID, sm.CurrentTerm, false}}
		}

	}

}

func (sm *StateMachine) ProcessEvent() {
	// for {
	// var ev interface{}
	select {
	case ev := <-sm.netCh:
		switch ev.(type) {
		case VoteReq:
			sm.VoteReq(ev.(VoteReq))
		case VoteResp:
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
		// sm.updateCh <- SaveVotedFor{sm.VotedFor}

	}
	switch sm.State {

	case "follower":

	case "candidate":

		if sm.CurrentTerm == msg.Term {
			sm.VoteGranted[msg.ServerID] = msg.VoteGranted
			if aggregate(sm.VoteGranted) > NUMBER_OF_NODES/2 {
				sm.State = "leader"
				sm.LeaderID = sm.ServerID
				for _, peerId := range sm.PeerIds {
					if peerId != sm.ServerID {
						sm.NextIndex[peerId] = len(sm.Log)
						sm.MatchIndex[peerId] = -1

						sm.actionCh <- Send{peerId, AppendEntriesReq{sm.CurrentTerm, sm.ServerID, sm.LastLogIndex, sm.LastLogTerm, nil, sm.CommitIndex}}
					}
				}
				sm.actionCh <- Alarm{delay: (1.0 + rand.Float64()) * HEARTBEAT_TIMEOUT}
			}
		}
	case "leader":

	}
}

func (sm *StateMachine) AppendEntriesReq(msg AppendEntriesReq) {
	if sm.CurrentTerm < msg.Term {
		sm.State = "follower"
		sm.CurrentTerm = msg.Term
		sm.VotedFor = 0
		// sm.updateCh <- SaveVotedFor{sm.VotedFor}

		sm.actionCh <- Alarm{delay: (1.0 + rand.Float64()) * ELECTION_TIMEOUT}
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
			if success {
				index = msg.PrevLogIndex
				for j := 0; j < len(msg.Entries); j++ {
					index++
					sm.actionCh <- LogStore{index, msg.Entries[j].Data}
				}
				sm.LastLogIndex = index
				sm.LastLogTerm = sm.CurrentTerm
				sm.CommitIndex = min(msg.CommitIndex, index)
			} else {
				index = -1
			}
			sm.actionCh <- Send{msg.LeaderId, AppendEntriesResp{sm.ServerID, sm.CurrentTerm, index, success}}
			sm.actionCh <- Alarm{delay: (1.0 + rand.Float64()) * ELECTION_TIMEOUT}
		}

	}
}

func (sm *StateMachine) AppendEntriesResp(msg AppendEntriesResp) {
	if sm.CurrentTerm < msg.Term {
		sm.State = "follower"
		sm.CurrentTerm = msg.Term
		sm.VotedFor = 0
		// sm.updateCh <- SaveVotedFor{sm.VotedFor}

		sm.actionCh <- Alarm{delay: (1.0 + rand.Float64()) * ELECTION_TIMEOUT}
	}

	switch sm.State {
	case "follower", "candidate":

	case "leader":
		if sm.CurrentTerm == msg.Term {
			if !msg.Success {
				sm.NextIndex[msg.From] = max(0, sm.NextIndex[msg.From]-1)
				sm.actionCh <- Send{msg.From, AppendEntriesReq{sm.CurrentTerm, sm.ServerID, sm.NextIndex[msg.From] - 1, sm.getLogTerm(sm.NextIndex[msg.From] - 1), sm.Log[sm.NextIndex[msg.From] : sm.LastLogIndex+1], sm.CommitIndex}}
			} else {
				sm.MatchIndex[msg.From] = msg.MatchIndex
				sm.NextIndex[msg.From] = msg.MatchIndex + 1
				if sm.MatchIndex[msg.From] < len(sm.Log)-1 {
					sm.actionCh <- Send{msg.From, AppendEntriesReq{sm.CurrentTerm, sm.ServerID, sm.MatchIndex[msg.From], sm.getLogTerm(sm.MatchIndex[msg.From]), sm.Log[sm.NextIndex[msg.From]:len(sm.Log)], sm.CommitIndex}}
				}

				var indices []int
				indices = append(indices, len(sm.Log)-1)
				for _, peerId := range sm.PeerIds {
					indices = append(indices, sm.MatchIndex[peerId])
				}
				sort.Sort(sort.IntSlice(indices))
				n := indices[NUMBER_OF_NODES/2]
				if sm.getLogTerm(n) == sm.CurrentTerm {
					sm.CommitIndex = n
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
		sm.actionCh <- LogStore{len(sm.Log), msg.command}
	}
}

func (sm *StateMachine) Timeout() {
	switch sm.State {
	case "follower":
		sm.State = "candidate"
		sm.CurrentTerm++
		sm.updateCh <- SaveTerm{sm.CurrentTerm}
		sm.VotedFor = sm.ServerID
		// sm.updateCh <- SaveVotedFor{sm.VotedFor}

		sm.VoteGranted[sm.ServerID] = true
		for _, peerID := range sm.PeerIds {
			if peerID != sm.ServerID {
				sm.VoteGranted[sm.ServerID] = false
				sm.actionCh <- Send{peerID, VoteReq{sm.CurrentTerm, sm.ServerID, sm.LastLogIndex, sm.LastLogTerm}}
			}
		}
		sm.actionCh <- Alarm{delay: (1.0 + rand.Float64()) * ELECTION_TIMEOUT}

	case "candidate":
		sm.CurrentTerm++
		sm.updateCh <- SaveTerm{sm.CurrentTerm}
		sm.VotedFor = sm.ServerID
		// sm.updateCh <- SaveVotedFor{sm.VotedFor}

		sm.VoteGranted[sm.ServerID] = true
		for _, peerID := range sm.PeerIds {
			if peerID != sm.ServerID {
				sm.VoteGranted[sm.ServerID] = false
				sm.actionCh <- Send{peerID, VoteReq{sm.CurrentTerm, sm.ServerID, sm.LastLogIndex, sm.LastLogTerm}}
			}
		}
		sm.actionCh <- Alarm{delay: (1.0 + rand.Float64()) * ELECTION_TIMEOUT}

	case "leader":
		for _, peerID := range sm.PeerIds {
			if peerID != sm.ServerID {
				sm.actionCh <- Send{peerID, AppendEntriesReq{sm.CurrentTerm, sm.ServerID, sm.LastLogIndex, sm.LastLogTerm, nil, sm.CommitIndex}}
			}
		}
		sm.actionCh <- Alarm{delay: (1.0 + rand.Float64()) * HEARTBEAT_TIMEOUT}

	}
}

func main() {
}
