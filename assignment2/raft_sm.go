package main

import (
	"fmt"
	"sync"
	"time"
	"reflect"
)

type LogEntry struct {
	Data      []byte
	Committed bool
	Term      int
	LogIndex  int
}

type AppendMsg struct {
	Data []byte
}

type AppendEntriesReq struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesResp struct {
	From       int
	Term       int
	MatchIndex int
	Success    bool
}

type VoteReq struct {
	From         int
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type VoteResp struct {
	From        int
	Term        int
	VoteGranted bool
}

type StateMachine struct {
	CurrentTerm int
	LeaderID    int
	ServerID    int
	State       string
	VoteGranted map[int]bool
	VotedFor    int
	NextIndex   map[int]int
	MatchIndex  map[int]int
	CommitIndex int
	Log         []LogEntry
	clientCh    chan interface{}
	netCh       chan interface{}
	actionCh    chan interface{}
	Timer       *time.Timer
	Mutex       sync.RWMutex
	PeerChan	chan interface{}
}

func (sm *StateMachine) doVoteReq() {
	switch sm.State {
		case "follower":
			if sm.CurrentTerm < msg.term:
				sm.CurrentTerm = msg.term
				sm.votedFor = nil
				# increment election alarm?
			if sm.CurrentTerm == msg.term && 
				(sm.votedFor == nil or sm.votedFor == msg.candidateId):
				if (msg.lastLogTerm > sm.logTerm[-1] or 
					(msg.lastLogTerm == sm.logTerm[-1] and 
						msg.lastLogIndex >= len(sm.log))):
					sm.CurrentTerm = msg.term
					sm.votedFor = msg.candidateId
					action = Send(msg.from, VoteResp(sm.CurrentTerm, voteGranted=yes))
			else: #reject vote:
				action = Send(msg.from, VoteResp(sm.CurrentTerm, voteGranted=no))

			Alarm(time.now() + rand(1.0, 2.0) * ELECTION_TIMEOUT)
					
		case "candidate":
			
		case "leader":

	}
}

func (sm *StateMachine) eventLoop() {
	for {
		select {
		case appendMsg := <- sm.clientCh:
			t := reflect.TypeOf(appendMsg)
			fmt.Println(t)

		case peerMsg := <- sm.netCh:
			t := reflect.TypeOf(peerMsg)
			switch t.Name() {
				case "AppendEntriesReq":
					go sm.doAppendEntriesReq()
				case "AppendEntriesResp":
					go sm.doAppendEntriesResp()
				case "VoteResp": 
					go sm.doVoteResp()
				case "VoteReq":
					go sm.doVoteReq()
			}
		}
	}
}

func NewStateMachine() *StateMachine {
	sm := StateMachine{
		VoteGranted: make(map[int]bool),
		NextIndex:   make(map[int]int),
		MatchIndex:  make(map[int]int),
		clientCh:       make(chan interface{}),
		netCh:       make(chan interface{}),
		actionCh:       make(chan interface{}),
	}
	return &sm
}

func () {

}

func main() {
	sm1 := NewStateMachine()
	go sm1.eventLoop()

	sm2 := NewStateMachine()
	go sm2.eventLoop()

	sm1.PeerChan = sm2.netCh
	sm2.PeerChan = sm1.netCh

	vr := VoteResp{From: 1, Term: 2, VoteGranted: true}

	sm2.netCh <- vr
	sm1.PeerChan <- vr
	time.Sleep(10*time.Second)
	// go func() {
	// 	vR := <-sm.netCh
	// 	fmt.Println(vR)
	// }()
	// sm.netCh <- vr
}
