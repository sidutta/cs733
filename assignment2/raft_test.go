package main

import (
	"log"
	"reflect"
	"strconv"
	"testing"
)

func (sm *StateMachine) LogStore(index int, entry LogEntry) {
	if index < len(sm.Log) {
		sm.Log[index] = entry
		sm.LastLogIndex = index
		sm.LastLogTerm = sm.CurrentTerm
	} else if index == len(sm.Log) {
		sm.Log = append(sm.Log, entry)
		sm.LastLogIndex = len(sm.Log) - 1
		sm.LastLogTerm = sm.CurrentTerm
	} else {
		log.Println("invalid index for logstore")
	}
}

func errorCheck(t *testing.T, expected string, response string, error string) {
	if expected != response {
		log.Println("Expected : " + expected + " Got : " + response + " Error : " + error)
	}
}

func initialize(testserver *StateMachine) {
	testserver.AddPeer(2)
	testserver.AddPeer(3)
	testserver.AddPeer(4)
	testserver.AddPeer(5)
}

func Test1(t *testing.T) {
	followerTimesout(t)
	candidateTimesout(t)
	leaderTimesout(t)
	candidateSteppingDownOnVoteResp(t)
	leaderSteppingDownOnVoteResp(t)
	leadershipElection(t)
	appendEntriesReqtoFollower(t)
	appendEntriesReqtoCandidate(t)
	// appendEntriesReqtoLeader(t)
	clientToFollower(t)
	clientToCandidate(t)
	clientToLeader(t)
	voteReq(t)
	appendEntriesResponse(t)
}

func clientToFollower(t *testing.T) {
	// leader has lower term
	var testserver *StateMachine
	testserver = NewStateMachine(1)
	initialize(testserver)
	testserver.State = "follower"
	testserver.CurrentTerm = 3
	testserver.LeaderID = 4
	testserver.netCh <- AppendEntry{[]byte{'s', 'l', 'e', 'e', 'p'}}
	testserver.ProcessEvent()

	res := <-testserver.actionCh
	errorCheck(t, "Send", reflect.TypeOf(res).Name(), "3")
	errorCheck(t, strconv.Itoa(4), strconv.Itoa(res.(Send).peerID), "3")
	errorCheck(t, "AppendEntry", reflect.TypeOf(res.(Send).event).Name(), "3")
	errorCheck(t, "sleep", string(res.(Send).event.(AppendEntry).command[:len(res.(Send).event.(AppendEntry).command)]), "3")

}

func clientToCandidate(t *testing.T) {
	// leader has lower term
	var testserver *StateMachine
	testserver = NewStateMachine(1)
	initialize(testserver)
	testserver.State = "candidate"
	testserver.CurrentTerm = 3
	testserver.LeaderID = 4
	testserver.netCh <- AppendEntry{[]byte{'s', 'l', 'e', 'e', 'p'}}
	testserver.ProcessEvent()

	res := <-testserver.actionCh
	errorCheck(t, "Send", reflect.TypeOf(res).Name(), "3")
	errorCheck(t, strconv.Itoa(4), strconv.Itoa(res.(Send).peerID), "3")
	errorCheck(t, "AppendEntry", reflect.TypeOf(res.(Send).event).Name(), "3")
	errorCheck(t, "sleep", string(res.(Send).event.(AppendEntry).command[:len(res.(Send).event.(AppendEntry).command)]), "3")

}

func clientToLeader(t *testing.T) {
	// leader has lower term
	var testserver *StateMachine
	testserver = NewStateMachine(1)
	initialize(testserver)
	testserver.State = "leader"
	testserver.CurrentTerm = 3
	testserver.LeaderID = 4
	testserver.netCh <- AppendEntry{[]byte{'s', 'l', 'e', 'e', 'p'}}
	testserver.ProcessEvent()

	res := <-testserver.actionCh

	errorCheck(t, "LogStore", reflect.TypeOf(res).Name(), "18")
	errorCheck(t, "sleep", string(res.(LogStore).data[:len(res.(LogStore).data)]), "3")
}

func followerTimesout(t *testing.T) {
	var testserver *StateMachine
	testserver = NewStateMachine(1)
	initialize(testserver)
	testserver.State = "follower"
	oldTerm := testserver.CurrentTerm
	timeoutEvent := Timeout{}
	testserver.netCh <- timeoutEvent
	testserver.ProcessEvent()
	newTerm := <-testserver.updateCh
	errorCheck(t, strconv.Itoa(1+oldTerm), strconv.Itoa(newTerm.(SaveTerm).Term), "13")

	errorCheck(t, "candidate", testserver.State, "state didn't change")

	for i := 2; i <= 5; i++ {
		res := <-testserver.actionCh
		errorCheck(t, "Send", reflect.TypeOf(res).Name(), "12")
		errorCheck(t, strconv.Itoa(i), strconv.Itoa(res.(Send).peerID), "13")
		errorCheck(t, "VoteReq", reflect.TypeOf(res.(Send).event).Name(), "13b")
	}
	res := <-testserver.actionCh
	errorCheck(t, "Alarm", reflect.TypeOf(res).Name(), "14")
}

func candidateTimesout(t *testing.T) {
	var testserver *StateMachine
	testserver = NewStateMachine(1)
	initialize(testserver)
	testserver.State = "candidate"
	oldTerm := testserver.CurrentTerm
	timeoutEvent := Timeout{}
	testserver.netCh <- timeoutEvent
	testserver.ProcessEvent()
	newTerm := <-testserver.updateCh
	errorCheck(t, strconv.Itoa(1+oldTerm), strconv.Itoa(newTerm.(SaveTerm).Term), "13")

	errorCheck(t, "candidate", testserver.State, "state didn't change")

	for i := 2; i <= 5; i++ {
		res := <-testserver.actionCh
		errorCheck(t, "Send", reflect.TypeOf(res).Name(), "12")
		errorCheck(t, strconv.Itoa(i), strconv.Itoa(res.(Send).peerID), "13")
		errorCheck(t, "VoteReq", reflect.TypeOf(res.(Send).event).Name(), "13b")
	}
	res := <-testserver.actionCh
	errorCheck(t, "Alarm", reflect.TypeOf(res).Name(), "14")
}

func leaderTimesout(t *testing.T) {
	var testserver *StateMachine
	testserver = NewStateMachine(1)
	initialize(testserver)
	testserver.State = "leader"
	timeoutEvent := Timeout{}
	testserver.netCh <- timeoutEvent
	testserver.ProcessEvent()
	errorCheck(t, "leader", testserver.State, "state didn't change")

	for i := 2; i <= 5; i++ {
		res := <-testserver.actionCh
		errorCheck(t, "Send", reflect.TypeOf(res).Name(), "12")
		errorCheck(t, strconv.Itoa(i), strconv.Itoa(res.(Send).peerID), "13")
		errorCheck(t, "AppendEntriesReq", reflect.TypeOf(res.(Send).event).Name(), "13b")
		if res.(Send).event.(AppendEntriesReq).Entries != nil {
			log.Println("heartbeat error")
		}
	}
	res := <-testserver.actionCh
	errorCheck(t, "Alarm", reflect.TypeOf(res).Name(), "14")
}

func candidateSteppingDownOnVoteResp(t *testing.T) {
	var testserver *StateMachine
	testserver = NewStateMachine(1)
	initialize(testserver)
	testserver.State = "candidate"
	testserver.CurrentTerm = 1
	testserver.netCh <- VoteResp{2, 5, false}
	testserver.ProcessEvent()
	errorCheck(t, "follower", testserver.State, "15")
	errorCheck(t, strconv.Itoa(testserver.CurrentTerm), strconv.Itoa(5), "16")
}

func leaderSteppingDownOnVoteResp(t *testing.T) {
	var testserver *StateMachine
	testserver = NewStateMachine(1)
	initialize(testserver)
	testserver.State = "candidate"
	testserver.CurrentTerm = 1
	testserver.netCh <- VoteResp{2, 5, false}
	testserver.ProcessEvent()
	errorCheck(t, "follower", testserver.State, "15")
	errorCheck(t, strconv.Itoa(testserver.CurrentTerm), strconv.Itoa(5), "16")
}

func leadershipElection(t *testing.T) {
	var testserver *StateMachine
	testserver = NewStateMachine(1)
	initialize(testserver)
	testserver.State = "candidate"
	testserver.CurrentTerm = 1
	testserver.VoteGranted[testserver.ServerID] = true

	vr := VoteResp{2, testserver.CurrentTerm, true}
	testserver.netCh <- vr
	testserver.ProcessEvent()
	errorCheck(t, "candidate", testserver.State, "17")

	vr = VoteResp{3, testserver.CurrentTerm, true}
	testserver.netCh <- vr
	testserver.ProcessEvent()
	errorCheck(t, "leader", testserver.State, "17")
	// won election as soon as recieved majority

	vr = VoteResp{4, testserver.CurrentTerm, true}
	testserver.netCh <- vr
	testserver.ProcessEvent()
	errorCheck(t, "leader", testserver.State, "17")

	vr = VoteResp{5, testserver.CurrentTerm, true}
	testserver.netCh <- vr
	testserver.ProcessEvent()
	errorCheck(t, "leader", testserver.State, "17")

	for i := 2; i <= 5; i++ {
		res := <-testserver.actionCh
		errorCheck(t, "Send", reflect.TypeOf(res).Name(), "17")
		errorCheck(t, strconv.Itoa(i), strconv.Itoa(res.(Send).peerID), "17")
		errorCheck(t, "AppendEntriesReq", reflect.TypeOf(res.(Send).event).Name(), "17")
		if res.(Send).event.(AppendEntriesReq).Entries != nil {
			log.Println("heartbeat error")
		}
	}
	res := <-testserver.actionCh
	errorCheck(t, "Alarm", reflect.TypeOf(res).Name(), "17")
}

func appendEntriesReqtoFollower(t *testing.T) {
	// leader has lower term
	var testserver *StateMachine
	testserver = NewStateMachine(1)
	initialize(testserver)
	testserver.State = "follower"
	testserver.CurrentTerm = 3
	logentries := make([]LogEntry, 1)
	logentry := LogEntry{[]byte{'s', 's', 'd', 'd', '#'}, 6}
	logentries[0] = logentry
	testserver.netCh <- AppendEntriesReq{1, 2, 1, 1, logentries, 1}
	testserver.ProcessEvent()
	res := <-testserver.actionCh
	errorCheck(t, "Send", reflect.TypeOf(res).Name(), "18")
	errorCheck(t, "AppendEntriesResp", reflect.TypeOf(res.(Send).event).Name(), "18")
	errorCheck(t, "3", strconv.Itoa(res.(Send).event.(AppendEntriesResp).Term), "18")
	errorCheck(t, "false", strconv.FormatBool(res.(Send).event.(AppendEntriesResp).Success), "18")

	// leader with higher term, logstore works
	testserver.State = "follower"
	testserver.CurrentTerm = 3
	logentries = make([]LogEntry, 1)
	logentry = LogEntry{[]byte{'a', 'b', 'c', 'd'}, 6}
	logentries[0] = logentry
	testserver.netCh <- AppendEntriesReq{6, 2, -1, -1, logentries, -1}
	testserver.ProcessEvent()
	res = <-testserver.actionCh
	errorCheck(t, "follower", testserver.State, "state didn't change")
	errorCheck(t, "Alarm", reflect.TypeOf(res).Name(), "17")
	res = <-testserver.actionCh
	errorCheck(t, "LogStore", reflect.TypeOf(res).Name(), "18")
	errorCheck(t, "0", strconv.Itoa(res.(LogStore).index), "18")
	errorCheck(t, "abcd", string(res.(LogStore).data[:len(res.(LogStore).data)]), "18")
	datum := res.(LogStore).data
	logentry = LogEntry{datum, testserver.CurrentTerm}
	testserver.LogStore(res.(LogStore).index, logentry)
	errorCheck(t, "1", strconv.Itoa(len(testserver.Log)), "17")
	res = <-testserver.actionCh
	errorCheck(t, "Send", reflect.TypeOf(res).Name(), "17")
	errorCheck(t, "AppendEntriesResp", reflect.TypeOf(res.(Send).event).Name(), "18")
	errorCheck(t, "6", strconv.Itoa(res.(Send).event.(AppendEntriesResp).Term), "18")
	errorCheck(t, "true", strconv.FormatBool(res.(Send).event.(AppendEntriesResp).Success), "18")
	errorCheck(t, "1", strconv.Itoa(res.(Send).event.(AppendEntriesResp).From), "18")
	errorCheck(t, "0", strconv.Itoa(res.(Send).event.(AppendEntriesResp).MatchIndex), "18")
	res = <-testserver.actionCh
	errorCheck(t, "Alarm", reflect.TypeOf(res).Name(), "17")

	// leader with equal term
	testserver.State = "follower"
	testserver.CurrentTerm = 8
	logentries = make([]LogEntry, 1)
	logentry = LogEntry{[]byte{'a', 'b', 'c', 'd'}, 6}
	logentries[0] = logentry
	testserver.netCh <- AppendEntriesReq{8, 2, 3, -1, logentries, -1}
	testserver.ProcessEvent()
	errorCheck(t, "follower", testserver.State, "state didn't change")
	res = <-testserver.actionCh
	errorCheck(t, "Send", reflect.TypeOf(res).Name(), "17")
	errorCheck(t, "AppendEntriesResp", reflect.TypeOf(res.(Send).event).Name(), "18")
	errorCheck(t, "8", strconv.Itoa(res.(Send).event.(AppendEntriesResp).Term), "18")
	errorCheck(t, "false", strconv.FormatBool(res.(Send).event.(AppendEntriesResp).Success), "18")
	errorCheck(t, "1", strconv.Itoa(res.(Send).event.(AppendEntriesResp).From), "18")
	errorCheck(t, "-1", strconv.Itoa(res.(Send).event.(AppendEntriesResp).MatchIndex), "18")
	res = <-testserver.actionCh
	errorCheck(t, "Alarm", reflect.TypeOf(res).Name(), "17")

}

func appendEntriesReqtoCandidate(t *testing.T) {
	// leader has lower term
	var testserver *StateMachine
	testserver = NewStateMachine(1)
	initialize(testserver)
	testserver.State = "candidate"
	testserver.CurrentTerm = 3
	logentries := make([]LogEntry, 1)
	logentry := LogEntry{[]byte{'s', 's', 'd', 'd', '#'}, 6}
	logentries[0] = logentry
	testserver.netCh <- AppendEntriesReq{1, 2, 1, 1, logentries, 1}
	testserver.ProcessEvent()
	res := <-testserver.actionCh
	errorCheck(t, "Send", reflect.TypeOf(res).Name(), "18")
	errorCheck(t, "AppendEntriesResp", reflect.TypeOf(res.(Send).event).Name(), "18")
	errorCheck(t, "3", strconv.Itoa(res.(Send).event.(AppendEntriesResp).Term), "18")
	errorCheck(t, "false", strconv.FormatBool(res.(Send).event.(AppendEntriesResp).Success), "18")

	// leader with higher term
	testserver.State = "candidate"
	testserver.CurrentTerm = 3
	logentries = make([]LogEntry, 1)
	logentry = LogEntry{[]byte{'a', 'b', 'c', 'd'}, 6}
	logentries[0] = logentry
	testserver.netCh <- AppendEntriesReq{6, 2, -1, -1, logentries, -1}
	testserver.ProcessEvent()
	res = <-testserver.actionCh
	errorCheck(t, "follower", testserver.State, "state didn't change")
	errorCheck(t, "Alarm", reflect.TypeOf(res).Name(), "18")
	res = <-testserver.actionCh
	errorCheck(t, "LogStore", reflect.TypeOf(res).Name(), "18")
	errorCheck(t, "0", strconv.Itoa(res.(LogStore).index), "18")
	errorCheck(t, "abcd", string(res.(LogStore).data[:len(res.(LogStore).data)]), "18")
	res = <-testserver.actionCh
	errorCheck(t, "Send", reflect.TypeOf(res).Name(), "17")
	errorCheck(t, "AppendEntriesResp", reflect.TypeOf(res.(Send).event).Name(), "18")
	errorCheck(t, "6", strconv.Itoa(res.(Send).event.(AppendEntriesResp).Term), "18")
	errorCheck(t, "true", strconv.FormatBool(res.(Send).event.(AppendEntriesResp).Success), "18")
	errorCheck(t, "1", strconv.Itoa(res.(Send).event.(AppendEntriesResp).From), "18")
	errorCheck(t, "0", strconv.Itoa(res.(Send).event.(AppendEntriesResp).MatchIndex), "18")
	res = <-testserver.actionCh
	errorCheck(t, "Alarm", reflect.TypeOf(res).Name(), "17")

}

func voteReq(t *testing.T) {
	// candidate 1 converts to follower because of higher term of approaching candidate 2
	// must grant the vote
	var testserver *StateMachine
	testserver = NewStateMachine(1)
	initialize(testserver)
	testserver.State = "candidate"
	testserver.CurrentTerm = 2
	testserver.VotedFor = 1
	testserver.netCh <- VoteReq{4, 2, 5, 3}
	testserver.ProcessEvent()
	res := <-testserver.actionCh
	errorCheck(t, "follower", testserver.State, "state didn't change")
	errorCheck(t, "2", strconv.Itoa(testserver.VotedFor), "state didn't change")
	errorCheck(t, "Alarm", reflect.TypeOf(res).Name(), "19")
	res = <-testserver.actionCh
	errorCheck(t, "Send", reflect.TypeOf(res).Name(), "19")
	errorCheck(t, "VoteResp", reflect.TypeOf(res.(Send).event).Name(), "19")
	errorCheck(t, "4", strconv.Itoa(res.(Send).event.(VoteResp).Term), "19")
	errorCheck(t, "true", strconv.FormatBool(res.(Send).event.(VoteResp).VoteGranted), "19")
	errorCheck(t, "1", strconv.Itoa(res.(Send).event.(VoteResp).ServerID), "19")
	res = <-testserver.actionCh
	errorCheck(t, "Alarm", reflect.TypeOf(res).Name(), "19")

	// must not grant vote to another candidate 3, since he has already granted earlier to 2
	testserver.netCh <- VoteReq{4, 3, 5, 3}
	testserver.ProcessEvent()
	res = <-testserver.actionCh
	errorCheck(t, "follower", testserver.State, "state didn't change")
	errorCheck(t, "Send", reflect.TypeOf(res).Name(), "19")
	errorCheck(t, "VoteResp", reflect.TypeOf(res.(Send).event).Name(), "19")
	errorCheck(t, "4", strconv.Itoa(res.(Send).event.(VoteResp).Term), "19")
	errorCheck(t, "false", strconv.FormatBool(res.(Send).event.(VoteResp).VoteGranted), "19")
	errorCheck(t, "1", strconv.Itoa(res.(Send).event.(VoteResp).ServerID), "19")

	// if 2 didn't receive reply or contacting again, 1 must again grant him vote as he remebers
	testserver.netCh <- VoteReq{4, 2, 5, 3}
	testserver.ProcessEvent()
	errorCheck(t, "follower", testserver.State, "state didn't change")
	res = <-testserver.actionCh
	errorCheck(t, "Send", reflect.TypeOf(res).Name(), "19")
	errorCheck(t, "VoteResp", reflect.TypeOf(res.(Send).event).Name(), "19")
	errorCheck(t, "4", strconv.Itoa(res.(Send).event.(VoteResp).Term), "19")
	errorCheck(t, "true", strconv.FormatBool(res.(Send).event.(VoteResp).VoteGranted), "19")
	errorCheck(t, "1", strconv.Itoa(res.(Send).event.(VoteResp).ServerID), "19")
	res = <-testserver.actionCh
	errorCheck(t, "Alarm", reflect.TypeOf(res).Name(), "19")

	// wont grant if he has higher term
	testserver.netCh <- VoteReq{1, 3, 5, 3}
	testserver.ProcessEvent()
	res = <-testserver.actionCh
	errorCheck(t, "follower", testserver.State, "state didn't change")
	errorCheck(t, "Send", reflect.TypeOf(res).Name(), "19")
	errorCheck(t, "VoteResp", reflect.TypeOf(res.(Send).event).Name(), "19")
	errorCheck(t, "4", strconv.Itoa(res.(Send).event.(VoteResp).Term), "19")
	errorCheck(t, "false", strconv.FormatBool(res.(Send).event.(VoteResp).VoteGranted), "19")
	errorCheck(t, "1", strconv.Itoa(res.(Send).event.(VoteResp).ServerID), "19")

	// wont grant and no state change even if he himself was a candidate(cos he had higher term)
	testserver.State = "candidate"
	testserver.VotedFor = 1
	testserver.netCh <- VoteReq{1, 3, 5, 3}
	testserver.ProcessEvent()
	res = <-testserver.actionCh
	errorCheck(t, "candidate", testserver.State, " why state changed")
	errorCheck(t, "Send", reflect.TypeOf(res).Name(), "19")
	errorCheck(t, "VoteResp", reflect.TypeOf(res.(Send).event).Name(), "19")
	errorCheck(t, "4", strconv.Itoa(res.(Send).event.(VoteResp).Term), "19")
	errorCheck(t, "false", strconv.FormatBool(res.(Send).event.(VoteResp).VoteGranted), "19")
	errorCheck(t, "1", strconv.Itoa(res.(Send).event.(VoteResp).ServerID), "19")

	// grant vote since terms are same but last message term is greater for candidate
	testserver = NewStateMachine(1)
	initialize(testserver)
	testserver.State = "follower"
	testserver.CurrentTerm = 5
	datum := []byte{'b', 'b', 'c', 's'}
	var logentries []LogEntry
	logentry := LogEntry{datum, 3}
	for i := 0; i < 5; i++ {
		logentries = append(logentries, logentry)
	}
	testserver.Log = logentries
	testserver.netCh <- VoteReq{5, 2, 5, 4}
	testserver.ProcessEvent()

	res = <-testserver.actionCh
	errorCheck(t, "true", strconv.FormatBool(res.(Send).event.(VoteResp).VoteGranted), "19")
	res = <-testserver.actionCh
	errorCheck(t, "Alarm", reflect.TypeOf(res).Name(), "19")

	// deny vote since terms are same but last message term is smaller for candidate
	testserver = NewStateMachine(1)
	initialize(testserver)
	testserver.State = "follower"
	testserver.CurrentTerm = 5
	datum = []byte{'b', 'b', 'c', 's'}
	var logentries2 []LogEntry
	logentry = LogEntry{datum, 3}
	for i := 0; i < 5; i++ {
		logentries2 = append(logentries, logentry)
	}
	testserver.Log = logentries2
	testserver.netCh <- VoteReq{5, 2, 5, 2}
	testserver.ProcessEvent()

	res = <-testserver.actionCh
	errorCheck(t, "false", strconv.FormatBool(res.(Send).event.(VoteResp).VoteGranted), "19")

	// grant vote since terms are same but last log index is greater for candidate
	testserver = NewStateMachine(1)
	initialize(testserver)
	testserver.State = "follower"
	testserver.CurrentTerm = 5
	datum = []byte{'b', 'b', 'c', 's'}
	var logentries3 []LogEntry
	logentry = LogEntry{datum, 3}
	for i := 0; i < 5; i++ {
		logentries3 = append(logentries, logentry)
	}
	testserver.Log = logentries3
	testserver.netCh <- VoteReq{5, 2, 6, 3}
	testserver.ProcessEvent()

	res = <-testserver.actionCh
	errorCheck(t, "true", strconv.FormatBool(res.(Send).event.(VoteResp).VoteGranted), "19")
	res = <-testserver.actionCh
	errorCheck(t, "Alarm", reflect.TypeOf(res).Name(), "19")

	// deny vote since terms are same but last log index is smaller for candidate
	testserver = NewStateMachine(1)
	initialize(testserver)
	testserver.State = "follower"
	testserver.CurrentTerm = 5
	datum = []byte{'b', 'b', 'c', 's'}
	var logentries4 []LogEntry
	logentry = LogEntry{datum, 3}
	for i := 0; i < 5; i++ {
		logentries4 = append(logentries, logentry)
	}
	testserver.Log = logentries4
	testserver.netCh <- VoteReq{5, 2, 3, 3}
	testserver.ProcessEvent()

	res = <-testserver.actionCh
	errorCheck(t, "false", strconv.FormatBool(res.(Send).event.(VoteResp).VoteGranted), "19")

}

//
// Type AppendEntriesResp struct {
// 	From       int
// 		Term       int
// 			MatchIndex int
// 				Success    bool
// 			}

// type AppendEntriesReq struct {
// 	Term         int
// 	LeaderId     int
// 	PrevLogIndex int
// 	PrevLogTerm  int
// 	Entries      []LogEntry
// 	CommitIndex  int
// }

func appendEntriesResponse(t *testing.T) {
	// candidate 1 converts to follower because of higher term of approaching candidate 2
	// must grant the vote
	var testserver *StateMachine
	testserver = NewStateMachine(1)
	initialize(testserver)
	testserver.State = "leader"
	testserver.CurrentTerm = 2
	testserver.VotedFor = 1
	testserver.netCh <- AppendEntriesResp{2, 4, 15, false}
	testserver.ProcessEvent()
	res := <-testserver.actionCh
	errorCheck(t, "Alarm", reflect.TypeOf(res).Name(), "20")
	errorCheck(t, "follower", testserver.State, "20")

	testserver = NewStateMachine(1)
	initialize(testserver)
	testserver.State = "leader"
	testserver.CurrentTerm = 4
	testserver.VotedFor = 1
	testserver.NextIndex[2] = 1
	datum := []byte{'b', 'b', 'c', 's'}
	var logentries []LogEntry
	logentry := LogEntry{datum, 3}
	for i := 0; i < 5; i++ {
		logentries = append(logentries, logentry)
	}
	testserver.Log = logentries

	testserver.netCh <- AppendEntriesResp{2, 4, 15, false}
	testserver.ProcessEvent()

	res = <-testserver.actionCh
	errorCheck(t, "Send", reflect.TypeOf(res).Name(), "20")
	errorCheck(t, "-1", strconv.Itoa(res.(Send).event.(AppendEntriesReq).PrevLogIndex), "20")

	testserver = NewStateMachine(1)
	initialize(testserver)
	testserver.State = "leader"
	testserver.CurrentTerm = 4
	testserver.VotedFor = 1
	testserver.NextIndex[2] = 1
	testserver.MatchIndex[2] = 1
	testserver.MatchIndex[3] = 1
	testserver.MatchIndex[4] = 1
	testserver.MatchIndex[5] = 1
	datum = []byte{'b', 'b', 'c', 's'}
	logentry = LogEntry{datum, 4}
	var logentries2 []LogEntry

	for i := 0; i < 5; i++ {
		logentries2 = append(logentries2, logentry)
	}
	testserver.Log = logentries2

	testserver.netCh <- AppendEntriesResp{2, 4, 2, true}
	testserver.ProcessEvent()

	res = <-testserver.actionCh
	errorCheck(t, "Send", reflect.TypeOf(res).Name(), "20")
	errorCheck(t, "2", strconv.Itoa(res.(Send).event.(AppendEntriesReq).PrevLogIndex), "20")
	errorCheck(t, "1", strconv.Itoa(testserver.CommitIndex), "20")

}
