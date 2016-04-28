package main

import (
	"github.com/cs733-iitb/cluster"
	"github.com/cs733-iitb/log"
	"github.com/syndtr/goleveldb/leveldb"
	// "reflect"

	// "encoding/gob"
	"encoding/json"
	// "fmt"
	"errors"
	"github.com/cs733-iitb/cluster/mock"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

type Node interface {
	// Client's message to Raft node
	Append([]byte)
	// A channel for client to listen on. What goes into Append must come out of here at some point.
	CommitChannel() <-chan CommitInfo
	// Last known committed index in the log. This could be -1 until the system stabilizes.
	CommittedIndex() int64
	// Returns the data at a log index, or an error.
	Get(index int64) (error, []byte)
	// Node's id
	Id() int
	// Id of leader. -1 if unknown
	LeaderId() int
	// Signal to shut down all goroutines, stop sockets, flush log and close it, cancel timers.
	Shutdown()
}

// data goes in via Append, comes out as CommitInfo from the node's CommitChannel
// Index is valid only if err == nil
type CommitInfo struct {
	Data   []byte
	Index  int64
	Err    error // Err can be errred
	Leader bool
}

// This is an example structure for Config
type Config struct {
	cluster          cluster.Config // Information about all servers, including this.
	Id               int            // this node's id. One of the cluster's entries should match.
	LogDir           string         // Log file directory for this node
	ElectionTimeout  int
	HeartbeatTimeout int
}

type NetConfig struct {
	Id   int
	Host string
	Port int
}

var configs cluster.Config = cluster.Config{
	Peers: []cluster.PeerConfig{
		{Id: 1, Address: "localhost:8010"},
		{Id: 2, Address: "localhost:8020"},
		{Id: 3, Address: "localhost:8030"},
		{Id: 4, Address: "localhost:8040"},
		{Id: 5, Address: "localhost:8050"}}}

func GetPeerList(peers []cluster.PeerConfig) []int {
	var peerList []int
	for _, peerId := range peers {
		peerList = append(peerList, peerId.Id)
	}
	return peerList
}

type RaftNode struct {
	sm          *StateMachine
	timer       *time.Timer
	clientCh    chan AppendEntry
	actionCh    chan interface{}
	commitCh    chan CommitInfo
	byeBye      chan bool
	cluster     cluster.Config
	server      cluster.Server
	LogDir      string
	lg          *log.Log
	mutex       *sync.RWMutex
	logMutex    *sync.RWMutex
	currentTerm *leveldb.DB
	votedFor    *leveldb.DB
}

func New(config Config, serverArg cluster.Server) RaftNode {
	rn := RaftNode{
		cluster:  config.cluster,
		LogDir:   config.LogDir,
		clientCh: make(chan AppendEntry),
		//actionCh: make(chan interface{}, 100),
		byeBye:   make(chan bool),
		commitCh: make(chan CommitInfo, 250),

		mutex:    &sync.RWMutex{},
		logMutex: &sync.RWMutex{},
		server:   serverArg,
	}
	rn.lg, _ = log.Open(rn.LogDir + "/Log" + strconv.Itoa(int(config.Id)))
	rn.sm = NewStateMachine(int(config.Id), GetPeerList(rn.cluster.Peers), float64(config.ElectionTimeout), float64(config.HeartbeatTimeout), rn.lg)
	return rn
}

func (rn *RaftNode) Id() int {
	return int(rn.sm.ServerID)
}

func (rn *RaftNode) Append(data []byte) {
	var n Msg
	json.Unmarshal(data, &n)
	if n.Kind == 0 { // if read then do not replicate
		out := CommitInfo{data, -1, nil, true}
		rn.commitCh <- out
	} else {
		rn.clientCh <- AppendEntry{data}
	}
}

func (rn *RaftNode) CommitChannel() <-chan CommitInfo {
	return rn.commitCh
}

func (rn *RaftNode) CommittedIndex() int64 {
	rn.mutex.RLock()
	c := rn.sm.CommitIndex
	rn.mutex.RUnlock()
	return int64(c)
}

func (rn *RaftNode) Get(index int64) ([]byte, error) {
	rn.logMutex.RLock()
	c, err := rn.lg.Get(index)
	// entry := c.(LogEntry)
	var entry LogEntry
	json.Unmarshal(c.([]byte), &entry)
	// var entry LogEntry
	// 	json.Unmarshal(c.Msg.([]byte), &entry)
	rn.logMutex.RUnlock()
	return entry.Data, err
}

func (rn *RaftNode) LeaderId() int {
	rn.mutex.RLock()
	id := rn.sm.LeaderID
	rn.mutex.RUnlock()
	return id
}

func getLeader(r []RaftNode) *RaftNode {
	for _, node := range r {
		node.mutex.RLock()
		if node.sm.State != "shutdown" && node.sm.LeaderID == node.sm.ServerID {
			node.mutex.RUnlock()
			return &node
		}
		node.mutex.RUnlock()
	}
	return nil
}

func (rn *RaftNode) ShutDown() {
	rn.byeBye <- true
	rn.sm.State = "shutdown"

	currentTermDB, _ := leveldb.OpenFile(PATH+"/currentTerm", nil)
	defer currentTermDB.Close()
	currentTermDB.Put([]byte(strconv.Itoa(rn.sm.ServerID)), []byte(strconv.Itoa(rn.sm.CurrentTerm)), nil)

	votedForDB, _ := leveldb.OpenFile(PATH+"/votedFor", nil)
	defer votedForDB.Close()
	votedForDB.Put([]byte(strconv.Itoa(rn.sm.ServerID)), []byte(strconv.Itoa(rn.sm.VotedFor)), nil)

	out := CommitInfo{nil, -1, errors.New("Stop"), false}
	rn.commitCh <- out

	close(rn.sm.actionCh)
	close(rn.clientCh)
	close(rn.byeBye)
	close(rn.commitCh)

	rn.server.Close()
	rn.timer.Stop()
	rn.lg.Close()
}

// var configs cluster.Config = cluster.Config{
// 	Peers: []cluster.PeerConfig{
// 		{Id: 1, Address: "localhost:8010"},
// 		{Id: 2, Address: "localhost:8020"},
// 		{Id: 3, Address: "localhost:8030"},
// 		{Id: 4, Address: "localhost:8040"},
// 		{Id: 5, Address: "localhost:8050"}}}

func makeRafts() []RaftNode {
	// votedForDB, _ = leveldb.OpenFile(PATH+"/votedFor", nil)
	var nodes []RaftNode
	for i := 0; i < len(configs.Peers); i++ {
		config := Config{configs, configs.Peers[i].Id, PATH + "", 550, 50}
		server, _ := cluster.New(configs.Peers[i].Id, configs)
		nodes = append(nodes, New(config, server))
	}
	return nodes
}

func makeMockRafts() ([]RaftNode, *mock.MockCluster) {
	// votedForDB, _ = leveldb.OpenFile(PATH+"/votedFor", nil)
	clusterConfig := cluster.Config{Peers: []cluster.PeerConfig{
		{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4}, {Id: 5}}}
	clstr, _ := mock.NewCluster(clusterConfig)

	var nodes []RaftNode

	for i := 0; i < len(clusterConfig.Peers); i++ {

		config := Config{clusterConfig, clusterConfig.Peers[i].Id, PATH + "", 550, 50}
		nodes = append(nodes, New(config, clstr.Servers[i+1]))

	}
	// //fmt.Println("nodes:", nodes, "cls:", clstr)
	return nodes, clstr
}

func (rn *RaftNode) fSaveTerm(i int) {
	var currentTermDB *leveldb.DB
	var err error
	currentTermDB, err = leveldb.OpenFile(PATH+"/currentTerm", nil)
	for err != nil {
		currentTermDB, err = leveldb.OpenFile(PATH+"/currentTerm", nil)
	}
	// fmt.Println("sad", currentTermDB, reflect.TypeOf(currentTermDB), err, reflect.TypeOf(err))
	defer currentTermDB.Close()
	currentTermDB.Put([]byte(strconv.Itoa(rn.sm.ServerID)), []byte(strconv.Itoa(i)), nil)

}

func (rn *RaftNode) fSaveVotedFor(i int) {

	var votedForDB *leveldb.DB
	var err error
	votedForDB, err = leveldb.OpenFile(PATH+"/currentTerm", nil)
	for err != nil {
		votedForDB, err = leveldb.OpenFile(PATH+"/currentTerm", nil)
	}

	defer votedForDB.Close()
	votedForDB.Put([]byte(strconv.Itoa(rn.sm.ServerID)), []byte(strconv.Itoa(i)), nil)
}

func (rn *RaftNode) startProcessing() {
	rand.Seed(int64(rn.sm.ServerID))
	rn.timer = time.NewTimer(time.Duration(rand.Intn(int(rn.sm.ELECTION_TIMEOUT))))
	for {
		rn.mutex.Lock()
		select {
		case <-rn.timer.C:
			rn.sm.netCh <- Timeout{}
			rn.sm.ProcessEvent()
			//fmt.Println(rn.sm.ServerID, "time out")
		case appendMsg := <-rn.clientCh:
			rn.sm.netCh <- appendMsg
			rn.sm.ProcessEvent()
		case envelop := <-rn.server.Inbox():
			// //fmt.Println(rn.sm.ServerID, "recieved", (envelop.Msg.(interface{})))
			rn.sm.netCh <- envelop.Msg
			rn.sm.ProcessEvent()
		case ev := <-rn.sm.actionCh:
			switch ev.(type) {
			case Alarm:
				rn.timer.Reset(time.Duration(ev.(Alarm).Delay) * time.Millisecond)
			case Send:
				ev, _ := ev.(Send)
				rn.server.Outbox() <- &cluster.Envelope{Pid: ev.PeerID, Msg: ev.Event}
				// //fmt.Println(rn.sm.ServerID, "sent", ev.PeerID, string(arg))
			case Commit:
				ev, _ := ev.(Commit)
				// fmt.Println("info recd", int64(ev.Index), rn.sm.ServerID, rn.sm.LeaderID, ev.Err)
				out := CommitInfo{ev.Data, int64(ev.Index), ev.Err, ev.Leader}
				rn.commitCh <- out
			case LogTransfer:
				ev, _ := ev.(LogTransfer)
				//fmt.Println("log transfer called")
				rn.logMutex.Lock()
				//fmt.Println("log transfer lock got")
				rn.lg.TruncateToEnd(int64(ev.Index))
				arg, _ := json.Marshal(ev.LEntry)
				rn.lg.Append(arg)
				// rn.lg.Append(ev.LEntry)
				rn.logMutex.Unlock()
			}
		case ev := <-rn.sm.updateCh:
			switch ev.(type) {
			case SaveVotedFor:
				rn.fSaveVotedFor(ev.(SaveVotedFor).VotedFor)
			case SaveTerm:
				rn.fSaveTerm(ev.(SaveTerm).Term)
			}
		case <-rn.byeBye:
			rn.mutex.Unlock()
			return
		}
		rn.mutex.Unlock()
	}

	main_wait <- true
}

var main_wait chan bool

func setup() {

	// DBReset()

	// gob.Register(VoteReq{})
	// gob.Register(VoteResp{})
	// gob.Register(AppendEntriesReq{})
	// gob.Register(AppendEntriesResp{})
	// gob.Register(AppendEntry{})
	// gob.Register(Timeout{})

	// main_wait = make(chan bool)
	// 	nodes := makeRafts()
	// 	for i := 0; i < len(configs.Peers); i++ {
	// 		defer nodes[i].lg.Close()
	// 		go nodes[i].startProcessing()
	// 	}
	// 	time.Sleep(10 * time.Second)
	//
	// 	ldr := getLeader(nodes)
	// 	//fmt.Println("ldr is ", ldr.sm.ServerID)
	// 	ldr.Append([]byte("foo"))
	// 	time.Sleep(1 * time.Second)
	// 	for _, node := range nodes {
	// 		//fmt.Println("waiting")
	// 		select {
	//
	// 		case ci := <-node.CommitChannel():
	// 			//fmt.Println("over")
	// 			if ci.Err != nil {
	// 				//fmt.Println(ci.Err)
	// 			}
	// 			if string(ci.Data) != "foo" {
	// 				//fmt.Println("Got different data", string(ci.Data))
	// 			} else {
	// 				//fmt.Println("Proper Commit")
	// 			}
	// 			//default: //fmt.Println("Expected message on all nodes")
	// 		}
	// 	}
	//
	// 	<-main_wait
}
