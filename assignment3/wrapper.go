package main

import "github.com/syndtr/goleveldb/leveldb"
import "github.com/cs733-iitb/log"
import "github.com/cs733-iitb/cluster"
import "github.com/cs733-iitb/cluster/mock"
import "fmt"
import "math/rand"
import "time"

import "strconv"
import "encoding/json"
import "sync"

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
	Data  []byte
	Index int64
	Err   error // Err can be errred
}

// This is an example structure for Config
type Config struct {
	cluster          []NetConfig // Information about all servers, including this.
	Id               int         // this node's id. One of the cluster's entries should match.
	LogDir           string      // Log file directory for this node
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
		peerList = append(peerList, peerId)
	}
	return peerList
}

type RaftNode struct {
	sm          *StateMachine
	timer       *time.Timer
	clientCh    chan AppendEntry
	actionCh    chan events
	commitCh    chan CommitInfo
	quitCh      chan bool
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
		actionCh: make(chan events, 100),
		quitCh:   make(chan bool),
		commitCh: make(chan CommitInfo, 100),
		sm:       NewStateMachine(int64(config.Id), GetPeerList(rn.cluster.Peers), rn.actionCh, config.ElectionTimeout, config.HeartbeatTimeout, rn.lg),
		mutex:    &sync.RWMutex{},
		logMutex: &sync.RWMutex{},
		server:   serverArg,
	}
	rn.lg, _ = log.Open(rn.LogDir + "/Log" + strconv.Itoa(config.Id))
	return rn
}

func (rn *RaftNode) Id() int {
	return int(rn.sm.ServerID)
}

func (rn *RaftNode) Append(data []byte) {
	rn.clientCh <- AppendEntry{data}
}

func (rn *RaftNode) CommitChannel() <-chan CommitInfo {
	return rn.commitCh
}

func (rn *RaftNode) CommittedIndex() int64 {
	rn.mutex.RLock()
	c := rn.sm.CommitIndex
	rn.mutex.RUnlock()
	return c
}

func (rn *RaftNode) Get(index int64) ([]byte, error) {
	rn.logMutex.RLock()
	c, err := rn.lg.Get(index)
	var entry LEntry
	json.Unmarshal(c, &entry)
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
		if rn.sm.LeaderID == rn.sm.LeaderID {
			return &node
		}
	}
	return nil
}

func (rn *RaftNode) ShutDown() {
	rn.quitCh <- true
	rn.sm.status = "shutdown"

	currentTermDB, _ := leveldb.OpenFile("/Users/Siddhartha/Documents/Academics/8thSem/cs733/assignment3/currentTerm", nil)
	defer currentTermDB.Close()
	currentTermDB.Put([]byte(strconv.FormatInt(rn.sm.ServerID, 10)), []byte(strconv.FormatInt(rn.sm.CurrentTerm, 10)), nil)

	votedForDB, _ := leveldb.OpenFile("/Users/Siddhartha/Documents/Academics/8thSem/cs733/assignment3/votedFor", nil)
	defer votedForDB.Close()
	votedForDB.Put([]byte(strconv.FormatInt(rn.sm.ServerID, 10)), []byte(strconv.FormatInt(rn.sm.VotedFor, 10)), nil)
	close(rn.actionCh)
	close(rn.clientCh)
	close(rn.quitCh)
	close(rn.commitCh)

	rn.server.Close()
	rn.timer.Stop()
	rn.lg.Close()
}

func DBReset() {

	currentTermDB, _ := leveldb.OpenFile("/Users/Siddhartha/Documents/Academics/8thSem/cs733/assignment3/currentTerm", nil)
	defer currentTermDB.Close()
	for i := 0; i < len(configs.Peers); i++ {
		currentTermDB.Put([]byte(strconv.FormatInt(int64(i), 10)), []byte(strconv.FormatInt(int64(0), 10)), nil)
	}

	votedForDB, _ := leveldb.OpenFile("/Users/Siddhartha/Documents/Academics/8thSem/cs733/assignment3/votedFor", nil)
	defer votedForDB.Close()

	for i := 0; i < len(configs.Peers); i++ {
		votedForDB.Put([]byte(strconv.FormatInt(int64(configs.Peers[i].Id), 10)), []byte(strconv.FormatInt(int64(-1), 10)), nil)

		lg, _ := log.Open("/Users/Siddhartha/Documents/Academics/8thSem/cs733/assignment3/Log" + strconv.Itoa(i))
		lg.TruncateToEnd(0)
		lg.Close()
	}
}

func makeRafts() []RaftNode {
	var nodes []RaftNode
	for i := 0; i < len(configs.Peers); i++ {
		config := Config{configs, configs.Peers[i].Id, "/Users/Siddhartha/Documents/Academics/8thSem/cs733/assignment3", 150, 50}
		server, _ = cluster.New(configs.Peers[i].Id, configs)
		nodes = append(nodes, New(config, server))
	}
	return nodes
}

func (rn *RaftNode) startProcessing() {
	rn.timer = time.NewTimer(time.Duration(rand.Intn(rn.sm.ELECTION_TIMEOUT)))
	for {
		select {
		case <-rn.timer.C:
			rn.sm.netCh <- Timeout{}
			rn.sm.ProcessEvent()
		case appendMsg := <-rn.clientCh:
			rn.sm.netCh <- appendMsg
			rn.sm.ProcessEvent()
		case envelop := <-rn.server.Inbox():
			rn.sm.netCh <- envelop.Msg
			rn.sm.ProcessEvent()
		case ev := <-rn.actionCh:
			switch ev.(type) {
			case Alarm:
				rn.timer.Reset(ev.(Alarm).delay * Millisecond)
			case Send:
				ev, _ := ev.(Send)
				arg, _ := json.Marshal(ev.Event)
				rn.server.Outbox() <- &cluster.Envelope{Pid: ev.peerID, Msg: arg}
			case Commit:
				ev, _ := ev.(Commit)
				out := CommitInfo{ev.Data, ev.Index, ev.Err}
				rn.commitCh <- out
			case LogTransfer:
				ev, _ := ev.(LogTransfer)
				rn.lg.TruncateToEnd(ev.Index)
				arg, _ := json.Marshal(ev.LEntry)
				rn.lg.Append(arg)
			}
		case <-rn.quitCh:
			return
		}

	}
	main_wait <- True
}

var main_wait chan bool

func begin() {
	gob.Register(VoteReq{})
	gob.Register(VoteResp{})
	gob.Register(AppendEntriesReq{})
	gob.Register(AppendEntriesResp{})
	gob.Register(AppendEntry{})
	gob.Register(Timeout{})

	main_wait = make(chan bool)
	nodes := makeRafts()
	for i := 0; i < len(configs.Peers); i++ {
		//defer nodes[i].lg.Close()
		go nodes[i].startProcessing()
	}
	<-main_wait
}

func main() {
	begin()
}
