package main

import "fmt"
import "os"
import "net"
import "encoding/gob"

func serverMain(rafts []RaftNode) {

	gob.Register(VoteReq{})
	gob.Register(VoteResp{})
	gob.Register(AppendEntriesReq{})
	gob.Register(AppendEntriesResp{})
	gob.Register(AppendEntry{})
	gob.Register(Timeout{})

	fservers := make([]*FileService, len(configs.Peers))

	for i := 0; i < len(configs.Peers); i++ {
		defer rafts[i].lg.Close()
		go rafts[i].startProcessing()
	}

	var max_clients int = 1000

	var cArray []*ClientHandler
	var rArray []chan Response
	rArray = make([]chan Response, max_clients)

	for i := 0; i < max_clients; i++ {
		rArray[i] = make(chan Response)
	}

	for i := 0; i < len(configs.Peers); i++ {
		fservers[i] = NewFS(i+1, rArray, rafts[i].CommitChannel())
		go fservers[i].Listen()
	}

	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}

	for i := int64(0); ; i++ {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		c := &ClientHandler{i, rArray[int(i)]}
		cArray = append(cArray, c)

		// Handle new connection in a new thread
		go cArray[i].handleConnection(conn, rafts)
	}

}

type Msg struct {
	Id       int64
	Kind     int
	Filename string
	Contents []byte
	Exptime  string
	Version  string
	Exp      string
	NumBytes string
}

type Response struct {
	Output string
	Err    bool
	File   []byte
}
