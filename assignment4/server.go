package main

import (
	"bufio"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
)

func check(obj interface{}) {
	if obj != nil {
		fmt.Println(obj)
		os.Exit(1)
	}
}

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

	var clienthandler []*ClientHandler
	var responses []chan Response
	responses = make([]chan Response, max_clients)

	for i := 0; i < max_clients; i++ {
		responses[i] = make(chan Response)
	}

	for i := 0; i < len(configs.Peers); i++ {
		fservers[i] = NewFS(i+1, responses, rafts[i].CommitChannel())
		go fservers[i].Listen()
	}

	tcpaddr, err := net.ResolveTCPAddr("tcp", "localhost:8080")
	check(err)
	tcp_acceptor, err := net.ListenTCP("tcp", tcpaddr)
	check(err)

	for i := int64(0); ; i++ {
		tcp_conn, err := tcp_acceptor.AcceptTCP()
		check(err)
		// fmt.Println("called from  3 " + strconv.FormatInt(i, 10))
		c := &ClientHandler{i, responses[int(i)]}
		clienthandler = append(clienthandler, c)
		// fmt.Println("called from 1 " + strconv.FormatInt(i, 10))
		// Handle new connection in a new thread
		go clienthandler[i].handleConnection(tcp_conn, rafts)
		// fmt.Println("called from " + strconv.FormatInt(i, 10))
	}

}

var crlf = []byte{'\r', '\n'}

type ClientHandler struct {
	Id    int64
	route chan Response
}

func Replicate(msg []byte, rafts []RaftNode) {
	var ldr *RaftNode
	for {
		ldr = getLeader(rafts)
		if ldr != nil {
			break
		}
	}
	//fmt.Println("Leader chosen to Replicate ", ldr.Id())
	ldr.Append(msg)
}

func reply(conn *net.TCPConn, msg *Msg) bool {
	var err error
	write := func(data []byte) {
		if err != nil {
			return
		}
		_, err = conn.Write(data)
	}
	var resp string
	switch msg.Kind2 {
	case 'C': // read response
		resp = fmt.Sprintf("CONTENTS %d %d %d", msg.Version, msg.NumBytes, msg.Exptime)
	case 'O':
		resp = "OK "
		ver, _ := strconv.Atoi(msg.Version)
		if ver > 0 {
			resp += msg.Version
		}
	case 'F':
		resp = "ERR_FILE_NOT_FOUND"
	case 'V':
		resp = "ERR_VERSION " + msg.Version
	case 'M':
		resp = "ERR_CMD_ERR"
	case 'I':
		resp = "ERR_INTERNAL"
	default:
		fmt.Printf("Unknown response kind '%c'", msg.Kind2)
		return false
	}
	resp += "\r\n"
	write([]byte(resp))
	if msg.Kind2 == 'C' {
		write(msg.Contents)
		write(crlf)
	}
	return err == nil
}

func (c *ClientHandler) handleConnection(conn *net.TCPConn, rafts []RaftNode) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		msg, msgerr, fatalerr := GetMsg(reader)
		if fatalerr != nil || msgerr != nil {
			reply(conn, &Msg{Kind2: 'M'})
			conn.Close()
			break
		}

		if msgerr != nil {
			if (!reply(conn, &Msg{Kind2: 'M'})) {
				conn.Close()
				break
			}
		}
		// fmt.Println("f " + msg.Filename)
		switch msg.Kind2 {

		case 'w':
			b, _ := json.Marshal(msg)
			// fmt.Println("content is : " + string(msg.Contents))
			Replicate(b, rafts)
			r := <-c.route
			conn.Write([]byte(r.Output))
		case 'r':
			b, _ := json.Marshal(msg)
			Replicate(b, rafts)
			r := <-c.route
			// fmt.Println("c : " + string(r.Output) + " " + string(r.File))
			if r.Err {
				conn.Write([]byte(r.Output))
				return
			} else {
				conn.Write([]byte(r.Output))
			}
			for i := 0; i < len(r.File); i++ {
				conn.Write(r.File[i : i+1])
			}
			_, err := conn.Write([]byte("\r\n"))

			if err != nil && err != io.EOF {
				conn.Write([]byte("ERR_INTERNAL\r\n"))
			}
		case 'c':
			b, _ := json.Marshal(msg)
			Replicate(b, rafts)
			r := <-c.route
			conn.Write([]byte(r.Output))
		case 'd':
			b, _ := json.Marshal(msg)
			Replicate(b, rafts)
			r := <-c.route
			conn.Write([]byte(r.Output))
		}
	}
	// response := ProcessMsg(msg)
	// reply(conn, response)
	// conn.Close()

}

type Response struct {
	Output string
	Err    bool
	File   []byte
}
