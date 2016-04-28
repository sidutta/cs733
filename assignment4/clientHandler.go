package main

import (
	"encoding/json"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

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

type ClientHandler struct {
	Id    int64
	route chan Response
}

// const (
// 	CONNECTION_TYPE = "tcp"
// 	HOST            = "localhost"
// 	PORT            = "8080"
// )

//defining the mutex to be used for RW operation
// var mutex = &sync.RWMutex{}

func (c *ClientHandler) handleConnection(conn net.Conn, rafts []RaftNode) {

	defer conn.Close()

	// // Setting the port to listen on
	// sock, err := net.Listen(CONNECTION_TYPE, HOST+":"+PORT)
	// if err != nil {
	// 	log.Print("Error in listening:", err.Error())
	// }
	//
	// // Close the listener when the application closes.
	// defer sock.Close()
	// log.Println("Listening on " + HOST + ":" + PORT + " for incoming connections")
	//
	// datadb, err := leveldb.OpenFile("datadb", nil)
	// defer datadb.Close()
	// metadatadb, err := leveldb.OpenFile("metadatadb", nil)
	// defer metadatadb.Close()

	// Keep listening for incoming connections
	// for {
	// 		conn, err := sock.Accept()
	// 		if err != nil {
	// 			log.Println("Error while accepting incoming connection: ", err.Error())
	// 		}
	//
	// 		// Connections handled in new goroutine
	// 		go request_handler(conn, datadb, metadatadb)
	// 	}

	buffer_size := 1024

	// To ensure closing of connection on exit from the function
	defer conn.Close()

	leftover := make([]byte, 0)
	for {
		buf := make([]byte, 0, buffer_size)
		tmp := make([]byte, 1024, buffer_size)
		// log.Println("read error1")

		// var bytes_read int
		// var err error
		// c := make(chan int)
		// 		go reader(&bytes_read, &err, &conn, &tmp, c)
		// 		go cutter(c)
		// 		select {
		// 		case <-c:
		//
		// 			// case <-done:
		// 		}
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		bytes_read, err := conn.Read(tmp)
		if err != nil && err == io.EOF {
			return
			log.Println("read error22: ", err)
		}

		tmp = tmp[:bytes_read]

		// log.Println("read error2")
		// This prevents a crash and gives time by which test process completely closes the connection
		if bytes_read == 0 && len(leftover) == 0 {
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				} else {
					log.Println("error occurred:", err)
				}
			} else {
				continue
			}
		}
		// log.Println("defe", bytes_read, string(tmp), string(leftover))
		command := ""
		// log.Println("read error3")
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// log.Println("read timeout:", err)
				// time out
			} else if err != io.EOF {
				log.Println("read error1: ", err)
			} else {
				log.Println("read error2: ", err)
				conn.Write(append([]byte("ERR_INTERNAL\r\n")))
			}
		}

		// log.Println("read error4")
		leftover_bytes := 0

		if len(leftover) != 0 {

			tmp = append(leftover, tmp...)
			leftover_bytes = len(leftover)
		}

		leftover = make([]byte, 0)

		nrobserved := 0
		bytes_in_first_line := 0

		iterations := 0
		// robserved := false
		content_bytes_read := 0
		content_size := 0
		for {
			var i int
			for i = 0; i < bytes_read+leftover_bytes; i++ {
				if nrobserved == 1 {
					content_bytes_read++
				}

				if nrobserved == 0 && tmp[i] == '\n' && i != 0 && tmp[i-1] == '\r' {
					nrobserved++
					bytes_in_first_line = (i + 1) + iterations*buffer_size
					part := string(tmp[:i-1])
					fields := strings.Fields(part)
					command = fields[0]

					if !(command == "read" || command == "delete" || command == "cas" || command == "write") {
						// invalid command, return error, close connection, exit go routine
						conn.Write(append([]byte("ERR_CMD_ERR\r\n")))
						conn.Close()
						return
					}

					if command == "read" || command == "delete" {
						if len(fields) != 2 {
							conn.Write(append([]byte("ERR_CMD_ERR\r\n")))
							conn.Close()
							return
						}
						if i+1 != bytes_read {
							leftover = tmp[i+1:]
						}
						break
					}

					if command == "cas" || command == "write" {

						if command == "cas" {
							if len(fields) != 4 && len(fields) != 5 {
								conn.Write(append([]byte("ERR_CMD_ERR\r\n")))
								conn.Close()
								return
							}
							content_size, _ = strconv.Atoi(fields[3])
						}
						if command == "write" {

							if len(fields) != 3 && len(fields) != 4 {
								conn.Write(append([]byte("ERR_CMD_ERR\r\n")))
								conn.Close()
								return
							}
							content_size, _ = strconv.Atoi(fields[2])
						}
					}
				}

				if content_bytes_read == content_size+2 {
					if i != bytes_read+leftover_bytes-1 {
						leftover = tmp[i+1:]
						break
					}
				}
			}
			if nrobserved == 1 && (command == "read" || command == "delete") {
				buf = append(buf, tmp[:i+1]...)
				break
			} else if i == bytes_read+leftover_bytes && content_bytes_read == content_size+2 {
				buf = append(buf, tmp[:bytes_read+leftover_bytes]...)
				break
			} else if i == bytes_read+leftover_bytes {
				buf = append(buf, tmp[:bytes_read+leftover_bytes]...)
			} else if content_bytes_read == content_size+2 {
				buf = append(buf, tmp[:i+1]...)
				break
			}
			conn.SetReadDeadline(time.Now().Add(200 * time.Second))
			bytes_read, err = conn.Read(tmp)
			if err != nil {
				if err != io.EOF {
					log.Println("read error: ", err)
				}
				break
			}
			leftover_bytes = 0
			iterations++
		}

		switch command {

		case "write":
			// log.Println("here", string(buf))
			write(conn, buf, bytes_in_first_line, c, rafts)
		case "read":
			// log.Println("here2", string(buf))
			read(conn, buf, c, rafts)
		case "cas":
			// log.Println("here3", string(buf))
			cas(conn, buf, bytes_in_first_line, c, rafts)
		case "delete":
			// log.Println("here4", string(buf))
			delete(conn, buf, c, rafts)
		}
	}

}

// Command write
func write(conn net.Conn, input_bytes []byte, bytes_in_first_line int, c *ClientHandler, rafts []RaftNode) {
	input_string := string(input_bytes)
	inputs := strings.Fields(input_string)

	if len(inputs) < 3 {
		conn.Write([]byte("ERR_CMD_ERR\r\n"))
		return
	}

	filename := inputs[1]
	numbytes := inputs[2]
	var exptime string
	var exp string
	if len(inputs) == 4 {
		exptime = "2018-02-01 03:04:05 +0530 IST"
		exp = "0"
	} else {
		exp = inputs[3]
		delay, _ := strconv.Atoi(exp)
		exptime = time.Now().Add(time.Duration(delay) * time.Second).String()
	}

	msg := Msg{c.Id, 2, filename, []byte(input_bytes[bytes_in_first_line:]), exptime, "-1", exp, numbytes}

	b, _ := json.Marshal(msg)
	Replicate(b, rafts)
	r := <-c.route
	conn.Write([]byte(r.Output))
}

// Command read
func read(conn net.Conn, input_bytes []byte, c *ClientHandler, rafts []RaftNode) {
	input_string := string(input_bytes)
	inputs := strings.Fields(input_string)
	filename := inputs[1]

	msg := Msg{c.Id, 0, filename, nil, "-1", "-1", "0", "-1"}
	b, _ := json.Marshal(msg)
	Replicate(b, rafts)
	r := <-c.route
	if r.Err {
		conn.Write([]byte(r.Output))
		return
	} else {
		conn.Write([]byte(r.Output))
	}

	for i := 0; i < len(r.File)-2; i++ {
		conn.Write(r.File[i : i+1])
	}
	_, err := conn.Write([]byte("\r\n"))

	if err != nil && err != io.EOF {
		conn.Write([]byte("ERR_INTERNAL\r\n"))
	}
}

// Command cas
func cas(conn net.Conn, input_bytes []byte, bytes_in_first_line int, c *ClientHandler, rafts []RaftNode) {
	input_string := string(input_bytes)
	inputs := strings.Fields(input_string)
	filename := inputs[1]
	req_version := inputs[2]
	numbytes := inputs[3]

	var exptime string
	var exp string
	if len(inputs) == 5 {
		exptime = "2018-02-01 03:04:05 +0530 IST"
		exp = "0"
	} else {
		exp = inputs[4]
		delay, _ := strconv.Atoi(exp)
		exptime = time.Now().Add(time.Duration(delay) * time.Second).String()
	}

	msg := Msg{c.Id, 3, filename, []byte(input_bytes[bytes_in_first_line:]), exptime, req_version, exp, numbytes}
	b, _ := json.Marshal(msg)
	Replicate(b, rafts)
	r := <-c.route
	conn.Write([]byte(r.Output))

}

// Command delete
func delete(conn net.Conn, input_bytes []byte, c *ClientHandler, rafts []RaftNode) {
	input_string := string(input_bytes)
	inputs := strings.Fields(input_string)
	filename := inputs[1]

	msg := Msg{c.Id, 1, filename, nil, "-1", "-1", "0", "-1"}
	b, _ := json.Marshal(msg)
	Replicate(b, rafts)
	r := <-c.route
	conn.Write([]byte(r.Output))
}

func reader(bytes_read *int, err *error, conn *net.Conn, tmp *[]byte, c chan int) {
	*bytes_read, *err = (*conn).Read(*tmp)
	log.Println("Fr")
	c <- 0
}

func cutter(c chan int) {
	time.Sleep(1 * time.Second)
	c <- 1
}
