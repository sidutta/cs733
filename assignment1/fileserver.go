package main

import (
	"github.com/goleveldb/leveldb"
	"io"
	"log"

	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	CONNECTION_TYPE = "tcp"
	HOST            = "localhost"
	PORT            = "8080"
)

//defining the mutex to be used for RW operation
var mutex = &sync.RWMutex{}

func serverMain() {

	// Setting the port to listen on
	sock, err := net.Listen(CONNECTION_TYPE, HOST+":"+PORT)
	if err != nil {
		log.Print("Error in listening:", err.Error())
	}

	// Close the listener when the application closes.
	defer sock.Close()
	log.Println("Listening on " + HOST + ":" + PORT + " for incoming connections")

	datadb, err := leveldb.OpenFile("datadb", nil)
	defer datadb.Close()
	metadatadb, err := leveldb.OpenFile("metadatadb", nil)
	defer metadatadb.Close()

	// Keep listening for incoming connections
	for {
		conn, err := sock.Accept()
		if err != nil {
			log.Println("Error while accepting incoming connection: ", err.Error())
		}

		// Connections handled in new goroutine
		go request_handler(conn, datadb, metadatadb)
	}
}

func main() {
	serverMain()
}

func read_metadata(filename string, metadatadb *leveldb.DB) (version int, numbytes int, exptime time.Time, exp int, err error) {
	data, err2 := metadatadb.Get([]byte(filename), nil)
	// log.Println(data, filename, err2)
	err = err2
	if err != nil {
		version = 0
		numbytes = 0
		exptime = time.Now()
		exp = 0
	} else {
		line := string(data)
		fields := strings.Fields(line)
		version, _ = strconv.Atoi(fields[0])
		numbytes, _ = strconv.Atoi(fields[1])
		if len(fields) == 7 {
			exptime_str := fields[2] + " " + fields[3] + " " + fields[4] + " " + fields[5]
			layout := "2006-01-02 15:04:05 -0700 MST"
			exptime, _ = time.Parse(layout, exptime_str)
			exp, _ = strconv.Atoi(fields[6])
		} else {
			log.Println("why here?", string(data), string(filename))
		}
	}
	return
}

// Command write
func write(conn net.Conn, input_bytes []byte, datadb *leveldb.DB, metadatadb *leveldb.DB, bytes_in_first_line int) {
	input_string := string(input_bytes)
	inputs := strings.Fields(input_string)
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
	mutex.Lock()
	prev_version_int, _, _, _, err := read_metadata(filename, metadatadb)
	new_version := ""
	if err == nil {
		if err != nil {
			log.Println("error in conversion: ", err)
		}
		saved_metadata := strconv.Itoa(prev_version_int+1) + " " + numbytes + " " + exptime + " " + exp
		err = metadatadb.Put([]byte(filename), []byte(saved_metadata), nil)
		if err != nil {
			log.Println("failed to add to database: ", err)
		}
		new_version = strconv.Itoa(prev_version_int + 1)
	} else {
		saved_metadata := strconv.Itoa(1) + " " + numbytes + " " + exptime + " " + exp
		err = metadatadb.Put([]byte(filename), []byte(saved_metadata), nil)
		if err != nil {
			log.Println("failed to add to database: ", err)
		}
		new_version = "1"
	}
	err = datadb.Put([]byte(filename), []byte(input_bytes[bytes_in_first_line:]), nil)
	mutex.Unlock()
	if err != nil {
		log.Println("failed to add to database: ", err)
	}
	response := "OK " + string(new_version) + "\r\n"
	// log.Println(response)
	some_int, err := conn.Write([]byte(response))
	if err != nil {
		log.Println("failed to reply back: ", err, some_int)
	}
}

// Command read
func read(conn net.Conn, input_bytes []byte, datadb *leveldb.DB, metadatadb *leveldb.DB) {
	input_string := string(input_bytes)
	inputs := strings.Fields(input_string)
	filename := inputs[1]

	mutex.RLock()
	version, numbytes, exptime, exp, err1 := read_metadata(filename, metadatadb)

	if err1 == nil {
		if time.Now().After(exptime) {
			mutex.RUnlock()
			conn.Write([]byte("ERR_FILE_NOT_FOUND\r\n")) // content has expired
		} else {
			data, err2 := datadb.Get([]byte(filename), nil)
			mutex.RUnlock()
			if err2 != nil {
				log.Println("error in conversion: ", err1)
			}
			version_str := strconv.Itoa(version)
			numbytes_str := strconv.Itoa(numbytes)

			exp_str := "0"
			if exp != 0 {
				exp_str = strconv.Itoa(int(exptime.Sub(time.Now()).Seconds()))
			}
			response := append([]byte("CONTENTS "+version_str+" "+numbytes_str+" "+exp_str+" \r\n"), data...)
			// log.Println(response)
			conn.Write(response)
			// log.Println(response)
		}
	} else {
		mutex.RUnlock()
		conn.Write([]byte("ERR_FILE_NOT_FOUND\r\n"))
	}
}

// Command cas
func cas(conn net.Conn, input_bytes []byte, datadb *leveldb.DB, metadatadb *leveldb.DB, bytes_in_first_line int) {
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
		dot_pos := strings.Index(exptime, ".")
		exptime = exptime[:dot_pos]
	}
	mutex.Lock()
	prev_version_int, _, _, _, err := read_metadata(filename, metadatadb)

	if err != nil {
		mutex.Unlock()
		conn.Write([]byte("ERR_FILE_NOT_FOUND\r\n"))
	} else {

		req_version_int, _ := strconv.Atoi(req_version)

		if err != nil {
			log.Println("error in conversion: ", err)
		}

		if prev_version_int != req_version_int {
			prev_version_str := strconv.Itoa(prev_version_int)
			conn.Write([]byte("ERR_VERSION " + prev_version_str + "\r\n"))
		} else {

			new_version := ""

			saved_metadata := strconv.Itoa(prev_version_int+1) + " " + numbytes + " " + exptime + " " + exp
			err = metadatadb.Put([]byte(filename), []byte(saved_metadata), nil)

			if err != nil {
				log.Println("failed to add to database: ", err)
			}
			new_version = strconv.Itoa(prev_version_int + 1)

			err = datadb.Put([]byte(filename), []byte(input_bytes[bytes_in_first_line:]), nil)
			mutex.Unlock()
			if err != nil {
				log.Println("failed to add to database: ", err)
			}

			response := "OK " + string(new_version) + "\r\n"
			some_int, err := conn.Write([]byte(response))

			if err != nil {
				log.Println("failed to reply back: ", err, some_int)
			}
		}
	}
}

// Command dele
func delete(conn net.Conn, input_bytes []byte, datadb *leveldb.DB, metadatadb *leveldb.DB) {
	input_string := string(input_bytes)
	inputs := strings.Fields(input_string)
	filename := inputs[1]
	mutex.Lock()
	_, _, _, _, err := read_metadata(filename, metadatadb)

	if err != nil {
		mutex.Unlock()
		conn.Write(append([]byte("ERR_FILE_NOT_FOUND\r\n")))
	} else {

		datadb.Delete([]byte(filename), nil)
		metadatadb.Delete([]byte(filename), nil)
		mutex.Unlock()
		conn.Write(append([]byte("OK\r\n")))

	}
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

// Request Handler
func request_handler(conn net.Conn, datadb *leveldb.DB, metadatadb *leveldb.DB) {

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
			write(conn, buf, datadb, metadatadb, bytes_in_first_line)
		case "read":
			read(conn, buf, datadb, metadatadb)
		case "cas":
			cas(conn, buf, datadb, metadatadb, bytes_in_first_line)
		case "delete":
			delete(conn, buf, datadb, metadatadb)
		}
	}
}
