package main

import (
	// "bufio"
	"time"
	// "io/ioutil"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	// "time"
	// "encoding/binary"
	"./goleveldb/leveldb"
	"fmt"
	// "reflect"
)

const (
	CONNECTION_TYPE = "tcp"
	HOST            = "localhost"
	PORT            = "8080"
)

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
	versiondb, err := leveldb.OpenFile("versiondb", nil)
	defer versiondb.Close()

	// Keep listening for incoming connections
	for {
		conn, err := sock.Accept()
		if err != nil {
			log.Println("Error while accepting incoming connection: ", err.Error())
		}

		// Connections handled in new goroutine
		go request_handler(conn, datadb, versiondb)
	}
}

func main() {
	serverMain()
}

// Command write
func write(conn net.Conn, input_bytes []byte, datadb *leveldb.DB, versiondb *leveldb.DB, bytes_in_first_line int) {
	input_string := string(input_bytes)
	inputs := strings.Fields(input_string)
	filename := inputs[1]
	prev_version, err := versiondb.Get([]byte(filename), nil)
	new_version := ""
	if err == nil {

		prev_version_int := int(prev_version[0])

		if err != nil {
			fmt.Println("error in conversion: ", err)
		}

		err = versiondb.Put([]byte(filename), []byte(string(prev_version_int+1)), nil)
		if err != nil {
			fmt.Println("failed to add to database: ", err)
		}
		new_version = strconv.Itoa(prev_version_int + 1)
	} else {
		err = versiondb.Put([]byte(filename), []byte(string(1)), nil)
		fmt.Println("first time: ", err)
		if err != nil {
			fmt.Println("failed to add to database: ", err)
		}
		new_version = "1"
	}

	err = datadb.Put([]byte(filename), []byte(input_bytes[bytes_in_first_line:]), nil)

	if err != nil {
		fmt.Println("failed to add to database: ", err)
	}

	response := "OK " + string(new_version) + "\r\n"
	some_int, err := conn.Write([]byte(response))

	if err != nil {
		fmt.Println("failed to reply back: ", err, some_int)
	}
}

// Command read
func read(conn net.Conn, input_bytes []byte, datadb *leveldb.DB, versiondb *leveldb.DB) {
	input_string := string(input_bytes)
	inputs := strings.Fields(input_string)
	filename := inputs[0]

	version, err := versiondb.Get([]byte(filename), nil)

	if err != nil {

		data, err2 := datadb.Get([]byte(filename), nil)

		if err2 != nil {
			fmt.Println("error in conversion: ", err)
		}

		conn.Write(append([]byte("CONTENTS "+string(version)+" \r\n"), data...))

	} else {
		// TODO
		fmt.Println("filename not found")
	}
}

// Request Handler
func request_handler(conn net.Conn, datadb *leveldb.DB, versiondb *leveldb.DB) {

	buffer_size := 10

	// To ensure closing of connection on exit from the function
	defer conn.Close()

	for {
		buf := make([]byte, 0, buffer_size)
		tmp := make([]byte, buffer_size)

		bytes_read, err := conn.Read(tmp)
		if err != nil {
			if err != io.EOF {
				fmt.Println("read error: ", err)
			}
			// TODO: return error
		}
		buf = append(buf, tmp[:bytes_read]...)

		nrobserved := 0
		nrexpected := 0
		bytes_in_first_line := 0
		bytes_in_first_line_set := false

		if buf[0] == 'r' || buf[0] == 'd' {
			nrexpected = 1
		} else {
			nrexpected = 2
		}
		iterations := 0
		for {
			// fmt.Println(string(tmp), bytes_read)
			for i := 0; i < bytes_read; i++ {
				if tmp[i] == '\n' && tmp[i-1] == '\r' {

					nrobserved++
					if !bytes_in_first_line_set {
						bytes_in_first_line = (i + 1) + iterations*buffer_size
						bytes_in_first_line_set = true
					}
				}
			}
			if nrobserved == nrexpected {
				break
			}

			bytes_read, err = conn.Read(tmp)
			if err != nil {
				if err != io.EOF {
					fmt.Println("read error: ", err)
				}
				break
			}
			buf = append(buf, tmp[:bytes_read]...)
			iterations++
		}

		switch buf[0] {
		case 'w':
			write(conn, buf, datadb, versiondb, bytes_in_first_line)

		case 'r':
			read(conn, buf, datadb, versiondb)
			// case 'd':

			// case 'c':
			// cas(conn, buf, datadb, versiondb)

		}

		time.Sleep(2 * time.Second)

	}
}
