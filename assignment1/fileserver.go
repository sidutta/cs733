package main

import (
	// "bufio"
	// "io/ioutil"
	"log"
	"net"
	// "strconv"
	"io"
	// "strings"
	// "time"
	"fmt"
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

	// Keep listening for incoming connections
	for {
		conn, err := sock.Accept()
		if err != nil {
			log.Println("Error while accepting incoming connection: ", err.Error())
		}

		// Connections handled in new goroutine
		go request_handler(conn)
	}
}

func main() {
	serverMain()
}

// Command write
func write(conn net.Conn, input []string, noReply *bool) {

	filename := input[0]
	fmt.Println("Filename: " + filename)

}

// Request Handler
func request_handler(conn net.Conn) {

	// To ensure closing of connection on exit from the function
	defer conn.Close()
	buf := make([]byte, 0, 10)
	tmp := make([]byte, 10)

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

	if buf[0] == 'r' || buf[0] == 'd' {
		nrexpected = 1
	} else {
		nrexpected = 2
	}

	for {
		for i := 0; i < bytes_read; i++ {
			if tmp[i] == '\n' && tmp[i-1] == '\r' {
				nrobserved++
			}
		}
		if nrobserved == nrexpected {
			conn.Write(buf)
			// fmt.Println("total size:", len(buf))
			// log.Print(string(buf))
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
	}

}
