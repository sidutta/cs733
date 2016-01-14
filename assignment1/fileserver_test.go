package main

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

func TestMain(t *testing.T) {
	// Start the server
	go serverMain()
	time.Sleep(1 * time.Second)
}

func fireTestCases(t *testing.T, testcases []string, client_count int) {
	wait_ch := make(chan int, client_count)

	for i := 0; i < client_count; i++ {
		go shootTestCase(t, i+1, testcases, &wait_ch)
	}

	return_count := 0

	for return_count < client_count {
		routine_done := <-wait_ch
		routine_done++
		return_count++
	}
}

func shootTestCase(t *testing.T, routineID int, testcases []string, wait_ch *chan int) {
	// fmt.Println(CONNECTION_TYPE, HOST+":"+PORT)
	addr, err := net.ResolveTCPAddr(CONNECTION_TYPE, HOST+":"+PORT)
	conn, err := net.DialTCP(CONNECTION_TYPE, nil, addr)

	if err != nil {
		fmt.Println("Error while dialing server")
		return
	}

	defer conn.Close()

	for _, testcase := range testcases {

		conn.Write([]byte(testcase))
		got := make([]byte, 1024)

		size, err := conn.Read(got)

		if err != nil {
			t.Errorf("Error while reading from the server: ", err.Error())
		}

		got = got[:size]
		response := string(got)
		response = strings.TrimSpace(response)

		time.Sleep(1 * time.Second)

	}

	*wait_ch <- routineID

}

func Test1(t *testing.T) {

	client_count := 1

	var testcases = []string{
		"write siddhartha.txt 6\r\ngregre\r\n",
	}
	fireTestCases(t, testcases, client_count)
}
