package main

import (
	"fmt"
	"io"
	"net"
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
		go shootTestCase(t, i+1, testcases, &wait_ch, i)
	}

	return_count := 0

	for return_count < client_count {
		routine_done := <-wait_ch
		routine_done++
		return_count++
	}
}

func shootTestCase(t *testing.T, routineID int, testcases []string, wait_ch *chan int, thread_no int) {
	addr, err := net.ResolveTCPAddr(CONNECTION_TYPE, HOST+":"+PORT)
	conn, err := net.DialTCP(CONNECTION_TYPE, nil, addr)
	for _, testcase := range testcases {

		if err != nil {
			fmt.Println("Error while dialing server")
			return
		}

		defer conn.Close()
		// fmt.Println("fwrfw")
		conn.Write([]byte(testcase))
		fmt.Println(testcase)

		// got := make([]byte, 1024)
		//
		// 		size, err := conn.Read(got)
		//
		// 		if err != nil {
		// 			t.Errorf("Error while reading from the server: ", err.Error())
		// 		}
		//
		// 		got = got[:size]
		// 		response := string(got)
		// 		response = strings.TrimSpace(response)
		//
		// 		fmt.Println(response)

		buffer_size := 1024

		buf := make([]byte, 0, buffer_size)
		tmp := make([]byte, buffer_size)

		// time.Sleep(10 * time.Second)

		bytes_read := 0
		// fmt.Println("fwrfw")
		for bytes_read == 0 {
			bytes_read, err = conn.Read(tmp)
		}
		// fmt.Println("fwrfw")
		// This prevents a crash and gives time by which test process completely closes the connection
		// if bytes_read == 0 {
		//
		// 			continue
		// 		}

		if err != nil {
			if err != io.EOF {
				fmt.Println("read error: ", err)
			}
			// TODO: return error
		}
		buf = append(buf, tmp[:bytes_read]...)

		nrobserved := 0
		nrexpected := 0
		// bytes_in_first_line := 0
		// bytes_in_first_line_set := false

		if testcase[0] == 'r' {
			nrexpected = 2
		} else {
			nrexpected = 1
		}
		iterations := 0

		robserved := false
		for {
			// fmt.Println((tmp), bytes_read)
			for i := 0; i < bytes_read; i++ {
				if i == 0 && tmp[i] == '\n' && robserved {
					nrobserved++
				} else if tmp[i] == '\n' && tmp[i-1] == '\r' && i != 0 {
					nrobserved++
					// if !bytes_in_first_line_set {
					// 						bytes_in_first_line = (i + 1) + iterations*buffer_size
					// 						bytes_in_first_line_set = true
					// 					}
				}
				if i == 0 && robserved {
					robserved = false
				}
				if i == bytes_read-1 && tmp[i] == '\r' {
					robserved = true
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

		fmt.Printf("Thread #%d: %s", thread_no, string(buf))

		// time.Sleep(3 * time.Second)

	}
	*wait_ch <- routineID
}

func Test1(t *testing.T) {

	client_count := 100

	var testcases = []string{
		// "write siddhartha.txt 37 300\r\ngrgegvvebete1gegvvebetegegvve2betegre\r\n",
		"write siddhartha22.txt 4 5\r\naass\r\n",
		"write siddhartha23.txt 9\r\ntttrrrfff\r\n",
		// "delete siddhartha4.txt\r\n",
		// 		"cas siddhartha5.txt 15 6\r\ngregre\r\n",

		"read siddhartha23.txt\r\n",
		"read siddhartha22.txt\r\n",

		// 		"write siddhartha.txt 74 300\r\ngrgegvvebete1gegvvebetegegvve2betegregrgegvvebete1gegvvebetegegvve2betegre\r\n",
		// 		"cas siddhartha.txt 14 6 300\r\ngregre\r\n",
		// 		"read siddhartha.txt\r\n",
		// 		"read siddhartha.txt\r\n",
		// 		"cas siddhartha.txt 15 6\r\ngregre\r\n",
	}
	//
	// var answers = []string{
	// 	"write siddhartha.txt 6\r\ngregre\r\n",
	// }

	fireTestCases(t, testcases, client_count)
}
