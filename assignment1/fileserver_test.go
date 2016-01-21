package main

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var versionSetMutex = &sync.RWMutex{}

type IntSet struct {
	set map[int]bool
	ver int
}

func NewIntSet() *IntSet {
	return &IntSet{make(map[int]bool), 0}
}

func (set *IntSet) AddVer(v int) {
	set.ver = v
}

func (set *IntSet) GetVer() int {
	return set.ver
}

func (set *IntSet) GetVerString() string {
	return strconv.Itoa(set.ver)
}

func (set *IntSet) Size() int {
	versionSetMutex.Lock()
	sz := len(set.set)
	versionSetMutex.Unlock()
	return sz
}

func (set *IntSet) Add(i int) bool {
	versionSetMutex.Lock()
	_, found := set.set[i]
	set.set[i] = true
	versionSetMutex.Unlock()
	return !found //False if it existed already
}

func (set *IntSet) Get(i int) bool {
	versionSetMutex.Lock()
	_, found := set.set[i]
	versionSetMutex.Unlock()

	return found //true if it existed already
}

func TestMain(t *testing.T) {
	// Start the server
	go serverMain()
	time.Sleep(1 * time.Second)
}

func fireTestCases(t *testing.T, testcases []string, client_count int, verNoSet *IntSet, kind int) {

	wait_ch := make(chan int, client_count)

	if kind == 1 {
		for i := 0; i < client_count; i++ {
			go shootTestCase_type1(t, i+1, testcases, &wait_ch, i, verNoSet, kind)
		}
	} else if kind == 2 {
		fmt.Println("why here?")
		// for i := 0; i < client_count; i++ {
		// 			go shootTestCase_type2(t, i+1, testcases, &wait_ch, i, verNoSet)
		// 		}
	} else if kind == 3 {
		for i := 0; i < client_count; i++ {
			go shootTestCase_type3(t, i+1, testcases, &wait_ch, i, verNoSet)
		}
	} else if kind == 4 {
		var testcases_mod []string
		testcases_mod = append(testcases_mod, testcases[0])
		var ver int
		for i := 0; i < client_count; i++ {
			ver = shootTestCase_type1(t, i+1, testcases_mod, &wait_ch, i, verNoSet, kind)
		}
		verNoSet.AddVer(ver)
		testcases_mod[0] = testcases[1]
		ver = shootTestCase_type1(t, 0, testcases_mod, &wait_ch, 0, verNoSet, kind)
		testcases_mod[0] = testcases[2] + verNoSet.GetVerString() + testcases[3]
		ver = shootTestCase_type1(t, 0, testcases_mod, &wait_ch, 0, verNoSet, kind)
	}
	if kind != 4 {
		return_count := 0

		for return_count < client_count {
			routine_done := <-wait_ch
			routine_done++
			return_count++
			// fmt.Println("return of", return_count)
		}
	}
}

// "write siddhartha.txt 37 300\r\ngrgegvvebete1gegvvebetegegvve2betegre\r\n",
// "write siddhartha22.txt 8 5\r\n\xbd\xb2\x3d\xbc\x20\xe2\x8c\x98\r\n",
// "write siddhartha23.txt 9\r\nasdqweqwe\r\n",
// "delete siddhartha4.txt\r\n",
// 		"cas siddhartha5.txt 15 6\r\ngregre\r\n",

// "read siddhartha23.txt\r\n",
// "read siddhartha22.txt\r\n",

// 		"write siddhartha.txt 74 300\r\ngrgegvvebete1gegvvebetegegvve2betegregrgegvvebete1gegvvebetegegvve2betegre\r\n",
// 		"cas siddhartha.txt 14 6 300\r\ngregre\r\n",
// 		"read siddhartha.txt\r\n",
// 		"read siddhartha.txt\r\n",
// 		"cas siddhartha.txt 15 6\r\ngregre\r\n",

func Test1(t *testing.T) {

	var client_count int
	verNoSet := NewIntSet()
	var testcases []string

	// -----------------------------------------------------------------
	client_count = 100
	testcases = []string{
		"write siddhartha23.txt 9\r\nasdqweqwe\r\n",
	}
	// 100 clients concurrently adding data
	fireTestCases(t, testcases, client_count, verNoSet, 1)
	// -----------------------------------------------------------------
	client_count = 1
	testcases = []string{
		"read siddhartha23.txt\r\n",
	}
	// checking whether 100 new version no.s generated and whether read returns version no. of one them
	fireTestCases(t, testcases, client_count, verNoSet, 1)
	// -----------------------------------------------------------------
	client_count = 100
	testcases = []string{
		"write siddhartha23.txt 9\r\nasdqweqwe\r\n",
		"read siddhartha23.txt\r\n",
		"cas siddhartha23.txt ", " 6\r\ngregre\r\n",
	}
	// 100 clients adding data sequentially and checking whether version no. corresponds to latest write, cas takes its version argument from last write thus it too must succeed
	fireTestCases(t, testcases, client_count, verNoSet, 4)
	// -----------------------------------------------------------------
	testcases = []string{
		// checking whether the cas succeeded
		"read siddhartha23.txt\r\n",
		"delete siddhartha23.txt\r\n",
		"write siddhartha25.txt 9 10\r\nasdqweqwe\r\n",
	}
	answers := []string{
		"gregre",
		"OK",
		"OK",
	}
	shootTestCase_type2(t, 1, testcases, answers, verNoSet)
	// -----------------------------------------------------------------
	time.Sleep(1 * time.Second)
	testcases = []string{
		// checking whether exptime decreased or not, check is exptime<15
		"read siddhartha25.txt\r\n",
	}
	answers = []string{
		"asdqweqwe",
	}
	shootTestCase_type2(t, 3, testcases, answers, verNoSet)
	// -----------------------------------------------------------------
	time.Sleep(10 * time.Second)
	testcases = []string{
		// checking whether file expired as it should after sleep of 1+10 seconds
		"read siddhartha25.txt\r\n",
	}
	answers = []string{
		"ERR_FILE_NOT_FOUND",
	}
	shootTestCase_type2(t, 2, testcases, answers, verNoSet)
	// -----------------------------------------------------------------
	testcases = []string{
		// checking whether the delete succeeded, file not found error expected
		"read siddhartha23.txt\r\n",
	}
	answers = []string{
		"ERR_FILE_NOT_FOUND",
	}

	shootTestCase_type2(t, 2, testcases, answers, verNoSet)
	// -----------------------------------------------------------------
	client_count = 100
	testcases = []string{
		"write ritik1.txt 3\r\nttt\r\nwrite siddhartha23.txt 9\r\ntttrrrfff\r\n",
	}
	//checks whether 2 OKs are recieved for every such joined line sent
	fireTestCases(t, testcases, client_count, verNoSet, 3)
	// -----------------------------------------------------------------
	client_count = 90
	// random 90 actions happening
	testcases1 := []string{
		"write siddhartha26.txt 9 10\r\nasdqweqwe\r\n",
	}
	testcases2 := []string{
		"read siddhartha26.txt\r\n",
	}
	testcases3 := []string{
		"delete siddhartha26.txt\r\n",
	}
	wait_ch := make(chan int, client_count)
	for i := 0; i < client_count/3; i++ {
		go shootTestCase_type6(t, 3*i+1, testcases1, &wait_ch, i, verNoSet, 6)
		go shootTestCase_type6(t, 3*i+2, testcases2, &wait_ch, i, verNoSet, 6)
		go shootTestCase_type6(t, 3*i+3, testcases3, &wait_ch, i, verNoSet, 6)
	}
	return_count := 0
	for return_count < client_count {
		routine_done := <-wait_ch
		routine_done++
		return_count++
	}

	// -----------------------------------------------------------------
	client_count = 1
	// checking for a 5000 character write, most people may use a buffer upto 4096
	testcases = []string{
		"write siddhartha26.txt 5000\r\nabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd\r\n",
	}
	answers = []string{
		"OK",
	}
	shootTestCase_type2(t, 1, testcases, answers, verNoSet)
	// -----------------------------------------------------------------
	// testcases = []string{
	// 		"write ritik1.txt 3\r\nttt\r\nwrite siddhartha23.txt 9\r\ntttrrrfff\r\n",
	// 	}
	// 	//checks whether 2 OKs are recieved for every such joined line sent
	// 	fireTestCases(t, testcases, client_count, verNoSet, 3)

}

func shootTestCase_type1(t *testing.T, routineID int, testcases []string, wait_ch *chan int, thread_no int, verNoSet *IntSet, kind int) (latest_ver int) {
	addr, err := net.ResolveTCPAddr(CONNECTION_TYPE, HOST+":"+PORT)
	conn, err := net.DialTCP(CONNECTION_TYPE, nil, addr)

	for _, testcase := range testcases {

		if err != nil {
			fmt.Println("Error while dialing server")
			return
		}

		defer conn.Close()
		conn.Write([]byte(testcase))

		buffer_size := 1024

		buf := make([]byte, 0, buffer_size)
		tmp := make([]byte, buffer_size)

		// time.Sleep(10 * time.Second)

		bytes_read := 0
		// fmt.Println("fwrfw")
		for bytes_read == 0 {
			bytes_read, err = conn.Read(tmp)
		}

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
		if kind != 6 {
			if testcase[0] == 'w' {
				ret := string(buf)
				fields := strings.Fields(ret)
				ver, _ := strconv.Atoi(fields[1])
				latest_ver = ver
				verNoSet.Add(ver)
			} else if testcase[0] == 'r' {
				ret := string(buf)
				fields := strings.Fields(ret)
				ver, _ := strconv.Atoi(fields[1])

				if verNoSet.Get(ver) != true {
					fmt.Println("Error: Version not found despite insertion")
				}
				expected_size := 100
				if kind == 4 {
					data := fields[4]
					if data != "asdqweqwe" {
						fmt.Println("rdwefwe")
					}
					expected_size = 200
				}
				if verNoSet.Size() != expected_size {
					fmt.Println("Error: Version number repeated or wrong no.. of insertions", "size:", verNoSet.Size())
				}
				latest_ver = verNoSet.GetVer()
				if kind == 4 && latest_ver != ver {
					fmt.Println("Error: Read doesnt correspond to latest addition", latest_ver, ver)
				}
			}
		}

	}
	if kind != 4 {
		*wait_ch <- routineID
	}
	return
}

func shootTestCase_type2(t *testing.T, routineID int, testcases []string, answers []string, verNoSet *IntSet) {
	addr, err := net.ResolveTCPAddr(CONNECTION_TYPE, HOST+":"+PORT)
	conn, err := net.DialTCP(CONNECTION_TYPE, nil, addr)
	if err != nil {
		fmt.Println("Error while dialing server")
		return
	}

	defer conn.Close()
	for ind, testcase := range testcases {

		conn.Write([]byte(testcase))

		buffer_size := 1024

		buf := make([]byte, 0, buffer_size)
		tmp := make([]byte, buffer_size)

		bytes_read := 0
		for bytes_read == 0 {
			bytes_read, err = conn.Read(tmp)
		}

		if err != nil {
			if err != io.EOF {
				fmt.Println("read error: ", err)
			}
			// TODO: return error
		}
		buf = append(buf, tmp[:bytes_read]...)

		nrobserved := 0
		nrexpected := 0

		if testcase[0] == 'r' {
			if routineID == 2 {
				nrexpected = 1
			} else {
				nrexpected = 2
			}
		} else {
			nrexpected = 1
		}
		iterations := 0

		robserved := false
		for {
			for i := 0; i < bytes_read; i++ {
				if i == 0 && tmp[i] == '\n' && robserved {
					nrobserved++
				} else if tmp[i] == '\n' && tmp[i-1] == '\r' && i != 0 {
					nrobserved++
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
			fmt.Println("fwrfw", string(testcase))
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

		if routineID == 3 {
			if testcase[0] == 'r' {
				ret := string(buf)
				fields := strings.Fields(ret)
				data := fields[4]
				if data != string(answers[ind]) {
					fmt.Println("error", data, string(answers[ind]))
				}
				exptime := fields[3]
				exptime_int, _ := strconv.Atoi(exptime)
				if exptime_int >= 10 {
					fmt.Println("error: expiry time didn't decrease")
				}
			}
		} else if routineID != 2 {

			if testcase[0] == 'r' {
				ret := string(buf)
				fields := strings.Fields(ret)
				data := fields[4]
				if data != string(answers[ind]) {
					fmt.Println("error", data, string(answers[ind]))
				}
			} else if testcase[0] == 'd' {
				ret := string(buf)
				fields := strings.Fields(ret)
				data := fields[0]
				if data != string(answers[ind]) {
					fmt.Println("error", data, string(answers[ind]))
				}
			} else if testcase[0] == 'w' {
				ret := string(buf)
				fields := strings.Fields(ret)
				data := fields[0]
				if data != string(answers[ind]) {
					fmt.Println("error", data, string(answers[ind]))
				}
			}
		} else {
			if testcase[0] == 'r' {
				ret := string(buf)
				fields := strings.Fields(ret)
				data := fields[0]
				if data != string(answers[ind]) {
					fmt.Println("error", data, string(answers[ind]))
				}
			}
		}

	}
}

func shootTestCase_type3(t *testing.T, routineID int, testcases []string, wait_ch *chan int, thread_no int, verNoSet *IntSet) {
	addr, err := net.ResolveTCPAddr(CONNECTION_TYPE, HOST+":"+PORT)
	conn, err := net.DialTCP(CONNECTION_TYPE, nil, addr)
	for _, testcase := range testcases {

		if err != nil {
			fmt.Println("Error while dialing server")
			return
		}

		defer conn.Close()
		conn.Write([]byte(testcase))

		for j := 0; j < 2; j++ {
			buffer_size := 1024

			buf := make([]byte, 0, buffer_size)
			tmp := make([]byte, buffer_size)

			// time.Sleep(10 * time.Second)

			bytes_read := 0
			// fmt.Println("fwrfw")
			for bytes_read == 0 {
				bytes_read, err = conn.Read(tmp)
			}

			if err != nil {
				if err != io.EOF {
					fmt.Println("read error: ", err)
				}
				// TODO: return error
			}
			buf = append(buf, tmp[:bytes_read]...)

			nrobserved := 0
			nrexpected := 0

			if testcase[0] == 'r' {
				nrexpected = 2
			} else {
				nrexpected = 1
			}
			iterations := 0

			robserved := false
			for {
				for i := 0; i < bytes_read; i++ {
					if i == 0 && tmp[i] == '\n' && robserved {
						nrobserved++
					} else if tmp[i] == '\n' && tmp[i-1] == '\r' && i != 0 {
						nrobserved++
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

		}

	}
	*wait_ch <- routineID
}

func shootTestCase_type6(t *testing.T, routineID int, testcases []string, wait_ch *chan int, thread_no int, verNoSet *IntSet, kind int) (latest_ver int) {
	addr, err := net.ResolveTCPAddr(CONNECTION_TYPE, HOST+":"+PORT)
	conn, err := net.DialTCP(CONNECTION_TYPE, nil, addr)

	for _, testcase := range testcases {

		if err != nil {
			fmt.Println("Error while dialing server")
			return
		}

		defer conn.Close()
		conn.Write([]byte(testcase))

		buffer_size := 1024

		buf := make([]byte, 0, buffer_size)
		tmp := make([]byte, buffer_size)

		bytes_read := 0
		for bytes_read == 0 {
			bytes_read, err = conn.Read(tmp)
		}

		if err != nil {
			if err != io.EOF {
				fmt.Println("read error: ", err)
			}
			// TODO: return error
		}
		buf = append(buf, tmp[:bytes_read]...)

	}
	if kind != 4 {
		*wait_ch <- routineID
	}
	return
}
