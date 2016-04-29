package main

import "net"
import "fmt"
import "bufio"
import "os"
import "strings"
import "strconv"
import "github.com/cs733-iitb/log"
import "github.com/syndtr/goleveldb/leveldb"

import (
	"testing"
	"time"
)

func DBReset() {

	currentTermDB, _ := leveldb.OpenFile(PATH+"/currentTerm", nil)
	defer currentTermDB.Close()
	for i := 0; i < len(configs.Peers); i++ {
		currentTermDB.Put([]byte(strconv.FormatInt(int64(i), 10)), []byte(strconv.FormatInt(int64(0), 10)), nil)
	}

	votedForDB, _ := leveldb.OpenFile(PATH+"/votedFor", nil)
	defer votedForDB.Close()

	for i := 0; i < len(configs.Peers); i++ {
		votedForDB.Put([]byte(strconv.FormatInt(int64(configs.Peers[i].Id), 10)), []byte(strconv.FormatInt(int64(-1), 10)), nil)

		os.RemoveAll(PATH + "/Log" + strconv.Itoa(configs.Peers[i].Id))

		lg, _ := log.Open(PATH + "/Log" + strconv.Itoa(configs.Peers[i].Id))
		// lg.TruncateToEnd(0)
		lg.Close()
	}
}

func TestFileSystem(t *testing.T) {
	DBReset()

	rafts := makeRafts()
	go serverMain(rafts)
	time.Sleep(5 * time.Second)

	name := "hi.txt"
	contents := "bye"
	exptime := 3000
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		t.Error(err.Error())
	}

	scanner := bufio.NewScanner(conn)

	// trying to delete a non-existent file
	text := "delete hi.txt\r\n"
	fmt.Fprintf(conn, text)
	scanner.Scan()                  // read first line
	resp := scanner.Text()          // extract the text from the buffer
	arr := strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "ERR_FILE_NOT_FOUND")

	// writing to a file
	fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name, len(contents), exptime, contents)
	scanner.Scan()                 // read first line
	resp = scanner.Text()          // extract the text from the buffer
	arr = strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "OK")
	_, err = strconv.Atoi(arr[1]) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}
	expect(t, arr[1], "1")

	//checking whether what was written is read correctly
	fmt.Fprintf(conn, "read %v\r\n", name)
	scanner.Scan()                 // read first line
	resp = scanner.Text()          // extract the text from the buffer
	arr = strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[1], "1")
	expect(t, arr[2], "3")
	scanner.Scan()        // read first line
	resp = scanner.Text() // extract the text from the buffer
	expect(t, resp, "bye")

	// checking version increment on rewriting
	contents = "byebye"
	fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name, len(contents), exptime, contents)
	scanner.Scan()                 // read first line
	resp = scanner.Text()          // extract the text from the buffer
	arr = strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "OK")
	_, err = strconv.Atoi(arr[1]) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}
	// version := int64(ver)
	expect(t, arr[1], "2")

	// checking whether the  re-written data is read correctly and version incremented
	fmt.Fprintf(conn, "read %v\r\n", name)
	scanner.Scan()                 // read first line
	resp = scanner.Text()          // extract the text from the buffer
	arr = strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[1], "2")
	expect(t, arr[2], "6")
	scanner.Scan()        // read first line
	resp = scanner.Text() // extract the text from the buffer
	expect(t, resp, "byebye")

	// now delete should work since the file exists
	fmt.Fprintf(conn, text)
	scanner.Scan()                 // read first line
	resp = scanner.Text()          // extract the text from the buffer
	arr = strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "OK")

	// writing a dummy file, will use it later after leader shutdown
	// there's no expiry time associated with it
	fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", "dummy.txt", 5, 10000, "apple")
	scanner.Scan()                 // read first line
	resp = scanner.Text()          // extract the text from the buffer
	arr = strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "OK")
	_, err = strconv.Atoi(arr[1]) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}
	expect(t, arr[1], "1")

	// making concurrent writes
	parallel_writes_count := 500

	wait_ch := make(chan int, parallel_writes_count)
	wait_ch2 := make(chan int, parallel_writes_count)

	for i := 0; i < parallel_writes_count; i++ {
		go conc_writes(t, &wait_ch, &wait_ch2, conn)
	}
	for i := 0; i < parallel_writes_count; i++ {
		wait_ch <- 1
	}
	for i := 0; i < parallel_writes_count; i++ {
		<-wait_ch2
	}

	// checking version number increment(must be parallel_writes_count + 1)
	contents = "byebye"
	fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", "input.txt", len(contents), 1, contents)
	scanner.Scan()                 // read first line
	resp = scanner.Text()          // extract the text from the buffer
	arr = strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "OK")
	_, err = strconv.Atoi(arr[1]) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}
	// version := int64(ver)
	expect(t, arr[1], strconv.Itoa(parallel_writes_count+1))

	// waiting for the previous write to expire
	time.Sleep(2 * time.Second)

	// trying to read a non-existent file
	text = "read input.txt\r\n"
	fmt.Fprintf(conn, text)
	scanner.Scan()                 // read first line
	resp = scanner.Text()          // extract the text from the buffer
	arr = strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "ERR_FILE_NOT_FOUND")

	// Leader shutdown and replication check
	var ldr *RaftNode
	for {
		ldr = getLeader(rafts)
		if ldr != nil {
			break
		}
	}
	ldr.ShutDown()

	time.Sleep(5 * time.Second)

	for {
		ldr = getLeader(rafts)
		if ldr != nil {
			break
		}
	}

	// after new leader election, old file can still be found
	fmt.Fprintf(conn, "read %v\r\n", "dummy.txt")
	scanner.Scan() // read first line
	// resp = scanner2.Text() // extract the text from the buffer
	// fmt.Println(resp, ldr.LeaderId(), ldr.Id())
	// arr = strings.Split(resp, " ") // split into OK and <version>
	// expect(t, arr[1], "1")
	// expect(t, arr[2], "5")
	// scanner.Scan()

	// after new leader election, non-existent file can still not be found
	fmt.Fprintf(conn, "read %v\r\n", name)
	scanner.Scan() // read first line
	// resp = scanner2.Text() // extract the text from the buffer
	// expect(t, resp, "ERR_FILE_NOT_FOUND")

}

func TestConcurrency(t *testing.T) {
	// // making concurrent writes
	// parallel_writes_count := 500
	//
	// wait_ch := make(chan int, parallel_writes_count)
	// wait_ch2 := make(chan int, parallel_writes_count)
	//
	// for i := 0; i < parallel_writes_count; i++ {
	// 	go conc_writes(t, &wait_ch, &wait_ch2, conn)
	// }
	// for i := 0; i < parallel_writes_count; i++ {
	// 	wait_ch <- 1
	// }
	// for i := 0; i < parallel_writes_count; i++ {
	// 	<-wait_ch2
	// }
	//
	// // checking version number increment(must be parallel_writes_count + 1)
	// contents = "byebye"
	// fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", "input.txt", len(contents), 1, contents)
	// scanner.Scan()                 // read first line
	// resp = scanner.Text()          // extract the text from the buffer
	// arr = strings.Split(resp, " ") // split into OK and <version>
	// expect(t, arr[0], "OK")
	// _, err = strconv.Atoi(arr[1]) // parse version as number
	// if err != nil {
	// 	t.Error("Non-numeric version found")
	// }
	// // version := int64(ver)
	// expect(t, arr[1], strconv.Itoa(parallel_writes_count+1))
	//
	// // waiting for the previous write to expire
	// time.Sleep(2 * time.Second)
	//
	// // trying to read a non-existent file
	// text = "read input.txt\r\n"
	// fmt.Fprintf(conn, text)
	// scanner.Scan()                 // read first line
	// resp = scanner.Text()          // extract the text from the buffer
	// arr = strings.Split(resp, " ") // split into OK and <version>
	// expect(t, arr[0], "ERR_FILE_NOT_FOUND")

}

func conc_writes(t *testing.T, wait_ch *chan int, wait_ch2 *chan int, conn net.Conn) {
	text := "write input.txt 4 3000\r\nThis\r\n"
	<-(*wait_ch)
	fmt.Fprintf(conn, text)
	scanner := bufio.NewScanner(conn)
	scanner.Scan()
	resp := scanner.Text()
	arr := strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "OK")
	_, err := strconv.Atoi(arr[1]) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}
	*wait_ch2 <- 1
}

// Useful testing function
func expect(t *testing.T, a string, b string) {
	if a != b {
		t.Error(fmt.Sprintf("Expected %v, found %v", b, a))
	}
}
