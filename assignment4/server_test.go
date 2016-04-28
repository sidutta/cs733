package main

import "net"
import "fmt"
import "bufio"
import "sync"
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
	// version := int64(ver)
	expect(t, arr[1], "1")

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

	fmt.Fprintf(conn, "read %v\r\n", name)
	scanner.Scan()                 // read first line
	resp = scanner.Text()          // extract the text from the buffer
	arr = strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[1], "2")
	expect(t, arr[2], "6")
	scanner.Scan()        // read first line
	resp = scanner.Text() // extract the text from the buffer
	expect(t, resp, "byebye")

	// now delete should work
	fmt.Fprintf(conn, text)
	scanner.Scan()                 // read first line
	resp = scanner.Text()          // extract the text from the buffer
	arr = strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "OK")

	// /**************************************************************/
	// // Test version increment
	// text = "delete input.txt\r\n"
	// fmt.Fprintf(conn, text)
	//
	// message, _ := bufio.NewReader(conn).ReadString('\n')

	text = "write input.txt 15\r\nThis is a file.\r\n"
	fmt.Fprintf(conn, text)

	// connect to this socket
	conn2, _ := net.Dial("tcp", "localhost:8080")
	scanner2 := bufio.NewScanner(conn2)

	text2 := "write input.txt 8\r\nThis is.\r\n"
	fmt.Fprintf(conn2, text2)

	scanner.Scan()        // read first line
	resp = scanner.Text() // extract the text from the buffer
	expect(t, resp, "OK 1")

	fmt.Print("Message from server: " + resp)
	scanner2.Scan()          // read first line
	resp2 := scanner2.Text() // extract the text from the buffer
	expect(t, resp2, "OK 2")
	fmt.Print("Message from server: " + resp2)
	// message2, _ := bufio.NewReader(conn2).ReadString('\n')
	// fmt.Print("Message from server: " + message2)
	//
	// text = "cas input.txt 1 2\r\n%#\r\n"
	// fmt.Fprintf(conn, text)
	//
	// message, _ = bufio.NewReader(conn).ReadString('\n')
	//
	// if message1 == message2 || len(message) < 2 || (message[:2] != "OK" && (len(message) < 11 || message[:11] != "ERR_VERSION")) {
	// 	t.Error("Error in version concurrent write")
	// }
	//
	// text = "delete input.txt\r\n"
	// fmt.Fprintf(conn, text)
	// message, _ = bufio.NewReader(conn).ReadString('\n')
	// //fmt.Println("Done Version Check")
	//
	// /**************************************************************/
	// // Leader shutdown and replication check
	// var ldr *RaftNode
	// for {
	// 	ldr = getLeader(rafts)
	// 	if ldr != nil {
	// 		break
	// 	}
	// }
	// ldr.ShutDown()
	// //rafts[(ldr.Id() + 1)%5].ShutDown()
	//
	// time.Sleep(1 * time.Second)
	//
	// //fmt.Println("Started Leader ReElection")
	// for {
	// 	ldr = getLeader(rafts)
	// 	if ldr != nil {
	// 		break
	// 	}
	// }
	// //fmt.Println("Done Leader ReElection")
	//
	// fmt.Fprintf(conn, "read %v\r\n", name) // try a read now
	// scanner.Scan()
	//
	// arr = strings.Split(scanner.Text(), " ")
	// expect(t, arr[0], "CONTENTS")
	// expect(t, arr[1], fmt.Sprintf("%v", version)) // expect only accepts strings, convert int version to string
	// expect(t, arr[2], fmt.Sprintf("%v", len(contents)))
	// scanner.Scan()
	// expect(t, contents, scanner.Text())
	//
	// text = "delete hi.txt\r\n"
	// fmt.Fprintf(conn, text)
	// message, _ = bufio.NewReader(conn).ReadString('\n')
	//
	// /**************************************************************/
	// // Test concurrent write to the same file by multiple clients
	// text = "delete input2.txt\r\n"
	// fmt.Fprintf(conn, text)
	// message, _ = bufio.NewReader(conn).ReadString('\n')
	// //fmt.Println("Kill ok")
	//
	// i := 0
	// var wg sync.WaitGroup
	// wg.Add(10)
	// for i < 10 {
	// 	go clients(&wg, t)
	// 	i++
	// }
	//
	// wg.Wait()
	// //fmt.Println("Done Write")
	//
	// text = "read input2.txt\r\n"
	// fmt.Fprintf(conn, text)
	//
	// message, _ = bufio.NewReader(conn).ReadString('\n')
	// //fmt.Print("Message from server: "+message)
	//
	// if len(message) < 8 || message[:8] != "CONTENTS" {
	// 	t.Error("Error in concurrent write")
	// }
	//
	// /*
	// 	if len(message) < 8 || message[:8] != "CONTENTS" {
	// 		message, _ = bufio.NewReader(conn).ReadString('\n')
	// 		//fmt.Print("Message from server: "+message)
	//
	// 		if len(message) < 8 || message[:8] != "CONTENTS" {
	// 			t.Error("Error in concurrent write")
	// 		}
	// 	}
	// */
	//
	// text = "delete input2.txt\r\n"
	// fmt.Fprintf(conn, text)
	// message, _ = bufio.NewReader(conn).ReadString('\n')
	//
}

// Client which performs read and write in parellel with itself
func clients(wg *sync.WaitGroup, t *testing.T) {
	conn, _ := net.Dial("tcp", "localhost:8080")
	defer conn.Close()

	//checkError(err)
	reader := bufio.NewReader(conn)
	_, _ = conn.Write([]byte("write input2.txt 5\r\n"))
	_, _ = conn.Write([]byte("!#bin\r\n"))
	line, _ := reader.ReadBytes('\n')

	var str_temp string = string(line)
	if len(str_temp) < 2 || str_temp[:2] != "OK" {
		t.Error(str_temp)
	}

	wg.Done()
	//fmt.Println("Done single Write")
}

// Useful testing function
func expect(t *testing.T, a string, b string) {
	if a != b {
		t.Error(fmt.Sprintf("Expected %v, found %v", b, a)) // t.Error is visible when running `go test -verbose`
	}
}
