package main

import (
	// "fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

func errorCheck(t *testing.T, expected string, response string, error string) {
	if expected != response {
		t.Fatal("Expected : " + expected + " Got : " + response + " Error : " + error)
	}
}

func checkCommit(node *RaftNode, synchronizer *chan bool) {
	f, _ := os.Create("tmp" + strconv.Itoa(node.sm.ServerID))

	for {

		ci := <-node.CommitChannel()
		// delete(set, string(ci.Data))
		// if node.sm.ServerID != node.sm.LeaderID {
		// fmt.Println(node.sm.ServerID, node.sm.LeaderID, ci.Index)
		// }
		// set[string(ci.Data)] = false
		// num++
		f.Write([]byte(strconv.Itoa(int(node.sm.ServerID)) + " " + strconv.Itoa(node.sm.LeaderID) + " " + strconv.Itoa(int(ci.Index))))
		f.Sync()
		if int(ci.Index) == 499 {
			// fmt.Println("hurray")
			break
		}
	}
	// fmt.Println("hurray2")
	*synchronizer <- true
}

func request(t *testing.T, num int, ldr *RaftNode) {
	for sync2 == true {

	}
	for i := 0; i < NUM_MSGS; i++ {
		testStr := "testing" + strconv.Itoa(num) + "_" + strconv.Itoa(i)
		ldr.Append([]byte(testStr))
		// set[testStr] = true
	}

}

var NUM_CLIENTS int
var NUM_MSGS int
var sync2 bool

func TestConcurrency(t *testing.T) {
	NUM_CLIENTS = 50
	NUM_MSGS = 10
	synchronizer := make(chan bool, 5)
	// synchronizer2 := make(chan bool, 5)
	// synchronizer3 := make(chan bool, 5)
	// synchronizer4 := make(chan bool, 5)
	// synchronizer5 := make(chan bool, 5)
	setup()
	nodes, _ := makeMockRafts()

	for i := 0; i < len(configs.Peers); i++ {
		defer nodes[i].lg.Close()
		go nodes[i].startProcessing()
	}
	time.Sleep(1 * time.Second)

	ldr := getLeader(nodes)

	sync2 = true

	for i := 0; i < NUM_CLIENTS; i++ {
		go request(t, i, ldr)
		// for j := 0; j < NUM_MSGS; j++ {
		// 			testStr := "testing" + strconv.Itoa(i) + "_" + strconv.Itoa(j)
		// 			ldr.Append([]byte(testStr))
		// 			// set[testStr] = true
		// 		}
	}

	sync2 = false

	time.Sleep(5 * time.Second)

	// for j := 0; j < 1; j++ {
	// 		for i:= 0; i < 50; i++ {
	//testStr := "testing" + strconv.Itoa(i) + "_" + strconv.Itoa(j)
	status := make(map[string]bool)
	status["stat1"] = false
	status["stat2"] = false
	status["stat3"] = false
	status["stat4"] = false
	status["stat5"] = false
	// num := 0
	// 	for {
	// for _, node := range nodes {
	// 	// if status["stat"+strconv.Itoa(node.sm.ServerID)] == true {
	// 	// 				continue
	// 	// 			}
	// 	// 			ci := <-node.CommitChannel()
	// 	// 			// delete(set, string(ci.Data))
	// 	// 			fmt.Println(node.sm.ServerID, node.sm.LeaderID, ci.Index)
	// 	// 			// set[string(ci.Data)] = false
	// 	// 			// num++
	// 	//
	// 	// 			if ci.Index == 199 {
	// 	// 				num++
	// 	// 				status["stat"+strconv.Itoa(node.sm.ServerID)] = true
	// 	// 			}
	// 	go checkCommit(&node, &synchronizer)
	// 	fmt.Println(node.sm.ServerID)
	// }
	// if num == 5 {
	// 		break
	// 	}
	// }

	go checkCommit(&nodes[0], &synchronizer)
	// fmt.Println(nodes[0].sm.ServerID)
	go checkCommit(&nodes[1], &synchronizer)
	// fmt.Println(nodes[1].sm.ServerID)
	go checkCommit(&nodes[2], &synchronizer)
	// fmt.Println(nodes[2].sm.ServerID)
	go checkCommit(&nodes[3], &synchronizer)
	// fmt.Println(nodes[3].sm.ServerID)
	go checkCommit(&nodes[4], &synchronizer)
	// fmt.Println(nodes[4].sm.ServerID)

	<-synchronizer
	// fmt.Println("aww")
	<-synchronizer
	// fmt.Println("aww")
	<-synchronizer
	// fmt.Println("aww")
	<-synchronizer
	// fmt.Println("aww")
	<-synchronizer
	// fmt.Println("aww")

	// fmt.Println("aqw")

}

func TestWithoutMock(t *testing.T) {

	setup()
	// nodes, _ := makeMockRafts()
	nodes := makeRafts()

	// time.Sleep(50 * time.Second)
	for i := 0; i < len(configs.Peers); i++ {
		defer nodes[i].lg.Close()
		go nodes[i].startProcessing()
	}
	time.Sleep(1 * time.Second)

	ldr := getLeader(nodes)

	for i := 0; i < 5; i++ {
		testStr := "testing" + strconv.Itoa(i)
		ldr.Append([]byte(testStr))
		time.Sleep(1 * time.Second)
		for _, node := range nodes {
			select {

			case ci := <-node.CommitChannel():

				if ci.Err != nil {
					//log.Println(ci.Err)
				}
				errorCheck(t, testStr, string(ci.Data), "error in committing after successful append")

			}
		}
	}

	errorCheck(t, strconv.Itoa(4), strconv.Itoa(int(nodes[0].CommittedIndex())), "leaderid func returning wrong leader")

	// prevTerm := ldr.sm.CurrentTerm

	// //log.Println("Expected : ")
	prev_leader := ldr.Id()
	ldr.ShutDown()

	time.Sleep(2 * time.Second)
	for {
		ldr = getLeader(nodes)
		if ldr != nil {
			break
		}
	}

	for i := 0; i < 5; i++ {
		testStr := "testing" + strconv.Itoa(i)
		ldr.Append([]byte(testStr))
		time.Sleep(1 * time.Second)
		for _, node := range nodes {
			if node.sm.ServerID != prev_leader {
				select {

				case ci := <-node.CommitChannel():

					if ci.Err != nil {
						//log.Println(ci.Err)
					}
					errorCheck(t, testStr, string(ci.Data), "error in committing after successful append")

				}
			}
		}
	}
}

func TestAppendOnNonLeader(t *testing.T) {

	setup()
	// nodes, _ := makeMockRafts()
	nodes, _ := makeMockRafts()

	// time.Sleep(50 * time.Second)
	for i := 0; i < len(configs.Peers); i++ {
		defer nodes[i].lg.Close()
		go nodes[i].startProcessing()
	}
	time.Sleep(1 * time.Second)

	ldr := getLeader(nodes)

	testStr := "testing"

	for _, nd := range nodes {
		if nd.sm.ServerID != ldr.Id() {
			nd.Append([]byte(testStr))
			time.Sleep(1 * time.Second)
			for _, node := range nodes {
				select {

				case ci := <-node.CommitChannel():

					if ci.Err != nil {
						//log.Println(ci.Err)
					}
					errorCheck(t, testStr, string(ci.Data), "error in committing after successful append")

				}
			}
			break
		}
	}

}

func TestRecoveryAfterFailure(t *testing.T) {

	setup()

	nodes, _ := makeMockRafts()

	for i := 0; i < len(configs.Peers); i++ {
		defer nodes[i].lg.Close()
		go nodes[i].startProcessing()
	}

	time.Sleep(1 * time.Second)

	ldr := getLeader(nodes)

	testStr := "testing"
	ldr.Append([]byte(testStr))
	time.Sleep(1 * time.Second)
	for _, node := range nodes {
		select {

		case ci := <-node.CommitChannel():

			if ci.Err != nil {
				//log.Println(ci.Err)
			}
			errorCheck(t, testStr, string(ci.Data), "error in committing after successful append")

		}
	}

	for _, node := range nodes {
		node.ShutDown()
	}

	time.Sleep(1 * time.Second)

	nodes2, _ := makeMockRafts()

	for i := 0; i < len(configs.Peers); i++ {
		defer nodes2[i].lg.Close()
		go nodes2[i].startProcessing()
	}

	time.Sleep(1 * time.Second)

	ldr2 := getLeader(nodes2)
	errorCheck(t, strconv.Itoa(ldr2.sm.ServerID), strconv.Itoa(nodes2[0].LeaderId()), "leaderid func returning wrong leader")

	testStr = "testing2"
	ldr2.Append([]byte(testStr))
	time.Sleep(1 * time.Second)
	for _, node := range nodes2 {
		select {

		case ci := <-node.CommitChannel():

			if ci.Err != nil {
				//log.Println(ci.Err)
			}
			errorCheck(t, testStr, string(ci.Data), "error in committing after successful append")

		}
	}

	ci, _ := ldr2.Get(1)
	errorCheck(t, testStr, string(ci), "error in committing after successful append")

	for _, node := range nodes2 {
		node.ShutDown()
	}
}

func TestPartitioning(t *testing.T) {

	setup()

	nodes, cltr := makeMockRafts()

	for i := 0; i < len(configs.Peers); i++ {
		defer nodes[i].lg.Close()
		go nodes[i].startProcessing()
	}

	time.Sleep(1 * time.Second)

	ldr := getLeader(nodes)

	testStr := "testing"
	ldr.Append([]byte(testStr))
	time.Sleep(1 * time.Second)
	for _, node := range nodes {
		select {

		case ci := <-node.CommitChannel():

			if ci.Err != nil {
				//log.Println(ci.Err)
			}
			errorCheck(t, testStr, string(ci.Data), "error in committing after successful append")

		}
	}

	if ldr.sm.ServerID < 3 {
		cltr.Partition([]int{1, 2}, []int{3, 4, 5})
	} else if ldr.sm.ServerID > 3 {
		cltr.Partition([]int{1, 2, 3}, []int{4, 5})
	} else {
		cltr.Partition([]int{1, 2, 5}, []int{3, 4})
	}

	testStr = "testing3"
	ldr.Append([]byte(testStr))
	time.Sleep(2 * time.Second)

	for _, node := range nodes {
		select {
		case ci := <-node.CommitChannel():
			t.Fatal(ci.Err)
		default:
		}
	}

	cltr.Heal()

	time.Sleep(2 * time.Second)
	for {
		ldr = getLeader(nodes)
		if ldr != nil {
			break
		}
	}

	testStr = "testing4"
	ldr.Append([]byte(testStr))
	time.Sleep(1 * time.Second)
	for _, node := range nodes {
		select {

		case ci := <-node.CommitChannel():

			if ci.Err != nil {
				//log.Println(ci.Err)
			}
			errorCheck(t, testStr, string(ci.Data), "error in committing after successful append")

		}
	}

	for _, node := range nodes {
		node.ShutDown()
	}

}
