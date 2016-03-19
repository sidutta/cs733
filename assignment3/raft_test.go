package main

import (
	//"log"
	"strconv"
	"testing"
	"time"
)

func errorCheck(t *testing.T, expected string, response string, error string) {
	if expected != response {
		t.Fatal("Expected : " + expected + " Got : " + response + " Error : " + error)
	}
}

func TestRaft(t *testing.T) {

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

	// prevTerm := ldr.sm.CurrentTerm

	// //log.Println("Expected : ")
	prev_leader := ldr.sm.ServerID
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

/**************************************************************/
// Usage of mock cluster for testing

// Shutdown and restart of all nodes
// Check if Log, Term and VotedFor is stored, flushed and then initialised using previous data
// Append on top of initialised log
func TestShutDown(t *testing.T) {

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

//
// // Test Append on majority
// // Leader election on leader shutdown and leader term monotonicity
// // Overwriting log to match leader
func TestPartitions(t *testing.T) {

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
