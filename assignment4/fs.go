package main

import (
	"encoding/json"
	"github.com/syndtr/goleveldb/leveldb"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

type FileService struct {
	Id         int64
	datadb     *leveldb.DB
	metadatadb *leveldb.DB
	mutex      *sync.RWMutex
	input      <-chan CommitInfo
	routes     []chan Response
}

func NewFS(Id int, routes []chan Response, input <-chan CommitInfo) *FileService {
	var f *FileService = &FileService{}
	f.Id = int64(Id)
	f.routes = routes
	f.input = input
	f.datadb, _ = leveldb.OpenFile("datadb"+strconv.Itoa(Id), nil)
	f.metadatadb, _ = leveldb.OpenFile("metadatadb"+strconv.Itoa(Id), nil)
	f.mutex = &sync.RWMutex{}
	return f
}

func (f *FileService) Listen() {
	defer f.datadb.Close()
	defer f.metadatadb.Close()

	for {
		ci := <-f.input
		var n Msg
		if ci.Err != nil {
			break
		}
		json.Unmarshal(ci.Data, &n)
		f.ProcessMsg(n, ci.Leader)
		//fmt.Println("Heard ", f.Id, " ", n.Kind, ";", n.Filename, ":")
	}
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

func (f *FileService) ProcessMsg(msg Msg, toSend bool) {
	switch msg.Kind {
	case 0:

		f.mutex.RLock()
		version, numbytes, exptime, exp, err1 := read_metadata(msg.Filename, f.metadatadb)

		if err1 == nil {
			if time.Now().After(exptime) {
				f.mutex.RUnlock()
				// conn.Write([]byte("ERR_FILE_NOT_FOUND\r\n")) // content has expired
				if toSend {
					f.routes[msg.Id] <- Response{"ERR_FILE_NOT_FOUND\r\n", true, nil}
				}
			} else {
				data, err2 := f.datadb.Get([]byte(msg.Filename), nil)
				f.mutex.RUnlock()
				if err2 != nil {
					log.Println("error in conversion: ", err1)
				}
				version_str := strconv.Itoa(version)
				numbytes_str := strconv.Itoa(numbytes)

				exp_str := "0"
				if exp != 0 {
					exp_str = strconv.Itoa(int(exptime.Sub(time.Now()).Seconds()))
				}
				// response := append([]byte("CONTENTS "+version_str+" "+numbytes_str+" "+exp_str+" \r\n"), data...)
				// log.Println(response)
				// conn.Write(response)
				// log.Println(response)
				if toSend {
					f.routes[msg.Id] <- Response{"CONTENTS " + version_str + " " + numbytes_str + " " + exp_str + " \r\n", false, data}
				}
			}
		} else {
			f.mutex.RUnlock()
			if toSend {
				f.routes[msg.Id] <- Response{"ERR_FILE_NOT_FOUND\r\n", true, nil}
			}
		}
		return

	case 1:

		f.mutex.Lock()
		_, _, _, _, err := read_metadata(msg.Filename, f.metadatadb)

		if err != nil {
			f.mutex.Unlock()
			if toSend {
				f.routes[msg.Id] <- Response{"ERR_FILE_NOT_FOUND\r\n", true, nil}
			}
		} else {

			f.datadb.Delete([]byte(msg.Filename), nil)
			f.metadatadb.Delete([]byte(msg.Filename), nil)
			f.mutex.Unlock()
			// conn.Write(append([]byte("OK\r\n")))

			if toSend {
				f.routes[msg.Id] <- Response{"OK\r\n", false, nil}
			}

		}

		return

	case 2:

		f.mutex.Lock()
		prev_version_int, _, _, _, err := read_metadata(msg.Filename, f.metadatadb)
		new_version := ""

		if err == nil {
			if err != nil {
				log.Println("error in conversion: ", err)
			}
			saved_metadata := strconv.Itoa(prev_version_int+1) + " " + msg.NumBytes + " " + msg.Exptime + " " + msg.Exp
			err = f.metadatadb.Put([]byte(msg.Filename), []byte(saved_metadata), nil)
			if err != nil {
				log.Println("failed to add to database: ", err)
			}
			new_version = strconv.Itoa(prev_version_int + 1)
		} else {
			saved_metadata := strconv.Itoa(1) + " " + msg.NumBytes + " " + msg.Exptime + " " + msg.Exp
			err = f.metadatadb.Put([]byte(msg.Filename), []byte(saved_metadata), nil)
			if err != nil {
				log.Println("failed to add to database: ", err)
			}
			new_version = "1"
		}
		err = f.datadb.Put([]byte(msg.Filename), msg.Contents, nil)
		f.mutex.Unlock()
		if err != nil {
			log.Println("failed to add to database: ", err)
		}
		// response := "OK " + string(new_version) + "\r\n"
		// log.Println(response)
		// some_int, err := conn.Write([]byte(response))

		if toSend {
			f.routes[msg.Id] <- Response{"OK " + string(new_version) + "\r\n", false, nil}
		}

		// if err != nil {
		// 			log.Println("failed to reply back: ", err, some_int)
		// 		}

		return

	case 3:
		f.mutex.Lock()
		prev_version_int, _, _, _, err := read_metadata(msg.Filename, f.metadatadb)

		if err != nil {
			f.mutex.Unlock()
			f.routes[msg.Id] <- Response{"ERR_FILE_NOT_FOUND\r\n", true, nil}
		} else {

			req_version_int, _ := strconv.Atoi(msg.Version)

			if err != nil {
				log.Println("error in conversion: ", err)
			}

			if prev_version_int != req_version_int {
				prev_version_str := strconv.Itoa(prev_version_int)
				f.routes[msg.Id] <- Response{"ERR_VERSION " + prev_version_str + "\r\n", true, nil}
				f.mutex.Unlock()
			} else {

				new_version := ""

				saved_metadata := strconv.Itoa(prev_version_int+1) + " " + msg.NumBytes + " " + msg.Exptime + " " + msg.Exp
				err = f.metadatadb.Put([]byte(msg.Filename), []byte(saved_metadata), nil)

				if err != nil {
					log.Println("failed to add to database: ", err)
				}
				new_version = strconv.Itoa(prev_version_int + 1)

				err = f.datadb.Put([]byte(msg.Filename), msg.Contents, nil)
				f.mutex.Unlock()
				if err != nil {
					log.Println("failed to add to database: ", err)
				}

				f.routes[msg.Id] <- Response{"OK " + string(new_version) + "\r\n", true, nil}

			}
		}

		return
	}
}
