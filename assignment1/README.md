This is an implementation of a simple file server written in Go. It can write contents to a file, read from the file and delete a file. syndtr's implementation of LevelDB(github.com/syndtr/goleveldb/leveldb) is used for persistency. 

Each file has a version associated with it. This enables the compare and swap operation to be performed on a file. If the version no. provided in a compare and swap command matched the version number of a file, then the contents are swapped with the the data provided with the command. 

The file server is consistent, durable and is suitable for multi-threaded applications. SInce the implementation of leveldb used didn't provide for concurrency, mutexes had to be used for inserting into the database. Each file can be associated with an expiry time after which the contents of the file becomes unavailable. 

The server is written in assignment1/fileserver.go and testcases in assignment1/fileserver_test.go. The test will take 15-20 seconds since sleeps have been added to test for expiry. 

The server can be tested using 
go build fileserver.go
./fileserver
telnet localhost 8080


/**************************************************

The API to use the server is mentioned below.

1. Write: create a file, or update the file’s contents if it already exists.

write (filename) (numbytes) [(exptime)]\r\n
(content bytes)\r\n

The server responds with the following:
OK (version)\r\n

where version is a unique 64‑bit number (in decimal format) assosciated with the
filename.

2. Read: Given a filename, retrieve the corresponding file:

read (filename)\r\n

The server responds with the following format (or one of the errors described later)

CONTENTS (version) (numbytes) (exptime) \r\n
(content bytes)\r\n

3. Compare and swap. This replaces the old file contents with the new content
provided the version is still the same.

cas (filename) (version) (numbytes) [(exptime)]\r\n
(content bytes)\r\n

The server responds with the new version if successful (or one of the errors
described later)

OK (version)\r\n

4. Delete file

delete (filename)\r\n

Server response (if successful)

OK\r\n

**************************************************/