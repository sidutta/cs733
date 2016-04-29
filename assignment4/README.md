# Distributed File Server

A distributed fault-tolerant file server.

### Installation

```
go get github.com/sidutta/cs733/assign4
go test github.com/sidutta/cs733/assign4
go build 
```

### Commands
Client can issue the following commands :

##### 1. read
Given a filename, retrieve the corresponding file from the server. Return metadata( version number, num_bytes, expiry time) along with the content of file. If the file is not present on the server, "ERR_FILE_NOT_FOUND" error will be received.

Syntax:
```
read <file_name>\r\n
```

Response (if successful):

``` CONTENTS <version> <num_bytes> <exptime>\r\n ```<br />
``` <content_bytes>\r\n ```


##### 2. write

Create a file, or update the file’s contents if it already exists. Files are given new version numbers when updated.

Syntax:
```
write <file_name> <num_bytes> [<exp_time>]\r\n
<content bytes>\r\n
```

Response:

``` OK <version>\r\n ```

##### 3. compare-and-swap (cas)

This replaces the old file contents with the new content provided the version of file is still same as that in the command. Returns OK and new version of the file if successful, else error in cas and the new version, if unsuccessful.

Syntax:
```
cas <file_name> <version> <num_bytes> [<exp_time>]\r\n
<content bytes>\r\n
```

Response(If successful): 

``` OK <version>\r\n ```

##### 4. delete

Delete a file. Returns FILE_NOT_FOUND error if file is not present.

Syntax:
```
delete <file_name>\r\n
```

Response(If successful):


``` OK\r\n ```


### ERRORS
You may come across the following errors:
(Here is the meaning and the scenario in which they can occur)

``` ERR_CMD_ERR ```:                          Incorrect client command

``` ERR_FILE_NOT_FOUND ```:                   File requested is not found (Either expired or not created) 

``` ERR_VERSION <latest-version> ```:         Error in cas (Version mismatch)



### Test cases and notes

A test file is written to test the server. Following scenarios are tested :

1. When server starts, leader is elected. Read, delete, write and cas operations can be performed. 

2. Updates to leader are replicated across majority nodes, only then is a confirmation sent to the client.

3. From assignment1 tests like splitting of packets, extra long sequences of bytes, sending binaries, sequence of commands sent together was performed. 

4. Redirection is not exposed to the user(happens internally).

5. File is not exposed after its expiry.

6. If leader dies and new leader gets elected, committed results must persist.

