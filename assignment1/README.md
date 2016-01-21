This is an implementation of a simple file server written in Go. It can write contents to a file, read from the file and delete a file. syndtr's implementation of LevelDB(github.com/syndtr/goleveldb/leveldb) is used for persistency. 

Each file has a version associated with it. This enables the compare and swap operation to be performed on a file. If the version no. provided in a compare and swap command matched the version number of a file, then the contents are swapped with the the data provided with the command. 

The file server is consistent, durable and is suitable for multi-threaded applications.
