# MemoryDB

MemoryDb implement an in-memory database that can be accessed using the Standard Output and Standard Input. The package implements the following methods: GET, SET, DELETE, BEGIN, ROLLBACK, COMMIT, END.

## Getting Started
Setting this API up should be fairly easy if you have Go set up. 

### Prerequisites:
1) Install GoLang from the official [Go website](https://golang.org/).
2) Install the following Go package(s):
	* [clog](https://github.com/teejays/clog): ``` go get github.com/teejays/clog```

### Installation:
1) Clone this repository: 
	```git clone https://github.com/teejays/memorydb.git```

2) Go to the project folder
`make run`

### Testing:
Go to the project folder and run:
`make test`