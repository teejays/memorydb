GOCMD=go
GOBUILD=$(GOCMD) build
BINARY_NAME=memorydb.out
all: build
clean:
	rm $(BINARY_NAME)
build: fmt
	$(GOBUILD) -o $(BINARY_NAME)
fmt:
	$(GOCMD) fmt
run: build
	./$(BINARY_NAME)
test:
	$(GOCMD) test -v