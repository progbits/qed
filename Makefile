.PHONY: all build test fmt clean run

TARGET_NAME=qed

all: test build
build:
	go build -o bin/$(TARGET_NAME) -v ./main.go
test:
	echo "Write some tests!" && exit 0
fmt:
	go fmt ./...
clean:
	go clean
	@rm -r bin
run: build
	./bin/$(TARGET_NAME)
