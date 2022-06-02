.PHONY: all build test fmt clean run

TARGET_NAME=qed

all: test build

build:
	go build -o bin/$(TARGET_NAME) -v examples/hello/main.go

test:
	go test -count=1 ./...

fmt:
	go fmt ./...

clean:
	go clean
	@rm -r bin

run: build
	./bin/$(TARGET_NAME)
