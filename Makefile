.PHONY: all build test fmt clean run

TARGET_NAME=qed

all: test build

build:
	go build -o bin/$(TARGET_NAME) -v examples/hello/main.go

test:
	go test -race -count=1 ./...

fmt:
	go fmt ./...

example: build
	go run examples/hello/main.go

clean:
	go clean
	@rm -r bin

run: build
	./bin/$(TARGET_NAME)
