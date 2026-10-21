.PHONY: all test bench run lint clean

all: test

test:
	go test -v -race ./...

bench:
	go test -bench=. -benchmem ./...

run:
	go run ./cmd/demo/main.go

clean:
	go clean
	rm -f coverage.txt
