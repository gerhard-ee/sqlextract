.PHONY: all build test clean lint coverage help

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
BINARY_NAME=sqlextract
BINARY_UNIX=$(BINARY_NAME)_unix

all: test build

build: 
	$(GOBUILD) -o $(BINARY_NAME) -v ./cmd/sqlextract

test: 
	$(GOTEST) -v -race ./...

clean: 
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
	rm -f $(BINARY_UNIX)

run:
	$(GOBUILD) -o $(BINARY_NAME) -v ./cmd/sqlextract
	./$(BINARY_NAME)

lint:
	golangci-lint run

coverage:
	$(GOTEST) -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out

deps:
	$(GOMOD) download
	$(GOMOD) verify
	$(GOMOD) tidy

# Cross compilation
build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD) -o $(BINARY_UNIX) -v ./cmd/sqlextract

docker-build:
	docker build -t $(BINARY_NAME):latest .

help:
	@echo "make - Run tests and build binary"
	@echo "make build - Build binary"
	@echo "make test - Run tests"
	@echo "make clean - Clean build files"
	@echo "make run - Build and run locally"
	@echo "make lint - Run linter"
	@echo "make coverage - Generate test coverage"
	@echo "make deps - Download and verify dependencies"
	@echo "make build-linux - Cross compile for Linux"
 