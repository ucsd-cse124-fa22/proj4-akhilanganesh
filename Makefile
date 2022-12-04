.PHONY: install
install:
	rm -rf bin
	GOBIN=$(PWD)/bin go install ./...

.PHONY: run-both
run-both:
	go run cmd/SurfstoreServerExec/main.go -d -s both -p 8081 -l localhost:8081

.PHONY: run-blockstore
run-blockstore:
	go run cmd/SurfstoreServerExec/main.go -d -s block -p 8081 -l

.PHONY: run-metastore
run-metastore:
	go run cmd/SurfstoreServerExec/main.go -d -s meta -l localhost:8081

.PHONY: run-blocklocator
run-blocklocator:
	go run cmd/SurfstoreBlockLocatorExec/main.go 1 4096 test/file2.dat
	go run cmd/SurfstoreBlockLocatorExec/main.go 1 25 test/file2.dat
	go run cmd/SurfstoreBlockLocatorExec/main.go 2 25 test/file2.dat
	go run cmd/SurfstoreBlockLocatorExec/main.go 3 10 test/file2.dat
	@echo go run cmd/SurfstoreBlockLocatorExec/main.go -downServers 3,4 10 4096 test/file1.dat
	@echo go run cmd/SurfstoreBlockLocatorExec/main.go -downServers 0,2 10 4096 test/file1.dat