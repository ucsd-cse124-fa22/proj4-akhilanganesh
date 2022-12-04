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
	go run cmd/SurfstoreBlockLocatorExec/main.go 10 128 test/file3.dat
	go run cmd/SurfstoreBlockLocatorExec/main.go -downServers 4 10 128 test/file3.dat
	go run cmd/SurfstoreBlockLocatorExec/main.go -downServers 1,2,3,4,5,6,7,8,9 10 128 test/file3.dat
	go run cmd/SurfstoreBlockLocatorExec/main.go -downServers 0,1,2,3,4,5,6,7,8 10 128 test/file3.dat