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
	go run cmd/SurfstoreBlockLocatorExec/main.go -downServers 3,4 10 4096 test/template/2blocks.txt
