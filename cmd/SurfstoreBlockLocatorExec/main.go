package main

import (
	"cse224/proj4/pkg/surfstore"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

func main() {

	downServers := flag.String("downServers", "", "Comma-separated list of server IDs that have failed")
	flag.Parse()

	if flag.NArg() != 3 {
		fmt.Printf("Usage: %s numServers blockSize inpFilename\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	numServers, err := strconv.Atoi(flag.Arg(0))
	if err != nil {
		log.Fatal("Invalid number of servers argument: ", flag.Arg(0))
	}

	blockSize, err := strconv.Atoi(flag.Arg(1))
	if err != nil {
		log.Fatal("Invalid block size argument: ", flag.Arg(0))
	}

	inpFilename := flag.Arg(2)

	log.Println("Total number of blockStore servers: ", numServers)
	log.Println("Block size: ", blockSize)
	log.Println("Processing input data filename: ", inpFilename)

	if *downServers != "" {
		for _, downServer := range strings.Split(*downServers, ",") {
			log.Println("Server ", downServer, " is in a failed state")
		}
	} else {
		log.Println("No servers are in a failed state")
	}

	// This is an example of the format of the output
	// Your program will emit pairs for each block hash where the
	// first part of the pair is the block hash, and the second
	// element is the server number that the block resides on
	//
	// This output is simply to show the format, the actual mapping
	// isn't based on consistent hashing necessarily
	// fmt.Println("{{672e9bff6a0bc59669954be7b2c2726a74163455ca18664cc350030bc7eca71e, 7}, {31f28d5a995dcdb7c5358fcfa8b9c93f2b8e421fb4a268ca5dc01ca4619dfe5f,2}, {172baa036a7e9f8321cb23a1144787ba1a0727b40cb6283dbb5cba20b84efe50,1}, {745378a914d7bcdc26d3229f98fc2c6887e7d882f42d8491530dfaf4effef827,5}, {912b9d7afecb114fdaefecfa24572d052dde4e1ad2360920ebfe55ebf2e1818e,0}}")

	// get downed servers as a list of ints
	var downedServers []int
	if *downServers != "" {
		downedServersStrings := strings.Split(*downServers, ",")
		downedServers = make([]int, len(downedServersStrings))
		for i, str := range downedServersStrings {
			var err error
			downedServers[i], err = strconv.Atoi(str)
			if err != nil {
				log.Println(err)
			}
		}
	} else {
		downedServers = []int(nil)
	}

	// consistent hash ring
	hashRing := surfstore.NewConsistentHashRing(numServers, downedServers)

	// parse input file to blocks
	blocks, err := surfstore.ParseFileIntoBlocks(inpFilename, blockSize)
	if err != nil {
		log.Println(err)
	}

	// hash blocks
	var blockHashes []string = []string(nil)
	for _, block := range blocks {
		blockHashes = append(blockHashes, surfstore.GetBlockHashString(block.BlockData))
	}

	// get mappings from block hashes
	mappings := hashRing.OutputMap(blockHashes)

	// print mappings
	PrintMappings(&mappings)
}

func PrintMappings(mappings *map[string]string) {
	fmt.Printf("{")
	var i int = 0
	for key, val := range *mappings {
		fmt.Printf("{%s,%s}", key, strings.ReplaceAll(val, "blockstore", ""))
		i++
		if i != len(*mappings) {
			fmt.Printf(", ")
		}
	}
	fmt.Printf("}\n")
}
