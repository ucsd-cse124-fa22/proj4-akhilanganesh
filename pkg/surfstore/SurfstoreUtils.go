package surfstore

import (
	"path/filepath"
	"io"
	"log"
	"os"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	var i int // indexing variable


	// get list of local files in base directory
	log.Println("Syncing")
	path, err := filepath.Abs(client.BaseDir)
	baseDirFile, err := os.Open(path)
	if err != nil {
		log.Fatal("Base directory non-existent")
	}
	files, err := baseDirFile.Readdir(0)
	if err != nil {
		log.Printf("%v\n", err)
	}



	// gets all working directory file blocks and metadata
	blocks := make(map[string]*Block)
	metas := make(map[string]*FileMetaData)
	for _, info := range files {
		if !info.IsDir() && info.Name() != DEFAULT_META_FILENAME {
			log.Printf("Parsing %s\n", info.Name())
			filePath := ConcatPath(client.BaseDir, info.Name())
			fileBlocks, err := ParseFileIntoBlocks(filePath, client.BlockSize)
			if err != nil {
				log.Printf("%v\n", err)
				continue
			}
			for _, block := range fileBlocks {
				blocks[GetBlockHashString(block.BlockData)] = block
			}
			metas[info.Name()] = ParseFileMetaData(info.Name(), fileBlocks)
			if err != nil {
				log.Printf("Blocks not properly retrieved for %s\n", info.Name())
			}
		}
	}

	// local file set
	metas_keys := make([]string, len(metas))
	i = 0
	for key, _ := range metas {
		metas_keys[i] = key
		i++
	}
	log.Printf("Parsed local files: %v\n", metas_keys)



	// get metadata from local index
	index, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Printf("%v\n", err)
	}



	// find all file differences between index and local
	diffs := make(map[string]*FileMetaData) // proposed update set
	for key, val := range index {
		md, exists := metas[key]
		// deleted file on index
		if len(val.BlockHashList) == 1 && val.BlockHashList[0] == "0" {
			
			// file still on local
			if exists {
				diffs[key] = metas[key]
				diffs[key].Version = index[key].Version + 1
			}
		
		// file on index but not on local
		} else if !exists {
			diffs[key] = index[key]
			diffs[key].Version = index[key].Version + 1
			diffs[key].BlockHashList = []string{"0"}
		
		// file on both index and local
		} else {
			
			// different file sizes (in terms of blocks)
			if len(md.BlockHashList) != len(val.BlockHashList) {
				diffs[key] = metas[key]
				diffs[key].Version = index[key].Version + 1
			
			// check for differences in block hashes at each position
			} else {
				for i, hash := range md.BlockHashList {
					if hash != val.BlockHashList[i] {
						diffs[key] = metas[key]
						diffs[key].Version = index[key].Version + 1
					}
				}
			}
		}
	}
	// files on local not on index
	for key, _ := range metas {
		_, exists := index[key]
		if !exists {
			diffs[key] = metas[key]
			diffs[key].Version = 1
		}
	}

	// server update set
	diffs_keys := make([]string, len(diffs))
	i = 0
	for key, _ := range diffs {
		diffs_keys[i] = key
		i++
	}
	log.Printf("Server update set created: %v\n", diffs_keys)



	// push update set metadata to server
	var blockStoreAddr string
	err = client.GetBlockStoreAddr(&blockStoreAddr)
	if err != nil {
		log.Printf("%v\n", err)
	}
	for key, val := range diffs {
		var version int32
		err = client.UpdateFile(val, &version)
		if err != nil {
			log.Printf("%v\n", err)
		}
		if version == -1 {
			log.Printf("Update for %s failed\n", key)
			delete(diffs, key)
		}
	}
	log.Println("Pushed all updates")

	// push update set blocks to server
	for _, val := range diffs {
		for _, hash := range val.BlockHashList {
			if hash == "0" {
				continue
			}
			block, exists := blocks[hash]
			if !exists {
				log.Printf("Non-existent block with hash: %.10s", hash)
			}
			var success bool
			err = client.PutBlock(block, blockStoreAddr, &success)
			if err != nil {
				log.Printf("%v\n", err)
			}
		}
	}
	log.Println("Pushed all blocks")
	


	// get all new metadata and blocks
	var serverMD map[string]*FileMetaData
	err = client.GetFileInfoMap(&serverMD)
	if err != nil {
		log.Printf("%v\n", err)
	}



	// find new content from server metadata
	newContent := make(map[string]*FileMetaData)
	newBlockHashes := make([]string, 0)
	for key, val := range serverMD {
		md, exists := metas[key]

		// deleted file on server
		if len(val.BlockHashList) == 1 && val.BlockHashList[0] == "0" {
			
			// file still on local
			if exists {
				err = os.Remove(ConcatPath(client.BaseDir, val.Filename))
			}

		// file not on local but on server
		} else if !exists {
			newContent[key] = val
			for _, hash := range val.BlockHashList {
				newBlockHashes = append(newBlockHashes, hash)
			}
		
		// file on both local and server
		} else {
			var diff bool = false
			for i, hash := range val.BlockHashList {

				// different block between local and server versions
				if i >= len(md.BlockHashList) || hash != md.BlockHashList[i] {
					diff = true
					newBlockHashes = append(newBlockHashes, hash)
				}
			}

			// file has differences between local and server
			if diff || len(md.BlockHashList) > len(val.BlockHashList) {
				newContent[key] = val
			}
		}
	}

	// local update set
	newCont_keys := make([]string, len(newContent))
	i = 0
	for key, _ := range newContent {
		newCont_keys[i] = key
		i++
	}
	log.Printf("Local update set created: %v\n", newCont_keys)
	log.Printf("Local update block hashes: %v", newBlockHashes)

	// request new blocks
	err = client.HasBlocks(newBlockHashes, blockStoreAddr, &newBlockHashes)
	if err != nil {
		log.Printf("%v\n", err)
	}
	
	for _, hash := range newBlockHashes {
		var newBlock Block
		err = client.GetBlock(hash, blockStoreAddr, &newBlock)
		if err != nil {
			log.Printf("%v\n", err)
		}
		blocks[hash] = &newBlock
	}
	log.Println("Requisite blocks retrieved")



	// update local files (local reconstitution)
	for _, val := range newContent {
		fd, err := os.Create(ConcatPath(client.BaseDir, val.Filename))
		defer fd.Close()
		if err != nil {
			log.Printf("%v\n", err)
			continue
		}
		
		for _, hash := range val.BlockHashList {
			_, err := fd.Write(blocks[hash].BlockData)
			if err != nil {
				log.Printf("%v\n", err)
				log.Println("File corrupted")
			}
		}
	}
	log.Println("Updated local files")



	// update index.txt
	err = WriteMetaFile(serverMD, client.BaseDir)
	if err != nil {
		log.Printf("%v\n", err)
	}
	log.Println("All synched")

	// done
}

func ParseFileMetaData(fileName string, fileBlocks []*Block) *FileMetaData {
	log.Println("Parsing file metadata")
	var hashList []string
	for _, block := range fileBlocks {
		hashList = append(hashList, GetBlockHashString(block.BlockData))
	}
	return &FileMetaData{Filename: fileName, Version: -1 /* local */, BlockHashList: hashList}
}

func ParseFileIntoBlocks(fileName string, blockSize int) ([]*Block, error) {
	var blocks []*Block = make([]*Block, 0)
	
	fd, err := os.Open(fileName)
	if err != nil {
		return blocks, err
	}
	defer fd.Close()

	log.Printf("Parsing file %s into blocks", fileName)
	var buf []byte = make([]byte, blockSize)
	var block_data []byte = make([]byte, blockSize)
	var bytes_in_block int = 0
	for {
		num_bytes, err := fd.Read(buf)

		// if a block is filled, store the block and build the next one
		if bytes_in_block + num_bytes >= blockSize {
			copy(block_data[bytes_in_block:], buf[:blockSize - bytes_in_block])
			blocks = append(blocks, &Block{BlockData: append([]byte(nil), block_data...), BlockSize: int32(blockSize)})
			bytes_in_block = 0
			num_bytes -= blockSize - bytes_in_block
		}

		// fill block a bit by buffer amount
		copy(block_data[bytes_in_block:bytes_in_block + num_bytes], buf[:num_bytes])
		bytes_in_block += num_bytes

		// if EOF encountered
		if err == io.EOF {
			if bytes_in_block > 0 {
				blocks = append(blocks, &Block{BlockData: append([]byte(nil), block_data[:bytes_in_block]...), BlockSize: int32(bytes_in_block)})
			}
			break
		} else if err != nil { // error occurred, safely exit with error message
			return blocks, err
		}
	}

	return blocks, nil
}