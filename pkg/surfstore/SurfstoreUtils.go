package surfstore

import (
	"filepath"
	"io"
	"os"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	baseDirFile, err := os.Open(filepath.Abs(client.BaseDir))
	if err != nil {
		log.Println("Base directory non-existent, now created")
		os.MkDir(filepath.Abs(client.BaseDir), 0750)
		os.Exit(0)
	}

	indexExists bool
	files, err := baseDirFile.ReAddir(0)
	if err != nil {
		log.Fatal("ReAddir failed")
	}

	// gets all working directory file blocks and metadata
	blocks map[string]*Block
	metas map[string]*FileMetaData
	for _, info := range files {
		if !info.IsDir() {
			fileBlocks, err := ParseFileIntoBlocks(info.Name(), client.BlockSize)
			for _, block := range fileBlocks {
				blocks[GetBlockHashString(block.BlockData)] = block
			}
			metas[info.Name()] = ParseFileMetaData(info.Name(), fileBlocks)
			if err != nil {
				log.Printf("Blocks not properly retrieved for %s", info.Name())
			}
		}
	}

	// get metadata from local index
	index map[string]*FileMetaData = LoadMetaFromMetaFile(client.BaseDir)

	// find all file differences
	diffs map[string]*FileMetaData // proposed update set
	for key, val := range index {
		md, exists := metas[key]
		if !exists {
			diffs[key] = index[key]
			diffs[key].Version = index[key].Version + 1
			diffs[key].HashBlockList = ["0"]
		} else {
			if len(md.BlockHashList) != len(val.BlockHashList) {
				diffs[key] = metas[key]
				diffs[key].Version = index[key].Version + 1
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
	for key, val := range metas {
		_, exists := index[key]
		if !exists {
			diffs[key] = metas[key]
			diffs[key].Version = 1
		}
	}

	// try to push update set
	blockStoreAddr string
	err := client.GetBlockStoreAddr(&blockStoreAddr)
	for key, val := range diffs {
		version int
		err = client.UpdateFile(val, &version)
		if version == -1 {
			delete(diffs, key)
		}
	}
	for key, val := range diffs {
		for hash := range val.HashBlockList {
			success bool
			err = client.PutBlock(blocks[hash], blockStoreAddr, &success)
		}
	}
	
	// get all new metadata and blocks
	serverMD map[string]*FileMetaData
	err = client.GetFileInfoMap(&serverMD)

	// find new content from server metadata
	newContent map[string]*FileMetaData
	newBlockHashes []string
	for key, val := range serverMD {
		md, exists := metas[key]
		if !exists ||  {
			newContent[key] = val
			for _, hash := range val.HashBlockList {
				append(newBlockHashes, hash)
			}
		} else {
			diff bool = false
			for i, hash := range md.BlockHashList {
				if hash != val.BlockHashList[i] {
					diff = true
					append(newBlockHashes, hash)
				}
			}
			if diff || len(md.BlockHashList) != len(val.BlockHashList) {
				newContent[key] = val
			}
		}
	}

	// request new blocks
	err = client.HasBlocks(newBlockHashes, blockStoreAddr, &newBlockHashes)
	for _, hash := range newBlockHashes {
		newBlock Block
		err = client.GetBlock(hash, blockStoreAddr, &newBlock)
		blocks[hash] = &newBlock
	}

	// update local files
	for _, val := range newContent {
		fd, err := os.Create(val.FileName)
		defer fd.Close()
		if err != nil {
			continue
		}
		for _, hash := range val.HashBlockList {
			n_bytes, err := fd.Write(blocks[hash].BlockData)
			if err != nil {
				log.Println("File corrupted")
			}
		}
	}

	// update index.txt
	err = WriteMetaFile(serverMD, client.BaseDir)
	if err != nil {
		log.Println(err)
	}

	// done
}

func ParseFileMetaData(fileName string, fileBlocks []*Block) *FileMetaData {
	hashList string[]
	for _, block := range fileBlocks {
		append(hashList, GetBlockHashString(block.BlockData))
	}
	return &FileMetaData{Filename: fileName, Version: -1 /* local */, BlockHashList: hashList}
}

func ParseFileIntoBlocks(fileName string, blockSize int) ([]*Block, error) {
	blocks []Block
	
	fd, err := os.Open(filepath.Abs(ConcatPath(baseDir, fileName)))
	if err != nil {
		return blocks, err
	}
	defer fd.Close()

	buf []byte
	for {
		n_bytes, err := fd.Read(buf)
		for (n_bytes >= blockSize) {
			append(blocks, &Block{BlockData: buf[:blockSize], BlockSize: blockSize})
			buf = buf[blockSize:]
			n_bytes -= blockSize
		}

		if err == io.EOF {
			if n_bytes > 0 {
				append(blocks, &Block{BlockData: buf, BlockSize: n_bytes})
			}
			break
		} else if err != nil {
			return blocks, err
		}
	}

	return blocks, nil
}
