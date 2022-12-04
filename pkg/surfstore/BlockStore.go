package surfstore

import (
	context "context"
	"fmt"
	"log"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	hash := blockHash.GetHash()
	log.Printf("Request for block with hash %.10s\n", hash)
	block, exists := bs.BlockMap[hash]
	if exists {
		return block, nil
	}
	return nil, fmt.Errorf("No matching block found for %.10s", hash)
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	hash := GetBlockHashString(block.BlockData)
	log.Printf("Request to put block with hash %.10s\n", hash)
	_, exists := bs.BlockMap[hash]
	if !exists {
		bs.BlockMap[hash] = block
		return &Success{Flag: true}, nil
	}
	return &Success{Flag: false}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	log.Printf("Request to find subset of blocks stored here")
	var subsetHashes []string
	for key, _ := range bs.BlockMap {
		for _, hash := range blockHashesIn.GetHashes() {
			if key == hash {
				subsetHashes = append(subsetHashes, key)
			}
		}
	}
	return &BlockHashes{Hashes: subsetHashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
