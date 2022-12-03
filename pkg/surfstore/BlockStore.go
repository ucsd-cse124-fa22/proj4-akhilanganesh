package surfstore

import (
	context "context"
	"fmt"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	hash = blockHash.GetHash()
	block, exists = bs.BlockMap[hash]
	if exists {
		return block, nil//&Block{BlockData: block.BlockData, BlockSize: block.BlockSize}, nil
	}
	return nil, fmt.Errorf("No matching block found for %s", hash)
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	hash = blockHash.GetHash()
	_, exists = bs.BlockMap[hash]
	if !exists {
		BlockMap[hash] = block
		return &Success{Flag: true}, nil
	}
	return &Success{Flag: false}, fmt.Errorf("Already matching block found for %s", hash)
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	subsetHashes string[]
	for key := range bs.BlockMap {
		for hash := range blockHashesIn.GetHashes() {
			if key == hash {
				append(subsetHashes, key)
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
