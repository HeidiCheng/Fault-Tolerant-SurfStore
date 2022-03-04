package surfstore

import (
	context "context"
	"errors"
	"sync"
)

type BlockStore struct {
	BlockMap map[string]*Block
	lock     sync.Mutex
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	//panic("todo")
	// TODO deal with timeout
	bs.lock.Lock()
	defer bs.lock.Unlock()
	val, found := bs.BlockMap[blockHash.Hash]
	if found {
		return val, nil
	} else {
		return &Block{}, errors.New("cannot get target block")
	}

}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	//panic("todo")
	bs.lock.Lock()
	defer bs.lock.Unlock()
	hash := GetBlockHashString(block.BlockData)

	_, found := bs.BlockMap[hash]
	if found == false {
		bs.BlockMap[hash] = block
	}
	//bs.BlockMap[hash] = block

	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	//panic("todo") //when error will happen?
	var hasBlockes BlockHashes
	bs.lock.Lock()
	defer bs.lock.Unlock()
	for _, hash := range blockHashesIn.Hashes {
		if _, found := bs.BlockMap[hash]; found {
			hasBlockes.Hashes = append(hasBlockes.Hashes, hash)
		}
	}

	return &hasBlockes, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
