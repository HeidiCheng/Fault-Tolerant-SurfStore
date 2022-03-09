package surfstore

import (
	context "context"
	sync "sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	lock           sync.Mutex
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	//panic("todo") // timeout?
	m.lock.Lock()
	defer m.lock.Unlock()
	fileInfoMap := FileInfoMap{FileInfoMap: m.FileMetaMap}
	return &fileInfoMap, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	//panic("todo")
	m.lock.Lock()
	defer m.lock.Unlock()
	fm, found := m.FileMetaMap[fileMetaData.Filename]
	if found {
		if fileMetaData.Version == (fm.Version + 1) {
			m.FileMetaMap[fileMetaData.Filename].BlockHashList = fileMetaData.BlockHashList // not sure how to pass the value
			m.FileMetaMap[fileMetaData.Filename].Version = fileMetaData.Version
			return &Version{Version: fileMetaData.Version}, nil
		}
		return &Version{Version: -1}, nil
	}

	newMeta := FileMetaData{Filename: fileMetaData.Filename, BlockHashList: fileMetaData.BlockHashList, Version: fileMetaData.Version}
	m.FileMetaMap[fileMetaData.Filename] = &newMeta
	//m.FileMetaMap[fileMetaData.Filename] = fileMetaData

	return &Version{Version: fileMetaData.Version}, nil
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	//panic("todo") // timeout
	m.lock.Lock()
	defer m.lock.Unlock()
	blockStoreAddr := BlockStoreAddr{Addr: m.BlockStoreAddr}
	return &blockStoreAddr, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
