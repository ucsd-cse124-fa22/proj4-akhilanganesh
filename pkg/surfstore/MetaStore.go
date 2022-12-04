package surfstore

import (
	context "context"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"log"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	log.Printf("Request for metas map")//: %v\n", m.FileMetaMap)
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	log.Printf("Request for file update: %s\n", fileMetaData.Filename)
	// get server's file metadata
	sFileMD, sFileExists := m.FileMetaMap[fileMetaData.Filename]

	// if valid file update
	if !sFileExists || fileMetaData.Version == 1 + sFileMD.Version {
		// update file metadata
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
		log.Printf("New file update: %s, version %d\n", fileMetaData.Filename, fileMetaData.Version)
		return &Version{Version: fileMetaData.Version}, nil
	} else {
		log.Printf("File update request denied\n")
		return &Version{Version: -1}, nil
	}
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	log.Printf("Request for block store address")
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
