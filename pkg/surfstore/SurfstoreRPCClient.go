package surfstore

import (
	context "context"
	"errors"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	//panic("todo")
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	*succ = s.Flag

	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {

	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	bout, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	blockHashesOut = &bout.Hashes

	if err != nil {
		conn.Close()
		return err
	}

	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {

	// need to be changed later -> MetaStoreAddr[0] change to leader
	for _, addr := range surfClient.MetaStoreAddrs {

		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		//c := NewMetaStoreClient(conn)
		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		m, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
		if err != nil {
			conn.Close()
			continue
		}

		*serverFileInfoMap = m.FileInfoMap
		// close the connection
		return conn.Close()
	}

	// not sure how do we give exit 1 error
	return errors.New("GetFileInfoMap: Majority of servers are crashed")
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	//panic("todo")
	for _, addr := range surfClient.MetaStoreAddrs {

		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		//c := NewMetaStoreClient(conn)
		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		ver, err := c.UpdateFile(ctx, fileMetaData)
		if err != nil {
			conn.Close()
			continue
		}

		*latestVersion = ver.Version

		return conn.Close()
	}
	return errors.New("UpdateFile: Most of the servers are crashed.")
}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	//panic("todo")

	// connect to the server
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		//c := NewMetaStoreClient(conn)
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		addr, err := c.GetBlockStoreAddr(ctx, &emptypb.Empty{})
		if err != nil {
			conn.Close()
			continue
		}
		*blockStoreAddr = addr.Addr

		// close the connection
		return conn.Close()
	}
	return errors.New("GetBlockStoreAddr: Most of the servers are crashed.")
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
