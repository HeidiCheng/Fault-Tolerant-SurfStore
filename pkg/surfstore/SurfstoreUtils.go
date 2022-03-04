package surfstore

import (
	"errors"
	"io"
	"log"
	"os"
	"reflect"
)

func FileToHashList(filePath string, blockSize int) []string {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Error when reading file")
	}
	defer file.Close()

	hashList := make([]string, 0)

	for {
		block := make([]byte, blockSize)
		s, err := file.Read(block)
		if err != nil {
			if err != io.EOF {
				log.Fatal(err)
			}
			break
		}
		hashList = append(hashList, GetBlockHashString(block[:s]))
	}

	return hashList
}

func ReconstructFile(client RPCClient, fileMetaData *FileMetaData) error {
	if reflect.DeepEqual(fileMetaData.BlockHashList, []string{"0"}) == true {
		return nil
	}

	f, err := os.Create(ConcatPath(client.BaseDir, fileMetaData.Filename))
	if err != nil {
		return err
	}

	var blockStoreAddr string
	err = client.GetBlockStoreAddr(&blockStoreAddr)
	if err != nil {
		return err
	}

	for _, hash := range fileMetaData.BlockHashList {
		var block Block
		err := client.GetBlock(hash, blockStoreAddr, &block)
		if err != nil {
			return err
		}
		_, err = f.Write(block.BlockData)
		if err != nil {
			return err
		}
	}
	return nil
}

func UploadFile(client RPCClient, filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		//log.Fatal("Error when reading file")
		return err
	}
	defer file.Close()

	blockSize := client.BlockSize

	for {
		block := make([]byte, blockSize)
		s, err := file.Read(block)
		if err != nil {
			if err != io.EOF {
				//log.Fatal(err)
				return err
			}
			break
		}

		var blockStoreAddr string
		err = client.GetBlockStoreAddr(&blockStoreAddr)
		if err != nil {
			//log.Fatal(err)
			return err
		}

		var succ bool
		err = client.PutBlock(&Block{BlockData: block[:s], BlockSize: int32(client.BlockSize)}, blockStoreAddr, &succ)
		if err != nil {
			return err
		}
		if succ == false {
			return errors.New("upload file failed")
		}

	}

	return nil
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	//panic("todo")
	indexPath := ConcatPath(client.BaseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(indexPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			_, ferr := os.Create(indexPath)
			if ferr != nil {
				log.Fatal("Error when creating index.txt")
			}
		} else {
			log.Fatal("Error when reading index.txt")
		}
	}
	baseDir, err := os.Open(client.BaseDir)
	if err != nil {
		log.Fatal("Error when opening base directory")
	}

	files, err := baseDir.Readdir(0)
	if err != nil {
		log.Fatal("Error when reading base directory")
	}

	currFileInfoMap := make(map[string]*FileMetaData)
	indexFileInfoMap, err := LoadMetaFromMetaFile(client.BaseDir)

	tombstone := []string{"0"}

	// step 1: scan the base directory & compute the hash list for each file
	for _, f := range files {
		if f.Name() == DEFAULT_META_FILENAME {
			continue
		}
		hash := FileToHashList(ConcatPath(client.BaseDir, f.Name()), client.BlockSize)
		originalMD, found := indexFileInfoMap[f.Name()]

		currFileInfoMap[f.Name()] = &FileMetaData{Filename: f.Name(), BlockHashList: hash}
		if found {
			// file changed, version = v + 1; not changed, version = 0
			if reflect.DeepEqual(hash, originalMD.BlockHashList) == false {
				currFileInfoMap[f.Name()].Version = originalMD.Version + 1
			} else {
				currFileInfoMap[f.Name()].Version = 0
			}
		} else {
			// newly added file, version = 1
			currFileInfoMap[f.Name()].Version = 1
		}
	}
	baseDir.Close()
	//log.Println(currFileInfoMap)

	// step 2: Connect to the server & download remote index
	serverFileInfoMap := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&serverFileInfoMap)
	if err != nil {
		log.Fatal(err)
	}

	// step 3: Compare the local index with the remote index
	// version is higher on the server -> what if it does not? it is not possible

	for name, serverMD := range serverFileInfoMap {
		_, indexFound := indexFileInfoMap[name]
		_, baseFound := currFileInfoMap[name]
		// on server but not on local
		if indexFound == false && baseFound == false {
			err := ReconstructFile(client, serverMD)
			if err != nil {
				log.Fatal(err)
			}
			indexFileInfoMap[name] = &FileMetaData{Filename: serverMD.Filename, Version: serverMD.Version, BlockHashList: serverMD.BlockHashList}
			currFileInfoMap[name] = &FileMetaData{Filename: serverMD.Filename, Version: serverMD.Version, BlockHashList: serverMD.BlockHashList}
		}
	}

	for name, fileMD := range currFileInfoMap {
		// removed file information just added by server
		if reflect.DeepEqual(fileMD.BlockHashList, tombstone) == true {
			continue
		}
		_, serverFound := serverFileInfoMap[name]

		// newly added on local base directory
		if serverFound == false {
			err = UploadFile(client, ConcatPath(client.BaseDir, name))
			if err != nil {
				log.Fatal(err)
			}
			var latestVersion int32
			err := client.UpdateFile(fileMD, &latestVersion)
			if err != nil {
				log.Fatal(err)
			}
			if latestVersion == -1 {
				err = os.Remove(ConcatPath(client.BaseDir, name))
				if err != nil {
					log.Fatal(err)
				}
				err = client.GetFileInfoMap(&serverFileInfoMap)
				if err != nil {
					log.Fatal(err)
				}
				err = ReconstructFile(client, serverFileInfoMap[name])
				indexFileInfoMap[name] = serverFileInfoMap[name]
			} else {
				indexFileInfoMap[name] = &FileMetaData{Filename: fileMD.Filename, Version: latestVersion, BlockHashList: fileMD.BlockHashList}
			}
		} else {
			// file's version on server should be larger or equal to 1 => download file  on server
			if fileMD.Version == 1 {
				err = os.Remove(ConcatPath(client.BaseDir, name))
				if err != nil {
					log.Fatal(err)
				}
				err := client.GetFileInfoMap(&serverFileInfoMap)
				if err != nil {
					log.Fatal(err)
				}
				err = ReconstructFile(client, serverFileInfoMap[name])
				if err != nil {
					log.Fatal(err)
				}
				indexFileInfoMap[name] = serverFileInfoMap[name]
			}
			// file modified
			if fileMD.Version > 1 {
				var latestVersion int32
				err := UploadFile(client, ConcatPath(client.BaseDir, name))
				if err != nil {
					log.Fatal(err)
				}
				err = client.UpdateFile(fileMD, &latestVersion)
				if err != nil {
					log.Fatal(err)
				}
				// -1 -> upload failed; otherwise -> upload success
				if latestVersion == -1 {
					err = os.Remove(ConcatPath(client.BaseDir, name))
					if err != nil {
						log.Fatal(err)
					}
					err = client.GetFileInfoMap(&serverFileInfoMap)
					if err != nil {
						log.Fatal(err)
					}
					err = ReconstructFile(client, serverFileInfoMap[name])
					if err != nil {
						log.Fatal(err)
					}
					indexFileInfoMap[name] = serverFileInfoMap[name]
				} else {
					indexFileInfoMap[name] = &FileMetaData{Filename: fileMD.Filename, Version: latestVersion, BlockHashList: fileMD.BlockHashList}
				}
			}
		}
	}
	//log.Println(serverFileInfoMap)
	//log.Println(indexFileInfoMap)

	for name, fileMD := range indexFileInfoMap {

		baseMD, baseFound := currFileInfoMap[name]
		serverMD, serverFound := serverFileInfoMap[name]
		// base directory deleted file -> need to be reconstructed when version conflict
		if baseFound == false && serverFound {
			if reflect.DeepEqual(serverMD.BlockHashList, tombstone) && reflect.DeepEqual(fileMD.BlockHashList, tombstone) {
				continue
			}
			var latest int32
			fileMD.BlockHashList = tombstone
			fileMD.Version = fileMD.Version + 1
			err := client.UpdateFile(fileMD, &latest)
			if latest == -1 {
				err = client.GetFileInfoMap(&serverFileInfoMap)
				if err != nil {
					log.Fatal(err)
				}
				err = ReconstructFile(client, serverFileInfoMap[name])
				if err != nil {
					log.Fatal(err)
				}
				fileMD.Version = serverFileInfoMap[name].Version
				fileMD.BlockHashList = serverFileInfoMap[name].BlockHashList
			}
		}
		if baseFound && baseMD.Version == 0 && fileMD.Version < serverMD.Version {
			if reflect.DeepEqual(serverMD.BlockHashList, tombstone) == true {
				err := os.Remove(ConcatPath(client.BaseDir, name))
				if err != nil {
					log.Fatal("cannot remove file")
				}
				indexFileInfoMap[name] = &FileMetaData{Filename: serverMD.Filename, Version: serverMD.Version, BlockHashList: serverMD.BlockHashList}
			} else {
				err = client.GetFileInfoMap(&serverFileInfoMap)
				if err != nil {
					log.Fatal(err)
				}
				err = ReconstructFile(client, serverFileInfoMap[name])
				if err != nil {
					log.Fatal(err)
				}
				fileMD.Version = serverFileInfoMap[name].Version
				fileMD.BlockHashList = serverFileInfoMap[name].BlockHashList
			}
		}
	}
	err = WriteMetaFile(indexFileInfoMap, client.BaseDir)
	if err != nil {
		log.Fatal(err)
	}

}
