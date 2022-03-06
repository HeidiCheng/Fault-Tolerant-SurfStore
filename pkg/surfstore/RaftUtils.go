package surfstore

import (
	"bufio"

	//	"google.golang.org/grpc"
	"io"
	"log"

	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	grpc "google.golang.org/grpc"
)

func LoadRaftConfigFile(filename string) (ipList []string) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	serverCount := 0

	for index := 0; ; index++ {
		lineContent, _, e := configReader.ReadLine()
		if e != nil && e != io.EOF {
			log.Fatal("Error During Reading Config", e)
		}

		if e == io.EOF {
			return ipList
		}

		lineString := string(lineContent)
		splitRes := strings.Split(lineString, ": ")
		if index == 0 {
			serverCount, _ = strconv.Atoi(splitRes[1])
			ipList = make([]string, serverCount, serverCount)
		} else {
			ipList[index-1] = splitRes[1]
		}
	}

	return ipList
}

func NewRaftServer(id int64, ips []string, blockStoreAddr string) (*RaftSurfstore, error) {
	// TODO any initialization you need to do here

	isCrashedMutex := &sync.RWMutex{}
	isLeaderMutex := &sync.RWMutex{}
	logMutex := &sync.RWMutex{}
	termMutex := &sync.RWMutex{}

	server := RaftSurfstore{
		// TODO initialize any fields you add here
		isLeader:            false,
		term:                0,
		metaStore:           NewMetaStore(blockStoreAddr),
		log:                 make([]*UpdateOperation, 0),
		ip:                  ips[id],
		ipList:              ips,
		serverId:            id,
		lastApplied:         -1,
		lastLeaderCommitted: 0,
		commitIndex:         -1,
		pendingCommits:      make([]chan bool, 0),
		isCrashed:           false,
		notCrashedCond:      sync.NewCond(isCrashedMutex),
		isCrashedMutex:      isCrashedMutex,
		isLeaderMutex:       isLeaderMutex,
		logMutex:            logMutex,
		nextIndex:           0,
		matchIndex:          -1,
		termMutex:           termMutex,
		//PrevLogIndex:   -1,
		//PrevLogTerm:    0,
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	//panic("todo")
	s := grpc.NewServer()
	RegisterRaftSurfstoreServer(s, server)

	l, err := net.Listen("tcp", server.ip)
	if err != nil {
		return err
	}
	return s.Serve(l)
}
