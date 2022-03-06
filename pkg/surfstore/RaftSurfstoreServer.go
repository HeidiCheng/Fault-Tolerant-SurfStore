package surfstore

import (
	context "context"
	"math"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RaftSurfstore struct {
	// TODO add any fields you need
	isLeader bool
	term     int64
	log      []*UpdateOperation

	metaStore *MetaStore

	// Server Info
	ip       string
	ipList   []string
	serverId int64

	// Previous log info
	nextIndex  int64
	matchIndex int64
	//PrevLogIndex int64
	//PrevLogTerm  int64

	// State machine
	lastApplied int64

	// Commit
	commitIndex    int64
	pendingCommits []chan bool

	// Locks
	isLeaderMutex *sync.RWMutex
	logMutex      *sync.RWMutex
	termMutex     *sync.RWMutex

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	//panic("todo")

	s.isLeaderMutex.RLock()
	if s.isLeader == false {
		defer s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	s.isCrashedMutex.RLock()
	if s.isCrashed == true {
		s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()

	// Is it possible that we call GetBlockStoreAddr in a crashed leader?
	aliveServers := 1

	aliveChan := make(chan bool, len(s.ipList))
	for idx, _ := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}
		go s.CheckAliveness(int64(idx), aliveChan)
	}

	for {
		alive := <-aliveChan

		if alive == true {
			aliveServers++
		}
		if aliveServers > len(s.ipList)/2 {
			return &FileInfoMap{FileInfoMap: s.metaStore.FileMetaMap}, nil
		}
	}

	return nil, nil
}

func (s *RaftSurfstore) CheckAliveness(serverIdx int64, alive chan bool) {

	addr := s.ipList[serverIdx]
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	defer conn.Close()
	if err != nil {
		return
	}
	client := NewRaftSurfstoreClient(conn)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// get lock before isCrashed?
	state, _ := client.IsCrashed(ctx, &emptypb.Empty{})
	alive <- (!state.IsCrashed)
	return
}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	//panic("todo")
	s.isLeaderMutex.RLock()
	if s.isLeader == false {
		defer s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	s.isCrashedMutex.RLock()
	if s.isCrashed == true {
		defer s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()

	// Is it possible that we call GetBlockStoreAddr in a crashed server?
	aliveServers := 0
	s.isCrashedMutex.RLock()
	if s.isCrashed == false {
		aliveServers++
	}
	s.isCrashedMutex.RUnlock()

	aliveChan := make(chan bool, len(s.ipList))
	for idx, _ := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}
		go s.CheckAliveness(int64(idx), aliveChan)
	}

	for {
		alive := <-aliveChan
		if alive == true {
			aliveServers++
		}
		if aliveServers > len(s.ipList)/2 {
			return &BlockStoreAddr{Addr: s.metaStore.BlockStoreAddr}, nil
		}
	}
	return nil, nil
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	//panic("todo")
	//fmt.Println("Updating file")

	s.isLeaderMutex.RLock()
	if s.isLeader == false {
		defer s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	s.isCrashedMutex.RLock()
	if s.isCrashed == true {
		defer s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()

	op := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}

	s.logMutex.Lock()
	s.log = append(s.log, &op)
	s.logMutex.Unlock()
	s.nextIndex++
	//fmt.Println(s.log)

	committed := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, committed) // deal with multiple sync

	go s.AttemptCommit()

	//s.PrevLogIndex++
	//s.PrevLogTerm = s.term

	success := <-committed
	if success {
		_, _ = s.SendHeartbeat(ctx, &emptypb.Empty{})
		return s.metaStore.UpdateFile(ctx, filemeta)
	}

	return nil, nil
}

func (s *RaftSurfstore) AttemptCommit() {

	targetIndex := s.commitIndex + 1
	appendChan := make(chan *AppendEntryOutput, len(s.ipList))

	for idx, _ := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}
		go s.AppendEntriesToFollowers(int64(idx), targetIndex, appendChan)
	}

	appendCount := 1

	// TODO: handle leader change to followers
	for {
		appended := <-appendChan
		// leader change to follower
		if appended.Term > s.term {
			break
		}
		if appended != nil && appended.Success {
			appendCount++
		}
		if appendCount > len(s.ipList)/2 {
			s.pendingCommits[targetIndex] <- true
			s.commitIndex = targetIndex
			//break
		}
		if appendCount == len(s.ipList) {
			break
		}
	}
}

func (s *RaftSurfstore) AppendEntriesToFollowers(serverIndex, entryIndex int64, appendChan chan *AppendEntryOutput) {

	for {
		s.isLeaderMutex.Lock()
		if s.isLeader == false {
			defer s.isLeaderMutex.Unlock()
			break
		}
		s.isLeaderMutex.Unlock()

		addr := s.ipList[serverIndex]
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		defer conn.Close()
		// deal with connection failure
		if err != nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		client := NewRaftSurfstoreClient(conn)

		input := &AppendEntryInput{Term: s.term, PrevLogTerm: 0, LeaderCommit: s.commitIndex}

		if entryIndex == -1 {
			input.PrevLogIndex = int64(len(s.log) - 1)
			input.Entries = make([]*UpdateOperation, 0)
		} else {
			input.PrevLogIndex = entryIndex - 1
			input.Entries = []*UpdateOperation{s.log[entryIndex]}
		}

		if input.PrevLogIndex > -1 {
			input.PrevLogTerm = s.log[input.PrevLogIndex].Term
		}

		output, err := client.AppendEntries(ctx, input)
		// server crashed -> try to reconnect the server
		if err != nil {
			continue
		}
		// success
		if output.Success == true {
			appendChan <- output
			return
		}
		// leader change to follower
		if output.Term > s.term {
			appendChan <- output
			return
		}

		// previous log conflict
		if output.MatchedIndex == -1 {
			input.Entries = s.log
			input.PrevLogIndex = -1
			input.PrevLogTerm = 0
		} else {
			input.Entries = append(s.log[output.MatchedIndex:input.PrevLogIndex+1], input.Entries...)
			input.PrevLogIndex = output.MatchedIndex
			input.PrevLogTerm = s.log[input.PrevLogIndex].Term
		}
	}
}

//1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
//matches prevLogTerm (§5.3)
//3. If an existing entry conflicts with a new one (same index but different
//terms), delete the existing entry and all that follow it (§5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
//of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	//panic("todo")
	// If the server is crashed -> return error
	s.isCrashedMutex.RLock()
	if s.isCrashed == true {
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()

	output := AppendEntryOutput{
		ServerId:     s.serverId,
		Success:      false,
		MatchedIndex: -1,
		Term:         input.Term,
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if input.Term < s.term {
		output.Term = s.term
		return &output, nil
	}

	if input.Term > s.term {
		s.isLeaderMutex.Lock()
		s.isLeader = false // seems no need to do it here since if it was the leader, it will get error when sending the heartbeat
		s.isLeaderMutex.Unlock()
		s.term = input.Term
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
	// matches prevLogTerm (§5.3)
	if (int64(len(s.log)-1) < input.PrevLogIndex) || (len(s.log) > 0 && s.log[input.PrevLogIndex].Term != input.PrevLogTerm) {
		s.matchIndex = int64(math.Min(float64(s.matchIndex), float64(input.PrevLogIndex-1)))
		output.MatchedIndex = s.matchIndex
		return &output, nil
	}

	// 3. If an existing entry conflicts with a new one (same index but different
	// terms), delete the existing entry and all that follow it (§5.3)
	if int64(len(s.log)-1) > input.PrevLogIndex && s.log[input.PrevLogIndex+1].Term != input.Term {
		s.logMutex.Lock()
		s.log = s.log[:input.PrevLogIndex+1]
		s.logMutex.Unlock()
	}

	// 4. Append any new entries not already in the log
	s.logMutex.Lock()
	s.log = append(s.log, input.Entries...)
	s.logMutex.Unlock()
	s.nextIndex = int64(len(s.log))
	s.matchIndex = int64(len(s.log) - 1)
	//fmt.Println(s.log)
	//s.PrevLogIndex = int64(len(s.log) - 1)
	//s.PrevLogTerm = s.log[s.PrevLogIndex].Term

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
	// of last new entry)
	s.commitIndex = int64(math.Min(float64(len(s.log)-1), float64(input.LeaderCommit)))

	// Commit to entries to this server
	for s.lastApplied < s.commitIndex {
		s.lastApplied++
		entry := s.log[s.lastApplied]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
	}

	output.Success = true
	output.MatchedIndex = s.commitIndex

	//fmt.Println(s.log)
	return &output, nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// panic("todo")
	//fmt.Println("Setting leader")
	s.isCrashedMutex.RLock()
	defer s.isCrashedMutex.RUnlock()
	if s.isCrashed == true {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	// TODO: further check about whether the server can be set to be a leader
	s.isLeaderMutex.Lock()
	s.isLeader = true
	s.isLeaderMutex.Unlock()
	s.term++

	return &Success{Flag: true}, nil
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// panic("todo")
	//fmt.Println("Senging heartbeat")
	s.isCrashedMutex.RLock()
	if s.isCrashed == true {
		defer s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()

	s.isLeaderMutex.RLock()
	if s.isLeader == false {
		defer s.isLeaderMutex.RUnlock()
		return &Success{Flag: false}, nil
	}
	s.isLeaderMutex.RUnlock()

	// for idx, add := range s.ipList {

	// }

	appendChan := make(chan *AppendEntryOutput, len(s.ipList))
	for idx, _ := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}
		go s.AppendEntriesToFollowers(int64(idx), -1, appendChan)
	}

	appendCount := 1

	// TODO: handle leader change to followers
	for {
		output := <-appendChan

		if output != nil && output.Term > s.term {
			s.term = output.Term
			s.isLeaderMutex.Lock()
			s.isLeader = false
			s.isLeaderMutex.Unlock()
			return &Success{Flag: false}, nil
		}

		if output != nil && output.Success {
			appendCount++
		}
		if appendCount > len(s.ipList)/2 {
			break
		}
	}

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.notCrashedCond.Broadcast()
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) IsCrashed(ctx context.Context, _ *emptypb.Empty) (*CrashedState, error) {
	s.isCrashedMutex.RLock() // added by Heidi, not sure whether we can change code here
	defer s.isCrashedMutex.RUnlock()
	return &CrashedState{IsCrashed: s.isCrashed}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	return &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
