package SurfTest

import (
	context "context"
	"cse224/proj5/pkg/surfstore"
	"fmt"
	"testing"
	"time"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

func TestRaftSetLeader(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state.Term != int64(1) {
			t.Logf("Server %d should be in term %d", idx, 1)
			t.Fail()
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Logf("Server %d should be the leader", idx)
				t.Fail()
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Logf("Server %d should not be the leader", idx)
				t.Fail()
			}
		}
	}

	leaderIdx = 2
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state.Term != int64(2) {
			t.Logf("Server should be in term %d", 2)
			t.Fail()
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Logf("Server %d should be the leader", idx)
				t.Fail()
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Logf("Server %d should not be the leader", idx)
				t.Fail()
			}
		}
	}
}

func TestRaftFollowersGetUpdates(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}

	test.Clients[leaderIdx].UpdateFile(context.Background(), filemeta1)

	goldenMeta := surfstore.NewMetaStore("")
	goldenMeta.UpdateFile(test.Context, filemeta1)
	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta1,
	})

	for _, server := range test.Clients {
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if !SameLog(goldenLog, state.Log) {
			t.Log("Logs do not match")
			t.Fail()
		}
		if !SameMeta(goldenMeta.FileMetaMap, state.MetaMap.FileInfoMap) {
			t.Log("MetaStore state is not correct")
			t.Fail()
		}
	}
}

func TestRaftRecovery(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}

	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	go test.Clients[leaderIdx].UpdateFile(context.Background(), filemeta1)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})

	time.Sleep(2 * time.Second)
	goldenMeta := surfstore.NewMetaStore("")
	goldenMeta.UpdateFile(test.Context, filemeta1)
	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta1,
	})

	for _, server := range test.Clients {
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if !SameLog(goldenLog, state.Log) {
			t.Log("Logs do not match")
			t.Fail()
		}
		if !SameMeta(goldenMeta.FileMetaMap, state.MetaMap.FileInfoMap) {
			t.Log("MetaStore state is not correct")
			t.Fail()
		}
	}
}

func TestRaftBlockWhenMajorityDown(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}

	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	go test.Clients[leaderIdx].UpdateFile(context.Background(), filemeta1)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	time.Sleep(2 * time.Second)
	goldenMeta := surfstore.NewMetaStore("")
	goldenMeta.UpdateFile(test.Context, filemeta1)
	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta1,
	})

	for _, server := range test.Clients {
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if !SameLog(goldenLog, state.Log) {
			t.Log("Logs do not match")
			t.Fail()
		}
		if !SameMeta(goldenMeta.FileMetaMap, state.MetaMap.FileInfoMap) {
			t.Log("MetaStore state is not correct")
			t.Fail()
		}
	}
}

func TestRaftUpdateTwice(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}
	filemeta2 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       2,
		BlockHashList: nil,
	}

	test.Clients[leaderIdx].UpdateFile(context.Background(), filemeta1)
	test.Clients[leaderIdx].SendHeartbeat(context.Background(), &emptypb.Empty{})
	test.Clients[leaderIdx].UpdateFile(context.Background(), filemeta2)
	test.Clients[leaderIdx].SendHeartbeat(context.Background(), &emptypb.Empty{})

	goldenMeta := surfstore.NewMetaStore("")
	goldenMeta.UpdateFile(test.Context, filemeta1)
	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta1,
	})
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta2,
	})
	fmt.Println(goldenLog)

	for _, server := range test.Clients {
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if !SameLog(goldenLog, state.Log) {
			t.Log("Logs do not match")
			t.Fail()
		}
		if !SameMeta(goldenMeta.FileMetaMap, state.MetaMap.FileInfoMap) {
			t.Log("MetaStore state is not correct")
			t.Fail()
		}
	}
}

func TestRaftLogsConsistent(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}
	filemeta2 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       2,
		BlockHashList: nil,
	}

	test.Clients[2].Crash(context.Background(), &emptypb.Empty{})
	test.Clients[leaderIdx].UpdateFile(context.Background(), filemeta1)
	test.Clients[leaderIdx].SendHeartbeat(context.Background(), &emptypb.Empty{})

	test.Clients[leaderIdx].Crash(context.Background(), &emptypb.Empty{})
	test.Clients[2].Restore(context.Background(), &emptypb.Empty{})

	leaderIdx = 1
	test.Clients[leaderIdx].SetLeader(context.Background(), &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(context.Background(), &emptypb.Empty{})

	test.Clients[leaderIdx].UpdateFile(context.Background(), filemeta2)
	test.Clients[leaderIdx].SendHeartbeat(context.Background(), &emptypb.Empty{})

	test.Clients[0].Restore(context.Background(), &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(context.Background(), &emptypb.Empty{})

	goldenMeta := surfstore.NewMetaStore("")
	goldenMeta.UpdateFile(test.Context, filemeta1)
	goldenMeta.UpdateFile(test.Context, filemeta2)
	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta1,
	})
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         2,
		FileMetaData: filemeta2,
	})
	fmt.Println(goldenLog)
	fmt.Println(goldenMeta.FileMetaMap)

	for _, server := range test.Clients {
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if !SameLog(goldenLog, state.Log) {
			fmt.Println(state.Log)
			t.Log("Logs do not match")
			t.Fail()
		}
		if !SameMeta(goldenMeta.FileMetaMap, state.MetaMap.FileInfoMap) {
			fmt.Println(state.MetaMap.FileInfoMap)
			t.Log("MetaStore state is not correct")
			t.Fail()
		}
	}
}

func TestRaftNewLeaderPushesUpdates(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}
	// filemeta2 := &surfstore.FileMetaData{
	// 	Filename:      "testFile1",
	// 	Version:       2,
	// 	BlockHashList: nil,
	// }

	test.Clients[2].Crash(context.Background(), &emptypb.Empty{})
	test.Clients[1].Crash(context.Background(), &emptypb.Empty{})

	go test.Clients[leaderIdx].UpdateFile(context.Background(), filemeta1)
	test.Clients[leaderIdx].SendHeartbeat(context.Background(), &emptypb.Empty{})

	test.Clients[leaderIdx].Crash(context.Background(), &emptypb.Empty{})

	test.Clients[1].Restore(context.Background(), &emptypb.Empty{})
	test.Clients[2].Restore(context.Background(), &emptypb.Empty{})

	leaderIdx = 1
	test.Clients[leaderIdx].SetLeader(context.Background(), &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(context.Background(), &emptypb.Empty{})

	goldenMeta := surfstore.NewMetaStore("")
	//goldenMeta.UpdateFile(test.Context, filemeta1)
	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta1,
	})
	// goldenLog = append(goldenLog, &surfstore.UpdateOperation{
	// 	Term:         1,
	// 	FileMetaData: filemeta2,
	// })
	// fmt.Println(goldenLog)

	for _, server := range test.Clients {
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if !SameLog(goldenLog, state.Log) {
			fmt.Println("state log: ", state.Log)
			t.Log("Logs do not match")
			t.Fail()
		}
		if !SameMeta(goldenMeta.FileMetaMap, state.MetaMap.FileInfoMap) {
			fmt.Println("state meta: ", state.MetaMap.FileInfoMap)
			t.Log("MetaStore state is not correct")
			t.Fail()
		}
	}
}

func TestRaftLogsCorrectlyOverwritten(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}
	filemeta2 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       2,
		BlockHashList: nil,
	}

	test.Clients[2].Crash(context.Background(), &emptypb.Empty{})
	test.Clients[1].Crash(context.Background(), &emptypb.Empty{})

	go test.Clients[leaderIdx].UpdateFile(context.Background(), filemeta1)
	test.Clients[leaderIdx].SendHeartbeat(context.Background(), &emptypb.Empty{})

	test.Clients[leaderIdx].Crash(context.Background(), &emptypb.Empty{})

	time.Sleep(2 * time.Second)

	test.Clients[1].Restore(context.Background(), &emptypb.Empty{})
	test.Clients[2].Restore(context.Background(), &emptypb.Empty{})

	leaderIdx = 1
	test.Clients[leaderIdx].SetLeader(context.Background(), &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(context.Background(), &emptypb.Empty{})
	go test.Clients[leaderIdx].UpdateFile(context.Background(), filemeta2)
	test.Clients[leaderIdx].SendHeartbeat(context.Background(), &emptypb.Empty{})

	test.Clients[0].Restore(context.Background(), &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(context.Background(), &emptypb.Empty{})

	time.Sleep(2 * time.Second)
	goldenMeta := surfstore.NewMetaStore("")
	goldenMeta.UpdateFile(test.Context, filemeta2)
	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         2,
		FileMetaData: filemeta2,
	})
	// goldenLog = append(goldenLog, &surfstore.UpdateOperation{
	// 	Term:         1,
	// 	FileMetaData: filemeta2,
	// })
	// fmt.Println(goldenLog)

	for _, server := range test.Clients {
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if !SameLog(goldenLog, state.Log) {
			fmt.Println("state log: ", state.Log)
			t.Log("Logs do not match")
			t.Fail()
		}
		if !SameMeta(goldenMeta.FileMetaMap, state.MetaMap.FileInfoMap) {
			fmt.Println("state meta: ", state.MetaMap.FileInfoMap)
			t.Log("MetaStore state is not correct")
			t.Fail()
		}
	}
}
