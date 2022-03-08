package surfstore

import (
	"fmt"
)

var ERR_SERVER_CRASHED = fmt.Errorf("Server is crashed.")
var ERR_NOT_LEADER = fmt.Errorf("Server is not the leader")
var SUCCESS = int(1)
var NOT_LEADER = int(2)
var CRASHED = int(3)
