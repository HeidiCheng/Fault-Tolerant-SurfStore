# Fault-Tolerant SurfStore
Reproduced Raft log consensus algorithm for fault-tolerant SurfStore


# Makefile

Run BlockStore server:
```console
$ make run-blockstore
```

Run RaftSurfstore server:
```console
$ make IDX=0 run-raft
```

Test:
```console
$ make test
```

Specific Test:
```console
$ make TEST_REGEX=Test specific-test
```

Clean:
```console
$ make clean
```
# Reference
[1] Ongaro, Diego, and John Ousterhout. "In search of an understandable consensus algorithm." 2014 USENIX Annual Technical Conference (Usenix ATC 14). 2014.
