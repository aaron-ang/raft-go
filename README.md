# Raft Consensus Implementation

This repository contains a complete implementation of the Raft consensus algorithm in Go, based on the [extended Raft paper](https://raft.github.io/raft.pdf).

## Features Implemented

- [x] Part 2A: Leader Election
- Leader election and heartbeats via AppendEntries RPCs
- Election timeouts and term management
- Automatic re-election on leader failure

- [x] Part 2B: Log Replication  
- Log entry replication via AppendEntries RPCs
- Log consistency checks
- Commitment of entries across the cluster

- [x] Part 2C: Persistence
- Persistent state management
- Crash recovery
- State machine snapshots

- [x] Part 2D: Log Compaction
- Log trimming with snapshots
- InstallSnapshot RPC implementation
- Snapshot state transfer between peers

## Project Structure

```
.
├── config.go     # Test configuration and utilities
├── persister.go  # Persistence layer
├── raft.go       # Core Raft implementation
├── util.go       # Helper functions
└── pkg/
	├── labgob/   # Encoding utilities
	└── labrpc/   # RPC framework
```


## Usage

The Raft implementation supports the following interface:

```go
// Create a new Raft server instance
rf := Make(peers, me, persister, applyCh)

// Start agreement on a new log entry
rf.Start(command interface{}) (index, term, isleader)

// Get current term and leader status
rf.GetState() (term, isLeader)
```

## Testing
Run the full test suite with:

```bash
go test -race
```

Or test specific parts:

```bash
go test -run 2A -race # Leader election
go test -run 2B -race # Log replication
go test -run 2C -race # Persistence
go test -run 2D -race # Log compaction
```

The implementation passes all test cases while maintaining reasonable performance metrics (typical full test suite runtime is under 8 minutes).