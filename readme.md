![tests](https://github.com/vadiminshakov/gowal/actions/workflows/tests.yml/badge.svg?branch=main)
[![Go Reference](https://pkg.go.dev/badge/github.com/vadiminshakov/gowal.svg)](https://pkg.go.dev/github.com/vadiminshakov/gowal)
[![Go Report Card](https://goreportcard.com/badge/github.com/vadiminshakov/gowal)](https://goreportcard.com/report/github.com/vadiminshakov/gowal)

# GoWAL - Write-Ahead Logging in Go

GoWAL is a simple, efficient **Write-Ahead Log (WAL)** library written in Go.
It allows you to store data in an append-only log structure, which is useful for applications that require crash recovery, transaction logging, or high-availability systems.
GoWAL is optimized for performance with **batch writing**, configurable segment rotation, and in-memory indexing.

## Features

- ✅ **Batch Writing**: Automatic batching of write requests reduces syscalls by ~100x
- ✅ **High Concurrency**: Lock-free write submission with single worker goroutine pattern
- ✅ **Flexible Durability**: Configurable fsync modes (none, batch-level, per-write)
- ✅ **Segment Rotation**: Automatic rotation when threshold is reached
- ✅ **Two-Level Indexing**: Fast lookups with in-memory indexes
- ✅ **Data Integrity**: CRC32 checksums for corruption detection
- ✅ **Graceful Shutdown**: Drains pending writes before closing


## Installation

```bash
go get github.com/vadiminshakov/gowal
```

## Performance

GoWAL achieves high throughput through batch writing:

**Benchmarks (Apple M4 Pro):**
- Sequential writes: **173,433 ops/sec** (5.8 µs/op)
- Concurrent writes (2 goroutines): **229,006 ops/sec** (4.4 µs/op) - **1.32x improvement**
- Concurrent writes (4 goroutines): **231,847 ops/sec** (4.3 µs/op) - **1.34x improvement**

**Key Benefits:**
- Syscalls reduced by ~100x through batching (1 syscall per ~100 writes instead of 1 per write)
- Automatic batching based on size (default: 100 requests) or time (default: 10ms)
- Lock-free write submission eliminates contention

## Usage

### Initialization

To create a new WAL instance, specify the directory to store logs and a prefix for the log files:

```go
import "github.com/vadiminshakov/gowal"

cfg := gowal.Config{
    Dir:    "./log",
    Prefix: "segment_",
    SegmentThreshold: 1000,
    MaxSegments:      100,
    IsInSyncDiskMode: false,
}

wal, err := gowal.NewWAL(cfg)
if err != nil {
    log.Fatal(err)
}
defer wal.Close()
```

### Adding a log entry
You can append a new log entry by providing an index, a key, and a value:
```go
err := wal.Write(1, "myKey", []byte("myValue"))
if err != nil {
    log.Fatal(err)
}
```
If the entry with the same index already exists, the function will return an error.

### Writing with explicit sync

For critical writes that require immediate durability, use `WriteWithSync`:

```go
// Write with explicit fsync (blocks until data is on disk)
err := wal.WriteWithSync(1, "criticalKey", []byte("criticalValue"), true)
if err != nil {
    log.Fatal(err)
}
```

**Durability Modes:**
- `Write()`: Default mode, writes are batched without fsync (best performance)
- `WriteWithSync(..., true)`: Forces fsync for this specific write (guaranteed durability)
- `Config.BatchSync = true`: Fsync after each batch (balanced mode)

### Retrieving a log entry

You can retrieve a log entry by its index:

```go
key, value, err := wal.Get(1)
if err != nil {
    log.Println("Entry not found or error:", err)
} else {
    log.Printf("Key: %s, Value: %s", key, string(value))
}
```

### Iterating over log entries

You can iterate over all log entries using the `Iterator` function:

```go
for msg := range wal.Iterator() {
    log.Printf("Key: %s, Value: %s\n", msg.Key, string(msg.Value))
}
```

### Closing the WAL
Always ensure that you close the WAL instance to properly flush and close the log files:

```go
err := wal.Close()
if err != nil {
    log.Fatal(err)
}
```

### Recover corrupted WAL
If the WAL is corrupted, you can recover it by calling the `UnsafeRecover` function:

```go
removedFiles, err := gowal.UnsafeRecover("./log", "segment_")
if err != nil {
    log.Fatal(err)
}
log.Printf("Removed corrupted files: %v", removedFiles)
```

### Configuration
The behavior of the WAL can be configured using several configuration options (`Config` parameter in the `NewWAL` function):

#### Core Settings
- `Dir`: Directory where log files will be stored
- `Prefix`: Prefix for segment files (e.g., "segment_")
- `SegmentThreshold`: Maximum number of log entries per segment before rotation occurs. Default is 1000.
- `MaxSegments`: Maximum number of segments to keep before the oldest segments are deleted. Default is 5.
- `IsInSyncDiskMode`: When set to true, every write is synced to disk, ensuring durability at the cost of performance. Default is false.

#### Batch Writing Settings (New)
- `MaxBatchSize`: Maximum number of write requests per batch. Default is 100.
- `MaxBatchDelay`: Maximum time to wait for accumulating a batch. Default is 10ms.
- `WriteChannelSize`: Size of the buffered channel for write requests. Default is 1000.
- `BatchSync`: When true, fsync is called after each batch write. Default is false.

**Example with batch configuration:**
```go
cfg := gowal.Config{
    Dir:              "./log",
    Prefix:           "segment_",
    SegmentThreshold: 1000,
    MaxSegments:      100,
    IsInSyncDiskMode: false,

    // Batch writing configuration
    MaxBatchSize:     100,              // Batch up to 100 writes
    MaxBatchDelay:    10 * time.Millisecond, // Or flush after 10ms
    WriteChannelSize: 1000,             // Buffer for incoming writes
    BatchSync:        false,            // Don't fsync each batch (max performance)
}

wal, err := gowal.NewWAL(cfg)
if err != nil {
    log.Fatal(err)
}
defer wal.Close()
```

## Architecture

GoWAL uses a segmented architecture with batch writing and two-level indexing for efficient write and read operations:

### Batch Writing Architecture

GoWAL implements the **Group Commit Pattern** with a single worker goroutine:

```
Write() → Submit to Channel → Wait for Response
                                      ↓
                        [Write Worker Goroutine]
                                      ↓
                      Collect Batch (10ms or 100 writes)
                                      ↓
                     Single Batched Write (1 syscall)
                                      ↓
                            Notify All Waiters
```

**Benefits:**
- **Lock-free submission**: Multiple goroutines submit writes without blocking
- **Batch processing**: Worker collects multiple writes and processes them together
- **Single writer**: No write contention, simplified rotation and indexing
- **Syscall reduction**: One write syscall per batch instead of per request

#### Segments
Data is split into numbered files (`segment_0`, `segment_1`, etc.). Each record contains:
- Index, Key, Value
- CRC32 checksum for integrity verification

#### Two-Level Indexing
- **tmpIndex**: In-memory index for the current active segment. Maps record index to its position in the segment.
- **index**: Main in-memory index for all persisted (closed) segments. Provides fast lookups across historical data.

#### Write Flow

**Client Side (Lock-Free):**
1. Create write request with index, key, value
2. Submit to channel (non-blocking if buffer has space)
3. Wait for response from worker

**Worker Goroutine:**
1. Collect batch (up to `MaxBatchSize` requests or `MaxBatchDelay` timeout)
2. Acquire lock and check if rotation needed
3. For each request in batch:
   - Check for duplicate indexes (including within-batch duplicates)
   - Calculate CRC32 checksum
   - Serialize record using MessagePack
4. Write all serialized records in a single syscall
5. Optionally fsync (based on `BatchSync` or per-request `sync` flag)
6. Update indexes and notify all waiters

**Graceful Shutdown:**
- `Close()` signals shutdown and waits for worker to drain pending writes
- All in-flight writes either complete successfully or return `ErrShutdown`

#### Rotation & Segment Management
When `tmpIndex` size exceeds `SegmentThreshold`:
1. Current segment is closed
2. `tmpIndex` is merged into main `index`
3. `tmpIndex` is cleared
4. New segment is created

When `MaxSegments` limit is reached, the oldest segment is automatically deleted along with its index entries to manage disk space.

#### Read Operations
Lookups check both indexes:
1. Check `tmpIndex` first (current segment, smaller and more likely to contain recent data)
2. If not found, check main `index` (historical segments)
3. Verify checksum before returning data

### Contributing
Feel free to open issues or submit pull requests for improvements and bug fixes. We welcome contributions!

### License
This project is licensed under the Apache License.