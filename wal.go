package gowal

import (
	"iter"
	"os"
	"path"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

var ErrExists = errors.New("msg with such index already exists")
var ErrShutdown = errors.New("wal is shutting down")

// Wal is a write-ahead log that stores key-value pairs.
//
// Wal is append-only log, so we can't delete records from it, but log is divided into segments, which are rotated (oldest deleted) when
// segments number threshold is reached.
//
// Index stored in memory and loaded from disk on Wal init.
type Wal struct {
	// mutex for thread safety
	mu sync.RWMutex

	// append-only log with proposed messages that node consumed
	log *os.File

	// index that matches height of msg record with offset in file
	index    map[uint64]msg
	tmpIndex map[uint64]msg

	// path to directory with logs
	pathToLogsDir string

	// name of the old segment
	oldestSegName string

	// offset of last record in file
	lastOffset int64

	lastIndex atomic.Uint64

	// number of segments for log
	segmentsNumber int

	// prefix for segment files
	prefix string

	segmentsThreshold int

	maxSegments int

	isInSyncDiskMode bool

	// Batch writing fields
	writeCh    chan *writeRequest
	shutdownCh chan struct{}
	workerDone sync.WaitGroup
	batchCfg   batchConfig
}

// Config represents the configuration for the WAL (Write-Ahead Log).
type Config struct {
	// Dir is the directory where the log files will be stored.
	Dir string

	// Prefix is the prefix for the segment files.
	Prefix string

	// SegmentThreshold is the number of records after which a new segment is created.
	SegmentThreshold int

	// MaxSegments is the maximum number of segments allowed before the oldest segment is deleted.
	MaxSegments int

	// IsInSyncDiskMode indicates whether the log should be synced to disk after each write.
	IsInSyncDiskMode bool

	// MaxBatchSize is the maximum number of write requests per batch.
	// Default: 100
	MaxBatchSize int

	// MaxBatchDelay is the maximum time to wait for accumulating a batch.
	// Default: 10ms
	MaxBatchDelay time.Duration

	// WriteChannelSize is the size of the buffered channel for write requests.
	// Default: 1000
	WriteChannelSize int

	// BatchSync indicates whether to fsync after each batch write.
	// Default: false
	BatchSync bool
}

// NewWAL creates a new WAL with the given configuration.
func NewWAL(config Config) (*Wal, error) {
	if err := os.MkdirAll(config.Dir, 0755); err != nil {
		return nil, errors.Wrap(err, "failed to create log directory")
	}

	// Apply defaults for batch configuration
	if config.MaxBatchSize == 0 {
		config.MaxBatchSize = 100
	}
	if config.MaxBatchDelay == 0 {
		config.MaxBatchDelay = 10 * time.Millisecond
	}
	if config.WriteChannelSize == 0 {
		config.WriteChannelSize = 1000
	}

	segmentsNumbers, err := findSegmentNumber(config.Dir, config.Prefix)
	if err != nil {
		return nil, errors.Wrap(err, "failed to find segment numbers")
	}

	// load segments into mem
	fd, lastOffset, index, err := segmentInfoAndIndex(segmentsNumbers, path.Join(config.Dir, config.Prefix))
	if err != nil {
		return nil, errors.Wrap(err, "failed to load log segments")
	}
	numberOfSegments := len(index) / config.SegmentThreshold
	if numberOfSegments == 0 {
		numberOfSegments = 1
	}

	w := &Wal{
		log:               fd,
		index:             index,
		tmpIndex:          make(map[uint64]msg),
		lastOffset:        lastOffset,
		pathToLogsDir:     config.Dir,
		segmentsNumber:    numberOfSegments,
		prefix:            config.Prefix,
		segmentsThreshold: config.SegmentThreshold,
		maxSegments:       config.MaxSegments,
		isInSyncDiskMode:  config.IsInSyncDiskMode,
		writeCh:           make(chan *writeRequest, config.WriteChannelSize),
		shutdownCh:        make(chan struct{}),
		batchCfg: batchConfig{
			maxBatchSize:  config.MaxBatchSize,
			maxBatchDelay: config.MaxBatchDelay,
			channelSize:   config.WriteChannelSize,
			batchSync:     config.BatchSync,
		},
	}

	lastIndex := uint64(0)
	for v := range w.Iterator() {
		if v.Idx > lastIndex {
			lastIndex = v.Idx
		}
	}

	w.lastIndex.Store(lastIndex)

	// Start write worker goroutine
	w.workerDone.Add(1)
	go w.writeWorker()

	return w, nil
}

// UnsafeRecover recovers the WAL from the given directory.
// It is unsafe because it removes all the segment and checksum files that are corrupted (checksums do not match).
// It returns the list of segment and checksum files that were removed.
func UnsafeRecover(dir, segmentPrefix string) ([]string, error) {
	segmentsNumbers, err := findSegmentNumber(dir, segmentPrefix)
	if err != nil {
		return nil, errors.Wrap(err, "failed to find segment numbers")
	}

	return removeCorruptedSegments(segmentsNumbers, path.Join(dir, segmentPrefix))
}

// Get queries value at specific index in the log.
func (c *Wal) Get(index uint64) (string, []byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// check temporary index first (smaller, more likely to contain recent data)
	msg, ok := c.tmpIndex[index]
	if ok {
		// verify checksum on read
		if err := msg.verifyChecksum(); err != nil {
			return "", nil, err
		}
		return msg.Key, msg.Value, nil
	}

	// check main index for historical segments
	msg, ok = c.index[index]
	if ok {
		// verify checksum on read
		if err := msg.verifyChecksum(); err != nil {
			return "", nil, err
		}
		return msg.Key, msg.Value, nil
	}

	return "", nil, nil
}

// CurrentIndex returns current index of the log.
func (c *Wal) CurrentIndex() uint64 {
	return c.lastIndex.Load()
}

// Write writes key-value pair to the log.
func (c *Wal) Write(index uint64, key string, value []byte) error {
	return c.WriteWithSync(index, key, value, false)
}

// WriteWithSync writes key-value pair to the log with optional fsync.
// If sync is true, the write will be fsynced to disk before returning.
func (c *Wal) WriteWithSync(index uint64, key string, value []byte, sync bool) error {
	// Check shutdown first
	select {
	case <-c.shutdownCh:
		return ErrShutdown
	default:
	}

	// Create request
	req := &writeRequest{
		index:       index,
		key:         key,
		value:       value,
		sync:        sync,
		isTombstone: false,
		respCh:      make(chan writeResponse, 1),
	}

	// Submit to worker
	select {
	case c.writeCh <- req:
	case <-c.shutdownCh:
		return ErrShutdown
	}

	// Wait for response
	resp := <-req.respCh
	return resp.err
}

// WriteTombstone writes a tombstone record for the given index.
// If no record exists for the index, returns nil (no-op).
// If a record exists, overwrites it with a tombstone.
func (c *Wal) WriteTombstone(index uint64) error {
	// Check if record exists
	c.mu.RLock()
	var existingMsg msg
	var exists bool

	if msg, ok := c.index[index]; ok {
		existingMsg = msg
		exists = true
	} else if msg, ok := c.tmpIndex[index]; ok {
		existingMsg = msg
		exists = true
	}
	c.mu.RUnlock()

	// If no record exists, return nil (no-op)
	if !exists {
		return nil
	}

	// Create tombstone request (with isTombstone flag to allow overwrite)
	req := &writeRequest{
		index:       index,
		key:         existingMsg.Key,
		value:       []byte("tombstone"),
		sync:        false,
		isTombstone: true,
		respCh:      make(chan writeResponse, 1),
	}

	// Submit to worker
	select {
	case c.writeCh <- req:
	case <-c.shutdownCh:
		return ErrShutdown
	}

	// Wait for response
	resp := <-req.respCh
	return resp.err
}

// Iterator returns push-based iterator for the WAL messages.
// Messages are returned from the oldest to the newest.
//
// Should be used like this:
//
//	for msg := range wal.Iterator() {
//		...
func (c *Wal) Iterator() iter.Seq[msg] {
	return func(yield func(msg) bool) {
		c.mu.RLock()

		// collect indexes from both main index and tmpIndex
		allIndexes := make(map[uint64]msg, len(c.index)+len(c.tmpIndex))
		for k, v := range c.index {
			allIndexes[k] = v
		}
		for k, v := range c.tmpIndex {
			allIndexes[k] = v
		}

		msgIndexes := make([]uint64, 0, len(allIndexes))
		for k := range allIndexes {
			msgIndexes = append(msgIndexes, k)
		}

		// create a copy of messages to avoid holding lock during iteration
		msgs := make([]msg, len(msgIndexes))
		for i, idx := range msgIndexes {
			msgs[i] = allIndexes[idx]
		}
		c.mu.RUnlock()

		slices.SortFunc(msgs, func(a, b msg) int {
			if a.Idx < b.Idx {
				return -1
			}
			if a.Idx > b.Idx {
				return 1
			}
			return 0
		})

		for _, msg := range msgs {
			if !yield(msg) {
				break
			}
		}
	}
}

// PullIterator returns pull-based iterator for the WAL messages.
// Messages are returned from the oldest to the newest.
//
// Should be used like this:
//
//	next, stop := wal.PullIterator()
//	defer stop()
//	...
func (c *Wal) PullIterator() (next func() (msg, bool), stop func()) {
	return iter.Pull(c.Iterator())
}

// Close closes log file.
func (c *Wal) Close() error {
	// Signal shutdown to worker
	close(c.shutdownCh)

	// Wait for worker to drain pending writes and exit
	c.workerDone.Wait()

	// Now safe to close the file
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.log.Close(); err != nil {
		return errors.Wrap(err, "failed to close log file")
	}

	return nil
}
