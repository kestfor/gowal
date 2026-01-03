package gowal

import (
	"time"

	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack/v5"
)

// writeRequest represents a single write operation submitted to the batch writer.
type writeRequest struct {
	index       uint64
	key         string
	value       []byte
	sync        bool // whether this write requires fsync
	isTombstone bool // whether this is a tombstone write (allows overwrite)
	respCh      chan writeResponse
}

// writeResponse contains the result of a write operation.
type writeResponse struct {
	offset int64
	err    error
}

// batchConfig holds configuration for batch writing.
type batchConfig struct {
	maxBatchSize  int           // max requests per batch
	maxBatchDelay time.Duration // max time to wait for batch
	channelSize   int           // size of request channel
	batchSync     bool          // fsync after each batch
}

// writeBatch accumulates requests for batched write.
type writeBatch struct {
	requests []*writeRequest
}

// writeWorker is the main worker goroutine that processes write requests in batches.
// It runs continuously until shutdown signal is received.
func (c *Wal) writeWorker() {
	defer c.workerDone.Done()

	for {
		batch := c.collectBatch()
		if batch == nil {
			// Shutdown signal received
			return
		}
		c.processBatch(batch)
	}
}

// collectBatch collects write requests into a batch based on size and time limits.
// Returns nil when shutdown signal is received.
func (c *Wal) collectBatch() *writeBatch {
	batch := &writeBatch{
		requests: make([]*writeRequest, 0, c.batchCfg.maxBatchSize),
	}

	timer := time.NewTimer(c.batchCfg.maxBatchDelay)
	defer timer.Stop()

	// Collect first request (blocking) - ensures we don't busy wait
	select {
	case req := <-c.writeCh:
		batch.requests = append(batch.requests, req)
	case <-c.shutdownCh:
		return nil
	}

	// Collect additional requests (non-blocking) up to batch size or timeout
collectLoop:
	for len(batch.requests) < c.batchCfg.maxBatchSize {
		select {
		case req := <-c.writeCh:
			batch.requests = append(batch.requests, req)
		case <-timer.C:
			// Timeout reached - process current batch
			break collectLoop
		default:
			// No more pending requests - process current batch
			break collectLoop
		}
	}

	return batch
}

// processBatch processes a batch of write requests.
// All requests in the batch are written to disk in a single syscall.
func (c *Wal) processBatch(batch *writeBatch) {
	// Acquire lock first to protect rotation (which modifies index/tmpIndex)
	c.mu.Lock()

	// Check for rotation - needs lock because openNewSegment modifies index/tmpIndex
	if err := c.rotateIfNeeded(); err != nil {
		c.mu.Unlock()
		c.failBatch(batch, err)
		return
	}

	// Serialize all messages and filter out duplicates
	// IMPORTANT: Check duplicates with lock to avoid race conditions
	// Track indexes being processed in this batch to catch duplicates within the batch
	serialized := make([][]byte, 0, len(batch.requests))
	validRequests := make([]*writeRequest, 0, len(batch.requests))
	messages := make([]msg, 0, len(batch.requests))
	processingIndexes := make(map[uint64]bool) // Track indexes in current batch

	for _, req := range batch.requests {
		// For tombstones, we want to overwrite existing records
		// For normal writes, check for duplicates (race condition protection)
		if !req.isTombstone {
			// Check if already exists in storage
			if _, exists := c.tmpIndex[req.index]; exists {
				req.respCh <- writeResponse{err: ErrExists}
				continue
			}
			if _, exists := c.index[req.index]; exists {
				req.respCh <- writeResponse{err: ErrExists}
				continue
			}
			// Check if already being processed in this batch
			if processingIndexes[req.index] {
				req.respCh <- writeResponse{err: ErrExists}
				continue
			}
			// Mark as being processed
			processingIndexes[req.index] = true
		}

		// Create message with checksum
		m := msg{Key: req.key, Value: req.value, Idx: req.index}
		m.Checksum = m.calculateChecksum()

		// Serialize
		data, err := msgpack.Marshal(m)
		if err != nil {
			req.respCh <- writeResponse{err: errors.Wrap(err, "failed to encode msg")}
			continue
		}

		serialized = append(serialized, data)
		validRequests = append(validRequests, req)
		messages = append(messages, m)
	}
	c.mu.Unlock()

	// Single write syscall for entire batch
	var writeErr error
	for i, data := range serialized {
		if _, err := c.log.Write(data); err != nil {
			writeErr = errors.Wrap(err, "failed to write msg to log")
			// Fail remaining requests
			c.failRemainingBatch(validRequests[i:], writeErr)
			return
		}
		c.lastOffset += int64(len(data))
	}

	// Determine if fsync needed
	needSync := c.batchCfg.batchSync || c.isInSyncDiskMode
	if !needSync {
		// Check if any request explicitly requires sync
		for _, req := range validRequests {
			if req.sync {
				needSync = true
				break
			}
		}
	}

	// Fsync if required
	if needSync {
		if err := c.log.Sync(); err != nil {
			c.failBatch(batch, errors.Wrap(err, "failed to sync log"))
			return
		}
	}

	// Update indexes and notify waiters
	c.mu.Lock()
	for i, req := range validRequests {
		c.tmpIndex[req.index] = messages[i]
		c.lastIndex.Add(1)

		// For tombstones, remove from main index if it exists there
		if req.isTombstone {
			delete(c.index, req.index)
		}

		req.respCh <- writeResponse{offset: c.lastOffset, err: nil}
	}
	c.mu.Unlock()
}

// failBatch fails all requests in a batch with the given error.
func (c *Wal) failBatch(batch *writeBatch, err error) {
	for _, req := range batch.requests {
		select {
		case req.respCh <- writeResponse{err: err}:
		default:
			// Response channel might be closed
		}
	}
}

// failRemainingBatch fails remaining requests in a batch with the given error.
// Used when a write operation fails partway through a batch.
func (c *Wal) failRemainingBatch(requests []*writeRequest, err error) {
	for _, req := range requests {
		select {
		case req.respCh <- writeResponse{err: err}:
		default:
			// Response channel might be closed
		}
	}
}
