package gowal

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Benchmarks

func BenchmarkWrite_Sequential(b *testing.B) {
	os.RemoveAll("./bench_seq")
	defer os.RemoveAll("./bench_seq")

	log, err := NewWAL(Config{
		Dir:              "./bench_seq",
		Prefix:           "log_",
		SegmentThreshold: 10000,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(b, err)
	defer log.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := log.Write(uint64(i), "key", []byte("value"))
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func BenchmarkWrite_Concurrent_2(b *testing.B) {
	benchmarkWriteConcurrent(b, 2)
}

func BenchmarkWrite_Concurrent_4(b *testing.B) {
	benchmarkWriteConcurrent(b, 4)
}

func BenchmarkWrite_Concurrent_8(b *testing.B) {
	benchmarkWriteConcurrent(b, 8)
}

func BenchmarkWrite_Concurrent_16(b *testing.B) {
	benchmarkWriteConcurrent(b, 16)
}

func benchmarkWriteConcurrent(b *testing.B, goroutines int) {
	dir := fmt.Sprintf("./bench_conc_%d", goroutines)
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	log, err := NewWAL(Config{
		Dir:              dir,
		Prefix:           "log_",
		SegmentThreshold: 10000,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(b, err)
	defer log.Close()

	var counter atomic.Uint64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			idx := counter.Add(1)
			err := log.Write(idx, "key", []byte("value"))
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.StopTimer()
}

// Concurrent Stress Tests

func TestConcurrentWrites(t *testing.T) {
	os.RemoveAll("./test_concurrent")
	defer os.RemoveAll("./test_concurrent")

	log, err := NewWAL(Config{
		Dir:              "./test_concurrent",
		Prefix:           "log_",
		SegmentThreshold: 100,
		MaxSegments:      15, // Increased to accommodate 1000 writes (10 segments) + buffer
		IsInSyncDiskMode: false,
		MaxBatchSize:     20,               // Smaller batches
		MaxBatchDelay:    time.Millisecond, // Very short delay
	})
	require.NoError(t, err)
	defer log.Close()

	const numGoroutines = 10
	const writesPerGoroutine = 100
	const totalWrites = numGoroutines * writesPerGoroutine

	var wg sync.WaitGroup
	var successCount atomic.Uint64
	var errorCount atomic.Uint64

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < writesPerGoroutine; i++ {
				idx := uint64(goroutineID*writesPerGoroutine + i)
				err := log.Write(idx, fmt.Sprintf("key%d", idx), []byte(fmt.Sprintf("value%d", idx)))
				if err != nil {
					errorCount.Add(1)
					t.Logf("Write error for index %d: %v", idx, err)
				} else {
					successCount.Add(1)
				}
			}
		}(g)
	}

	wg.Wait()

	// Verify all writes succeeded
	require.Equal(t, uint64(totalWrites), successCount.Load(), "All writes should succeed")
	require.Equal(t, uint64(0), errorCount.Load(), "No writes should fail")

	// Give worker time to finish processing and updating indexes
	time.Sleep(100 * time.Millisecond)

	// Verify we can read all written values with retry logic
	for i := 0; i < totalWrites; i++ {
		var key string
		var value []byte
		var err error

		// Retry a few times if not found (worker might still be updating indexes)
		for retry := 0; retry < 5; retry++ {
			key, value, err = log.Get(uint64(i))
			if err == nil && key != "" {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}

		require.NoError(t, err, "Failed to read index %d", i)
		require.NotEmpty(t, key, "Key is empty for index %d", i)
		require.Equal(t, fmt.Sprintf("key%d", i), key)
		require.Equal(t, fmt.Sprintf("value%d", i), string(value))
	}
}

func TestConcurrentWritesDuplicates(t *testing.T) {
	os.RemoveAll("./test_concurrent_dup")
	defer os.RemoveAll("./test_concurrent_dup")

	log, err := NewWAL(Config{
		Dir:              "./test_concurrent_dup",
		Prefix:           "log_",
		SegmentThreshold: 100,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)
	defer log.Close()

	const numGoroutines = 10
	const sameIndex = uint64(42)

	var wg sync.WaitGroup
	var successCount atomic.Uint64
	var errorCount atomic.Uint64

	// All goroutines try to write to the same index
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			err := log.Write(sameIndex, "key", []byte(fmt.Sprintf("value%d", goroutineID)))
			if errors.Is(err, ErrExists) {
				errorCount.Add(1)
			} else if err != nil {
				t.Logf("Unexpected error: %v", err)
			} else {
				successCount.Add(1)
			}
		}(g)
	}

	wg.Wait()

	// Exactly one should succeed
	require.Equal(t, uint64(1), successCount.Load(), "Exactly one write should succeed")
	require.Equal(t, uint64(numGoroutines-1), errorCount.Load(), "Others should get ErrExists")
}

// Batch Boundary Tests

func TestBatchBoundary_ExactSize(t *testing.T) {
	os.RemoveAll("./test_batch_exact")
	defer os.RemoveAll("./test_batch_exact")

	const maxBatchSize = 50

	log, err := NewWAL(Config{
		Dir:              "./test_batch_exact",
		Prefix:           "log_",
		SegmentThreshold: 1000,
		MaxSegments:      5,
		MaxBatchSize:     maxBatchSize,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)
	defer log.Close()

	// Write exactly maxBatchSize entries
	for i := 0; i < maxBatchSize; i++ {
		err := log.Write(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i)))
		require.NoError(t, err)
	}

	// Verify all entries
	for i := 0; i < maxBatchSize; i++ {
		key, value, err := log.Get(uint64(i))
		require.NoError(t, err)
		require.Equal(t, "key"+strconv.Itoa(i), key)
		require.Equal(t, "value"+strconv.Itoa(i), string(value))
	}
}

func TestBatchBoundary_OverSize(t *testing.T) {
	os.RemoveAll("./test_batch_over")
	defer os.RemoveAll("./test_batch_over")

	const maxBatchSize = 50

	log, err := NewWAL(Config{
		Dir:              "./test_batch_over",
		Prefix:           "log_",
		SegmentThreshold: 1000,
		MaxSegments:      5,
		MaxBatchSize:     maxBatchSize,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)
	defer log.Close()

	// Write maxBatchSize + 1 entries (should create two batches)
	for i := 0; i < maxBatchSize+1; i++ {
		err := log.Write(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i)))
		require.NoError(t, err)
	}

	// Verify all entries
	for i := 0; i < maxBatchSize+1; i++ {
		key, value, err := log.Get(uint64(i))
		require.NoError(t, err)
		require.Equal(t, "key"+strconv.Itoa(i), key)
		require.Equal(t, "value"+strconv.Itoa(i), string(value))
	}
}

// Shutdown Tests

func TestGracefulShutdown(t *testing.T) {
	os.RemoveAll("./test_shutdown")
	defer os.RemoveAll("./test_shutdown")

	log, err := NewWAL(Config{
		Dir:              "./test_shutdown",
		Prefix:           "log_",
		SegmentThreshold: 1000,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)

	const numWrites = 100
	var wg sync.WaitGroup
	var successCount atomic.Uint64
	var shutdownErrCount atomic.Uint64

	// Start concurrent writes
	for i := 0; i < numWrites; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			err := log.Write(uint64(idx), fmt.Sprintf("key%d", idx), []byte(fmt.Sprintf("value%d", idx)))
			if err == nil {
				successCount.Add(1)
			} else if errors.Is(err, ErrShutdown) {
				shutdownErrCount.Add(1)
			} else {
				t.Logf("Unexpected error: %v", err)
			}
		}(i)
	}

	// Let some writes complete before closing
	time.Sleep(10 * time.Millisecond)

	// Close while writes are in flight
	err = log.Close()
	require.NoError(t, err)

	wg.Wait()

	t.Logf("Successful writes: %d, Shutdown errors: %d", successCount.Load(), shutdownErrCount.Load())
	require.Equal(t, uint64(numWrites), successCount.Load()+shutdownErrCount.Load(), "All writes should complete or get shutdown error")

	// Verify all successful writes are readable
	log2, err := NewWAL(Config{
		Dir:              "./test_shutdown",
		Prefix:           "log_",
		SegmentThreshold: 1000,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)
	defer log2.Close()

	for i := 0; i < int(successCount.Load()); i++ {
		_, _, err := log2.Get(uint64(i))
		require.NoError(t, err, "All successful writes should be readable after restart")
	}
}

func TestWriteAfterShutdown(t *testing.T) {
	os.RemoveAll("./test_write_after_shutdown")
	defer os.RemoveAll("./test_write_after_shutdown")

	log, err := NewWAL(Config{
		Dir:              "./test_write_after_shutdown",
		Prefix:           "log_",
		SegmentThreshold: 100,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)

	// Write some data
	err = log.Write(0, "key0", []byte("value0"))
	require.NoError(t, err)

	// Close the WAL
	err = log.Close()
	require.NoError(t, err)

	// Give it a moment to fully shutdown
	time.Sleep(10 * time.Millisecond)

	// Try to write after close - should return ErrShutdown immediately
	done := make(chan error, 1)
	go func() {
		done <- log.Write(1, "key1", []byte("value1"))
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrShutdown, "Write after shutdown should return ErrShutdown")
	case <-time.After(1 * time.Second):
		t.Fatal("Write after shutdown took too long - possible deadlock")
	}
}

// WriteWithSync Tests

func TestWriteWithSync(t *testing.T) {
	os.RemoveAll("./test_write_sync")
	defer os.RemoveAll("./test_write_sync")

	log, err := NewWAL(Config{
		Dir:              "./test_write_sync",
		Prefix:           "log_",
		SegmentThreshold: 100,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
		BatchSync:        false, // Disable batch sync to test per-write sync
	})
	require.NoError(t, err)
	defer log.Close()

	// Write with explicit sync
	err = log.WriteWithSync(0, "key0", []byte("value0"), true)
	require.NoError(t, err)

	// Write without sync
	err = log.WriteWithSync(1, "key1", []byte("value1"), false)
	require.NoError(t, err)

	// Verify both are readable
	key, value, err := log.Get(0)
	require.NoError(t, err)
	require.Equal(t, "key0", key)
	require.Equal(t, "value0", string(value))

	key, value, err = log.Get(1)
	require.NoError(t, err)
	require.Equal(t, "key1", key)
	require.Equal(t, "value1", string(value))
}

func TestBatchSync(t *testing.T) {
	os.RemoveAll("./test_batch_sync")
	defer os.RemoveAll("./test_batch_sync")

	log, err := NewWAL(Config{
		Dir:              "./test_batch_sync",
		Prefix:           "log_",
		SegmentThreshold: 100,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
		BatchSync:        true, // Enable batch sync
	})
	require.NoError(t, err)
	defer log.Close()

	// Write multiple entries (should be synced as a batch)
	for i := 0; i < 10; i++ {
		err := log.Write(uint64(i), fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("value%d", i)))
		require.NoError(t, err)
	}

	// Verify all entries
	for i := 0; i < 10; i++ {
		key, value, err := log.Get(uint64(i))
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("key%d", i), key)
		require.Equal(t, fmt.Sprintf("value%d", i), string(value))
	}
}

// Rotation with Batching Tests

func TestRotationDuringBatch(t *testing.T) {
	os.RemoveAll("./test_rotation_batch")
	defer os.RemoveAll("./test_rotation_batch")

	const segmentThreshold = 10

	log, err := NewWAL(Config{
		Dir:              "./test_rotation_batch",
		Prefix:           "log_",
		SegmentThreshold: segmentThreshold,
		MaxSegments:      5,
		MaxBatchSize:     20, // Batch size larger than segment threshold
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)
	defer log.Close()

	// Write enough to trigger rotation during batch processing
	const numWrites = segmentThreshold * 2
	for i := 0; i < numWrites; i++ {
		err := log.Write(uint64(i), fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("value%d", i)))
		require.NoError(t, err)
	}

	// Verify all entries
	for i := 0; i < numWrites; i++ {
		key, value, err := log.Get(uint64(i))
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("key%d", i), key)
		require.Equal(t, fmt.Sprintf("value%d", i), string(value))
	}
}
