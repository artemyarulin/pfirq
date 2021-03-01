# PFIRQ

PFIRQ - **P**ersisted **FI**FO **R**ing **Q**ueue

Embedded Go persisted queue storage where processed items from the head could be atomically appended to the tail for processing again. PFIRQ was designed for cases when you need to process data over and over again.
```go
// Where data is persisted
storagePath := "/data"
// Data will be saved in files of 100MB
segmentSize := 100 * 1024 * 1024
// Read buffer size - used for reading
rbufSize := 10 * 1024 * 1024  
// Write buffer size - used for writing
wbufSize := 90 * 1024 * 1024
// Total buffers size is max memory usage. During work Pfirq does not allocates, see benchmarks
q := NewPfirq(storagePath, segmentSize, rbufSize, wbufSize)

// Push data to the write buffer which is saved to disk in background
q.Write([]byte{0x01}, []byte{0x00})

// Read data from the queue
q.Read(func(key []byte, val []byte) (bool, []byte, []byte) {
    // Return false if item cannot be processed yet and should remain in queue e.g. pop
    if !canProcess(key, val) {
    	return false, nil, nil
    }
    // Return data if record should be removed from the queue
    // but new record is added to the end for processing again
    key2, val2 := process(key, val)
    return true, key2, val2
})

// Close waits until all data got persisted and fsynced to the disk
q.Close()
```

## Goals

- Configurable and fixed memory usage with low allocation
- Atomic - after crash partially written data will be ignored
- At least once guarantee - after crash already processed records may be processed again
- Backpressure - Read/Write blocks when read/write buffers are full and waits until data read/written to disk
- Data agnostic - keys and values are array of bytes of any size
- Compact - each record has only 32 bytes of additional metadata for each key/val record
- Thread safe
- Instant startup time
- TODO Record deletion  
- TODO Speed equal to the sequential read/write speed of the disk. Current bottleneck is fsync which happens after every write
- TODO Compression

## Non goals

- Durability - background write process fsyncs all, but Write/Read API work via read/write cache. Data can be lost in case of a crash
- Random reads not possible with current design. Data format is simple and fixed so for debugging purposes we just read files manually

## Performance

Currently bounded by fsync calls which happens after each write. Example benchmark with SSD:

- 5000 random records from 1 to 500KB, total size 1GB
- 30 000 processed records
- Total time: 70s
- Average time for one record: 2.3ms
- Average processed read+write data per second: 180MB/s
- Total data for read+write: 13GB
- Total allocations after init: 2.5MB
```go
segmentSize = 10*1024*1024
rbufSize = 10*1024*1024
wbufSize = 90*1024*1024
buf := New(tmpPath, segmentSize, rbufSize, wbufSize)
for i := 0; i < b.N; i++ {
	buf.Read(func(key []byte, val []byte) (bool, []byte, []byte) {
		return true, key, val
	})
}
```
