package pring

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

/*
Code conventions:
- panic for everything unrecoverable - is there is no space on disk it's end of the world situation, etc.
- fmt/log allocates - use constant strings if possible
- panic("Description" + err.Errors()) allocates. Use panic(err.Error()) // Description (to reduce noise in escape analysis report)
- don't create new []byte buffers in hot path - try to receive those buffers via function parameters

Record:
 0 - 8  [8]byte - Record length
 8 - 16 [8]byte - Head pos
16 - 24 [8]byte - Head name
24 - 32 [8]byte - Key length
32 - *  [*]byte - Key
 * - *  [*]byte - Val

Offset:
[8]byte - Head position
[8]byte - Head name
[8]byte - Tail position
[8]byte - Tail name

*/
const (
	offsetHeadPos  = 8
	offsetHeadName = 16
	offsetKeyLen   = 24
	offsetPayload  = 32
)

var (
	nameFormat       = "%08d"
	nameFirst        = "00000000"
	nameOffset       = "OFFSET"
	zeroHeadMetadata = []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30}
)

type Pfirq struct {
	dataPath          string
	dataPathOffset    string
	dataPathOffsetTmp string
	segmentSize       int64
	wg                sync.WaitGroup
	stop              atomic.Value // bool
	head              atomic.Value // *os.File
	headPosition      atomic.Value // int64
	headReader        atomic.Value // bufio.Reader
	tail              atomic.Value // *os.File
	tailPosition      atomic.Value // int64
	wbuf              *ringbuf
	rbuf              *ringbuf
}

// New creates new Pfirq instance. Data will be split into multiple files of max size segmentSize
func New(dataPath string, segmentSize int64, rbufSize int64, wbufSize int64) *Pfirq {
	headName, headPos, tailName, tailPos := readOffset(dataPath)
	head := openSegment(path.Join(dataPath, headName), headPos, os.O_RDONLY|os.O_CREATE)
	tail := openSegment(path.Join(dataPath, tailName), tailPos, os.O_WRONLY|os.O_CREATE|os.O_APPEND)
	p := &Pfirq{
		dataPath:          dataPath,
		dataPathOffset:    path.Join(dataPath, nameOffset),
		dataPathOffsetTmp: path.Join(dataPath, nameOffset+".tmp"),
		segmentSize:       segmentSize,
		rbuf:              newRingbuf(rbufSize),
		wbuf:              newRingbuf(wbufSize),
		wg:                sync.WaitGroup{},
		stop:              atomic.Value{},
		headReader:        atomic.Value{},
	}
	// Truncate tail if there is partially written records after committed offset
	stats, err := os.Stat(tail.Name())
	if err != nil {
		panic(err.Error()) // cannot get tail segment stats
	}
	if stats.Size() > tailPos {
		if err := os.Truncate(tail.Name(), tailPos); err != nil {
			panic(err.Error()) // cannot truncate tail segment from not committed data
		}
	}
	deleteFiles(dataPath, "", tailName)
	p.stop.Store(false)
	p.head.Store(head)
	p.tail.Store(tail)
	p.headPosition.Store(headPos)
	p.tailPosition.Store(tailPos)
	p.headReader.Store(bufio.NewReader(head))
	p.wg.Add(1)
	go p.startReader()
	p.wg.Add(1)
	go p.startWriter()
	// fmt.Printf("Started at %v. Head=%v:%v. Tail=%v:%v. Buffers %v\n", p.dataPath, head.Name(), headPos, tail.Name(), tailPos, bufSize/2)
	return p
}

// Read reads data from internal read buffer and calls callback when data is available. Use data only during callback.
// Callback should return true if record should be removed from the head of the queue, false behaves like pop
// If callback return non nil key/val it will be added to the tail of the queue atomically with removing record from head
// After Close was called callback will never be called and this function will be returning immediately with false
// May block if no space is available in read/write buffer
func (p *Pfirq) Read(cb func(key []byte, val []byte) (bool, []byte, []byte)) bool {
	ok := p.rbuf.read(0, func(b []byte, lastRecOffset int) bool {
		headMetadata := b[offsetHeadPos:offsetKeyLen]
		keyLen := int(binary.BigEndian.Uint64(b[offsetKeyLen:]))
		recordProcessed, key, val := cb(b[offsetPayload:offsetPayload+keyLen], b[offsetPayload+keyLen:])
		if !recordProcessed { // Record cannot be processed - return
			return false
		}
		if len(key) == 0 || len(val) == 0 {
			panic("Deletion not supported yet")
		}
		// headMetadata points to the start of the record. Append length to confirm record got processed
		headPos := binary.BigEndian.Uint64(headMetadata[0:])
		binary.BigEndian.PutUint64(headMetadata[0:], headPos+uint64(len(b)))
		p.writeWithHead(key, val, headMetadata)
		return true
	})
	return ok
}

// Write appends data to internal write buffer
// May block if no space is available in write buffer
// After Close was called callback will never be called and this function will be returning immediately with false
func (p *Pfirq) Write(key []byte, val []byte) bool {
	return p.writeWithHead(key, val, zeroHeadMetadata)
}

// Close waits until all pending Read/Write operation complete
// May be called multiple times
func (p *Pfirq) Close() {
	p.stop.Store(true)
	p.rbuf.close()
	p.wbuf.close()
	p.wg.Wait()
	// Ignore if files were closed already to support calling Close multiple times for convenience
	if err := p.head.Load().(*os.File).Close(); err != nil {
		if !errors.Is(err, os.ErrClosed) {
			panic(err.Error()) // Head cannot be closed
		}
	}
	if err := p.tail.Load().(*os.File).Close(); err != nil {
		if !errors.Is(err, os.ErrClosed) {
			panic(err.Error()) // Tail cannot be closed
		}
	}
}

func (p *Pfirq) startReader() {
	metadata := make([]byte, 32)
	for {
		if p.stop.Load().(bool) {
			p.wg.Done()
			return
		}
		read, head, headPos := p.nextRecord(metadata)
		if !read { // Tail reached - wait and repeat
			time.Sleep(time.Millisecond * 100)
			continue
		}
		recLen := int(binary.BigEndian.Uint64(metadata))
		ok := p.rbuf.write(recLen, func(b []byte) {
			binary.BigEndian.PutUint64(b[offsetHeadPos:], uint64(headPos))
			copy(b[offsetHeadName:], filepath.Base(head.Name()))
			copy(b[offsetKeyLen:], metadata[offsetKeyLen:])
			reader := p.headReader.Load().(*bufio.Reader)
			_, err := io.ReadFull(reader, b[offsetPayload:])
			if err != nil {
				panic(err.Error()) // Error reading tail record
			}
			p.headPosition.Store(headPos + int64(recLen))
		})
		if !ok {
			p.wg.Done()
			return
		}
	}
}

func (p *Pfirq) writeWithHead(key []byte, val []byte, head []byte) bool {
	recLen := offsetPayload + len(key) + len(val)
	ok := p.wbuf.write(recLen, func(b []byte) {
		copy(b[offsetHeadPos:], head)
		binary.BigEndian.PutUint64(b[offsetKeyLen:], uint64(len(key)))
		copy(b[offsetPayload:], key)
		copy(b[offsetPayload+len(key):], val)
	})
	return ok
}

func (p *Pfirq) startWriter() {
	headPrev := zeroHeadMetadata
	headCur := make([]byte, 16)
	segmentPrev := make([]byte, 8)
	for {
		copy(segmentPrev, headPrev[8:])
		if !p.write(headPrev, headCur) {
			p.wg.Done()
			return
		}
		headPrev = headCur
		// If head segments got switched it means we can safely remove old ones
		if segmentCur := headPrev[8:]; !bytes.Equal(segmentPrev, segmentCur) {
			if err := os.Remove(path.Join(p.dataPath, bytesToString(segmentPrev))); err != nil {
				panic(err.Error()) // Error deleting old segment
			}
		}
	}
}

func (p *Pfirq) write(fallback []byte, headMetadata []byte) bool {
	tailPos := p.tailPosition.Load().(int64)
	tail := p.tail.Load().(*os.File)
	spaceInSegment := p.segmentSize - tailPos
	ok := p.wbuf.read(spaceInSegment, func(b []byte, lastRecOffset int) bool {
		if int64(len(b)) > spaceInSegment {
			tail, tailPos = switchSegment(p.dataPath, tail, os.O_WRONLY|os.O_CREATE|os.O_APPEND)
			p.tailPosition.Store(tailPos)
			p.tail.Store(tail)
		}
		lastRec := b[lastRecOffset:]
		copy(headMetadata, lastRec[offsetHeadPos:offsetKeyLen])
		if bytes.Equal(headMetadata, zeroHeadMetadata) { // New records has no head - fallback to last committed head
			copy(headMetadata, fallback)
		}
		if _, err := tail.Write(b); err != nil {
			panic(err.Error()) // cannot write to tail
		}
		if err := tail.Sync(); err != nil {
			panic(err.Error()) // cannot sync tail
		}
		writeOffset(headMetadata, filepath.Base(tail.Name()), tailPos+int64(len(b)), p.dataPathOffsetTmp, p.dataPathOffset)
		p.tailPosition.Store(tailPos + int64(len(b)))
		return true
	})
	return ok
}

func (p *Pfirq) nextRecord(buf []byte) (bool, *os.File, int64) {
	head := p.head.Load().(*os.File)
	headPos := p.headPosition.Load().(int64)
	tail := p.tail.Load().(*os.File)
	if head.Name() == tail.Name() {
		tailPos := p.tailPosition.Load().(int64)
		if headPos == tailPos {
			return false, nil, 0
		}
	}
	reader := p.headReader.Load().(*bufio.Reader)
	_, err := io.ReadFull(reader, buf)
	if err == io.EOF {
		head, headPos = switchSegment(p.dataPath, head, os.O_RDONLY|os.O_CREATE)
		reader.Reset(head)
		p.headPosition.Store(headPos)
		p.head.Store(head)
		return p.nextRecord(buf)
	} else if err != nil {
		panic(err.Error()) // Error reading tail record length
	}
	return true, head, headPos
}

func deleteFiles(dir string, minName string, maxName string) {
	min := 0
	max := math.MaxInt32
	if parsed, err := strconv.Atoi(filepath.Base(minName)); err == nil {
		min = parsed
	}
	if parsed, err := strconv.Atoi(filepath.Base(maxName)); err == nil {
		max = parsed
	}
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		panic(err.Error()) // cannot list files in directory
	}
	for _, file := range files {
		segment, err := strconv.Atoi(filepath.Base(file.Name()))
		if err != nil {
			continue
		}
		if segment < min || segment > max {
			if err := os.Remove(path.Join(dir, file.Name())); err != nil {
				panic(err.Error()) // cannot remove segment
			}
		}
	}
}

func writeOffset(headMetadata []byte, tailName string, tailPos int64, destTmp string, dest string) {
	f, err := os.OpenFile(destTmp, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		panic(err.Error()) // cannot create tmp offset file
	}
	data := make([]byte, 32)
	copy(data, headMetadata)
	binary.BigEndian.PutUint64(data[16:], uint64(tailPos))
	copy(data[24:], tailName)
	if _, err := f.Write(data); err != nil {
		panic(err.Error()) // cannot write to offset file
	}
	if err := f.Sync(); err != nil {
		panic(err.Error()) // fsync failed
	}
	if err := f.Close(); err != nil {
		panic(err.Error()) // cannot close offset file
	}
	if err := os.Rename(destTmp, dest); err != nil {
		panic(err.Error()) // cannot rename offset file
	}
}

func readOffset(folder string) (string, int64, string, int64) {
	data, err := ioutil.ReadFile(path.Join(folder, nameOffset))
	if os.IsNotExist(err) {
		return nameFirst, 0, nameFirst, 0
	} else if err != nil {
		panic(err.Error()) // Error reading lock file
	}
	headPos := int64(binary.BigEndian.Uint64(data[:8]))
	head := bytesToString(data[8:16])
	tailPos := int64(binary.BigEndian.Uint64(data[16:24]))
	tail := bytesToString(data[24:32])
	return head, headPos, tail, tailPos
}

// To reduce allocation. Looks unsafe but strings.Builder/String uses the same
func bytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func switchSegment(dir string, f *os.File, flags int) (*os.File, int64) {
	if err := f.Close(); err != nil {
		panic(err.Error()) // error closing head
	}
	n, err := strconv.Atoi(filepath.Base(f.Name()))
	if err != nil {
		panic(err.Error()) // bad file name for segment
	}
	segment := openSegment(path.Join(dir, fmt.Sprintf(nameFormat, n+1)), 0, flags)
	return segment, 0
}

func openSegment(path string, offset int64, flags int) *os.File {
	f, err := os.OpenFile(path, flags, os.ModePerm)
	if err != nil {
		panic(err.Error()) // cannot open segment
	}
	if _, err := f.Seek(offset, 0); err != nil {
		panic(err.Error()) // cannot seek on segment
	}
	return f
}
