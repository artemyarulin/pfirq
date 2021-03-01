package pring

import (
	"bytes"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

/*

T=Tail H=Head L=Length

Init          Write=2       Write=2       Read=4          Write=4

 T=0           T=0           T=0                   T=4             T=4
 |             |             |                     |               |
[0 0 0 0 0]   [1 1 0 0 0]   [1 1 1 1 0]   [0 0 0 0 0]     [1 1 1 0 1]
 |                 |                 |             |           |
 H=0               H=2               H=4           H=4         H=2
 L=0               L=2               L=4           L=0         L=4

*/

type ringbuf struct {
	data []byte
	head *int64
	tail *int64
	buf  bytes.Buffer
	stop *int64
	rm   sync.Mutex // Reader mutex
	wm   sync.Mutex // Writer mutex
	m    sync.Mutex // Struct mutex for modifications
	c    sync.Cond  // Cond variable for reader/writer blocking each other
}

func newRingbuf(size int64) *ringbuf {
	stop := int64(1)
	head := int64(0)
	tail := int64(0)
	return &ringbuf{
		data: make([]byte, size),
		c:    sync.Cond{L: &sync.Mutex{}},
		stop: &stop,
		head: &head,
		tail: &tail,
	}
}

func (r *ringbuf) read(maxSize int64, cb func(b []byte, lastRecOffset int) bool) bool {
	for {
		{
			r.rm.Lock()
			head := atomic.LoadInt64(r.head)
			tail := atomic.LoadInt64(r.tail)
			if head == tail {
				if atomic.LoadInt64(r.stop) == 0 {
					r.rm.Unlock()
					return false
				}
				r.waitUntilChanged()
				r.rm.Unlock()
				continue
			}
		}
		r.m.Lock()
		head := atomic.LoadInt64(r.head)
		tail := atomic.LoadInt64(r.tail)
		start := tail
		end := tail
		recOffset := 0
		for {
			if end == head {
				break // Reached the end
			}
			if end+8 > int64(len(r.data)) {
				break // Overflow
			}
			recLen := int64(binary.BigEndian.Uint64(r.data[end : end+8]))
			if end+recLen > int64(len(r.data)) {
				break // Overflow
			}
			// Even if max size is reached - we allow to return one record anyway
			if end-start+recLen > maxSize && start != end {
				break // Cannot fit
			}
			recOffset = int(end) - int(tail)
			end += recLen
		}
		if start != end { // Happy case - just return reference to mem
			b := r.data[start:end]
			if cb(b, recOffset) {
				atomic.StoreInt64(r.tail, end)
			}
		} else { // Edge case - return at least one record
			r.buf.Reset()
			pos := r.readToBuffer(r.data, int(tail), 8, &r.buf)
			pos = r.readToBuffer(r.data, pos, int(binary.BigEndian.Uint64(r.buf.Bytes()))-8, &r.buf)
			if cb(r.buf.Bytes(), 0) {
				atomic.StoreInt64(r.tail, int64(pos))
			}
		}
		r.m.Unlock()
		r.rm.Unlock()
		r.c.Signal()
		return true
	}
}

func (r *ringbuf) waitUntilChanged() {
	r.c.L.Lock()
	r.c.Wait()
	r.c.L.Unlock()
}

func (r *ringbuf) write(size int, cb func(b []byte)) bool {
	for {
		{
			r.wm.Lock()
			if size < 10 {
				panic("Min size: [8]byte record length + payload")
			}
			if size >= len(r.data) {
				panic("Buffer is too small")
			}
			var p1, p2 []byte
			head := atomic.LoadInt64(r.head)
			tail := atomic.LoadInt64(r.tail)
			if head < tail {
				p1, p2 = r.data[head:tail], nil
			} else {
				p1, p2 = r.data[head:], r.data[:tail]
			}
			if len(p1)+len(p2) <= size { // No space available for data
				if atomic.LoadInt64(r.stop) == 0 {
					r.wm.Unlock()
					return false
				}
				r.waitUntilChanged()
				r.wm.Unlock()
				continue
			}
		}
		r.m.Lock()
		head := atomic.LoadInt64(r.head)
		tail := atomic.LoadInt64(r.tail)
		var p1, p2 []byte
		if head < tail {
			p1, p2 = r.data[head:tail], nil
		} else {
			p1, p2 = r.data[head:], r.data[:tail]
		}
		if len(p1) > size { // Space available in one chunk
			data := p1[:size]
			binary.BigEndian.PutUint64(data, uint64(size))
			cb(data)
			if got := int(binary.BigEndian.Uint64(data)); got != size {
				panic("First 8 bytes should contain record length")
			}
		} else { // Data split, two chunks. Need to copy data after
			r.buf.Reset()
			r.buf.Grow(size)
			r.buf.Write(p1)
			r.buf.Write(p2[:size-len(p1)])
			tmp := r.buf.Bytes()
			binary.BigEndian.PutUint64(tmp, uint64(size))
			cb(tmp)
			if got := int(binary.BigEndian.Uint64(tmp)); got != size {
				panic("First 8 bytes should contain record length")
			}
			copy(p1, tmp)
			copy(p2, tmp[len(p1):])
		}
		head += int64(size)
		if head > int64(len(r.data)) {
			head -= int64(len(r.data))
		}
		atomic.StoreInt64(r.head, head)
		r.m.Unlock()
		r.wm.Unlock()
		r.c.Signal()
		return true
	}
}

func (r *ringbuf) readToBuffer(b []byte, start int, length int, buf *bytes.Buffer) int {
	end := start + length
	if end < len(b) {
		buf.Write(b[start:end])
		return end
	}
	// start + length is bigger than total size: Overflow with two parts
	part1 := b[start:]
	r.buf.Write(part1)
	end = length - len(part1)
	r.buf.Write(b[0:end])
	return end
}

func (r *ringbuf) close() {
	r.m.Lock()
	atomic.StoreInt64(r.stop, 0)
	r.m.Unlock()
	r.c.Signal() // Unblock anything pending
}
