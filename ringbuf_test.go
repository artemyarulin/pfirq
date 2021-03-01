package pring

import (
	"bytes"
	"sync"
	"sync/atomic"
	"testing"
)

func checkBuf(t *testing.T, b *ringbuf, head int, tail int) {
	t.Helper()
	if got := atomic.LoadInt64(b.head); got != int64(head) {
		t.Errorf("bad head %v != %v", got, head)
	}
	if got := atomic.LoadInt64(b.tail); got != int64(tail) {
		t.Errorf("bad tail %v != %v", got, tail)
	}
}

func TestRingbufReadCases(t *testing.T) {
	buf := newRingbuf(96)
	check := func(b []byte, i int, gotOffset int, wantOffset int, m string) {
		t.Helper()
		if len(b) != i || gotOffset != wantOffset {
			t.Errorf("%v. want size %v, got %v. Got offset %v, want %v. Data %v", m, i, len(b), gotOffset, wantOffset, b)
		}
	}
	buf.write(24, func(b []byte) {})
	checkBuf(t, buf, 24, 0)
	buf.write(24, func(b []byte) {})
	checkBuf(t, buf, 48, 0)
	buf.read(0, func(b []byte, offset int) bool {
		check(b, 24, offset, 0, "first record")
		return false
	})
	checkBuf(t, buf, 48, 0)
	buf.read(48, func(b []byte, offset int) bool {
		check(b, 48, offset, 24, "two records")
		return true
	})
	checkBuf(t, buf, 48, 48)
	buf.write(64, func(b []byte) {})
	buf.write(10, func(b []byte) {})
	buf.write(10, func(b []byte) {})
	checkBuf(t, buf, 36, 48)
	buf.read(100, func(b []byte, offset int) bool {
		check(b, 64, offset, 0, "overflow - only one record")
		return true
	})
	checkBuf(t, buf, 36, 16)
	buf.read(100, func(b []byte, offset int) bool {
		check(b, 20, offset, 10, "until the end")
		return true
	})
	checkBuf(t, buf, 36, 36)
}

func TestRingbufMaxSize(t *testing.T) {
	buf := newRingbuf(100)
	// Fill buf with 0 head/tail
	buf.write(99, func(b []byte) {})
	checkBuf(t, buf, 99, 0)
	buf.read(100, func(b []byte, lastRecOffset int) bool { return true })
	checkBuf(t, buf, 99, 99)

	// Fill buf with non 0 head/tail
	buf.write(14, func(b []byte) {})
	checkBuf(t, buf, 13, 99)
	buf.read(0, func(b []byte, lastRecOffset int) bool { return true })
	checkBuf(t, buf, 13, 13)
	buf.write(99, func(b []byte) {})
	checkBuf(t, buf, 12, 13)
	buf.read(0, func(b []byte, lastRecOffset int) bool { return true })
}

func TestRingbufReadWriteData(t *testing.T) {
	buf := newRingbuf(11)
	checkBuf(t, buf, 0, 0)
	buf.write(10, func(b []byte) {
		checkBuf(t, buf, 0, 0)
		if len(b) != 10 {
			t.Errorf("wrong byte length %v", len(b))
		}
		b[8] = 0xAA
		b[9] = 0xAB
	})
	checkBuf(t, buf, 10, 0)
	buf.read(0, func(b []byte, offset int) bool {
		want := []byte{0, 0, 0, 0, 0, 0, 0, 10, 0xAA, 0xAB}
		if !bytes.Equal(b, want) {
			t.Errorf("got unexected %v", b)
		}
		return true
	})
	checkBuf(t, buf, 10, 10)
	// Overflow
	buf.write(10, func(b []byte) {
		b[8] = 0xAC
		b[9] = 0xAD
	})
	checkBuf(t, buf, 9, 10)
	buf.read(0, func(b []byte, offset int) bool {
		want := []byte{0, 0, 0, 0, 0, 0, 0, 10, 0xAC, 0xAD}
		if !bytes.Equal(b, want) {
			t.Errorf("got unexected %v", b)
		}
		return true
	})
	checkBuf(t, buf, 9, 9)
}

func TestRingbufReadWriteMetadata(t *testing.T) {
	buf := newRingbuf(1024)
	check := func(res bool, head int, tail int, msg string) {
		t.Helper()
		if res != true {
			t.Error(msg)
		}
		checkBuf(t, buf, head, tail)
	}
	check(buf.write(1000, func(b []byte) {}), 1000, 0, "enough space")
	check(buf.read(0, func(b []byte, off int) bool { return false }), 1000, 0, "pop data")
	check(buf.read(0, func(b []byte, off int) bool { return true }), 1000, 1000, "read data")
	check(buf.write(1000, func(b []byte) {}), 976, 1000, "overflow")
	check(buf.write(10, func(b []byte) {}), 986, 1000, "add second record")
	check(buf.read(0, func(b []byte, off int) bool { return true }), 986, 976, "read record 1")
	check(buf.read(0, func(b []byte, off int) bool { return true }), 986, 986, "read record 2")
}

func TestRingbufAnySize(t *testing.T) {
	for size := 20; size < 1000; size++ {
		buf := newRingbuf(int64(size))
		for write := 10; write < size; write++ {
			buf.write(write, func(b []byte) {
				b[8] = 0x01
				b[len(b)-1] = 0x02
			})
			buf.read(0, func(b []byte, lastRecOffset int) bool {
				if len(b) != write || b[8] != 0x01 || b[len(b)-1] != 0x02 {
					t.Fatalf("Want %v len, got %v, %v, %v", write, len(b), b[8], b[len(b)-1])
				}
				return true
			})
		}
	}
}

func BenchmarkRingbufSpeed(b *testing.B) {
	b.ReportAllocs()
	buf := newRingbuf(1024 * 1024 * 100)
	wg := sync.WaitGroup{}
	p := 1
	for i := 0; i < p; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < b.N; i++ {
				buf.write(262139, func(data []byte) {
					data[8] = 0x01
					data[len(data)-1] = 0x02
				})
			}
			buf.close()
			wg.Done()
		}()
	}
	for i := 0; i < p; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < b.N; i++ {
				buf.read(262139, func(data []byte, off int) bool {
					if data[8] != 0x01 || data[len(data)-1] != 0x02 {
						b.Fatalf("Bad data: len %v, %v, %v", len(data), data[8], data[len(data)-1])
					}
					b.SetBytes(int64(len(data) * 2))
					return true
				})
			}
			buf.close()
			wg.Done()
		}()
	}
	wg.Wait()
}
