package pring

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path"
	"runtime"
	"testing"
	"time"
)

func withPfirq(t *testing.T, segment int64, cb func(p *Pfirq, dir string)) {
	t.Helper()
	tmp, err := ioutil.TempDir("", "pfirq")
	if err != nil {
		t.Fatal(err)
	}
	p := New(tmp, segment, segment, segment)
	cb(p, tmp)
	p.Close()
}

func TestStartClose(t *testing.T) {
	tmp, err := ioutil.TempDir("", "")
	if err != nil {
		t.Error(err)
	}
	p := New(tmp, 1024, 1024, 1024)
	p.Write([]byte{0x01}, []byte{0x02})
	p.Close()
	p = New(tmp, 1024, 1024, 1024)
	p.Read(func(key []byte, val []byte) (bool, []byte, []byte) {
		if !bytes.Equal(key, []byte{0x01}) || !bytes.Equal(val, []byte{0x02}) {
			t.Errorf("Bad key/val %v %v", key, val)
		}
		return true, key, val
	})
	p.Close()
}

func TestOffsetReadWrite(t *testing.T) {
	tmp, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	head, headPos, tail, tailPos := readOffset(tmp)
	if head != nameFirst || tail != nameFirst {
		t.Errorf("bad default names: %v/%v", head, tail)
	}
	if headPos != 0 || tailPos != 0 {
		t.Errorf("bad default pos: %v/%v", headPos, tailPos)
	}
	headName := fmt.Sprintf(nameFormat, 11)
	tailName := fmt.Sprintf(nameFormat, 12)
	headMetadata := make([]byte, 16)
	binary.BigEndian.PutUint64(headMetadata, 11)
	copy(headMetadata[8:], headName)
	writeOffset(headMetadata, tailName, 12, path.Join(tmp, nameOffset+".tmp"), path.Join(tmp, nameOffset))
	head, headPos, tail, tailPos = readOffset(tmp)
	if head != headName || headPos != 11 {
		t.Errorf("bad head %v:%v", head, headPos)
	}
	if tail != tailName || tailPos != 12 {
		t.Errorf("bad tail %v:%v", head, headPos)
	}
}

func TestReadNoData(t *testing.T) {
	withPfirq(t, 100, func(p *Pfirq, dir string) {
		events := make(chan string)
		go func() {
			events <- "start"
			read := p.Read(func(key []byte, val []byte) (bool, []byte, []byte) { panic("unreachable") })
			if read {
				t.Error()
			}
			events <- "error"
		}()
		if s := <-events; s != "start" {
			t.Error("unexpected event: " + s)
		}
		p.Close()
		if s := <-events; s != "error" {
			t.Error("unexpected event: " + s)
		}
	})
}

func TestAppendRead(t *testing.T) {
	withPfirq(t, 100, func(p *Pfirq, dir string) {
		k1 := []byte{0x11}
		v1 := []byte{0x12}
		p.Write(k1, v1)
		p.Read(func(key []byte, val []byte) (bool, []byte, []byte) {
			if !bytes.Equal(k1, key) {
				t.Errorf("Bad key %v != %v", k1, key)
			}
			if !bytes.Equal(v1, val) {
				t.Errorf("Bad val %v != %v", v1, val)
			}
			return true, key, val
		})
	})
}

func TestAppendNewSegment(t *testing.T) {
	var dir, tail1 string
	withPfirq(t, 40*2, func(p *Pfirq, d string) {
		dir = d
		k := []byte{0x01}
		v := []byte{0x02}
		_, _, tail1, _ = readOffset(dir)
		p.Write(k, v)
		p.Write(k, v)
		p.Write(k, v)
	})
	_, _, tail2, tail2Pos := readOffset(dir)
	if tail1 == tail2 || tail2Pos != 34 {
		t.Errorf("New segment has to be created, not the same %v", tail1)
	}
}

func TestRecoverAfterCrash(t *testing.T) {
	k1 := []byte{0x01}
	v1 := []byte{0x02}
	k2 := []byte{0x03}
	v2 := []byte{0x04}
	var dir string
	var tail string
	withPfirq(t, 100, func(p *Pfirq, d string) {
		dir = d
		p.Write(k1, v1)
		p.Write(k2, v2)
		_, _, tail, _ = readOffset(d)
	})
	extraSegment := path.Join(dir, fmt.Sprintf(nameFormat, 99))
	if err := ioutil.WriteFile(extraSegment, []byte{0x00}, os.ModePerm); err != nil {
		t.Errorf("cannot create extra segment")
	}
	segment, err := os.OpenFile(path.Join(dir, tail), os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if err != nil {
		t.Fatalf("cannot open first segment")
	}
	if _, err := segment.Write([]byte{0xFD, 0xFE, 0xFF}); err != nil {
		t.Errorf("cannot append data to segment %v", err)
	}
	if err := segment.Close(); err != nil {
		t.Error(err)
	}
	buf := New(dir, 100, 100, 100)
	if _, err := os.Stat(extraSegment); !os.IsNotExist(err) {
		t.Errorf("extra segment has to be removed")
	}
	buf.Read(func(key []byte, val []byte) (bool, []byte, []byte) {
		if !bytes.Equal(key, k1) || !bytes.Equal(val, v1) {
			t.Errorf("Want key %#v, got %#v. Want val %#v, got %#v", k1, key, v1, val)
		}
		return true, key, val
	})
	buf.Read(func(key []byte, val []byte) (bool, []byte, []byte) {
		if !bytes.Equal(key, k2) || !bytes.Equal(val, v2) {
			t.Errorf("Want key %#v, got %#v. Want val %#v, got %#v", k2, key, v2, val)
		}
		return true, key, val
	})
	buf.Read(func(key []byte, val []byte) (bool, []byte, []byte) {
		if !bytes.Equal(key, k1) || !bytes.Equal(val, v1) {
			t.Errorf("Want key %#v, got %#v. Want val %#v, got %#v", k1, key, v1, val)
		}
		return true, key, val
	})
	buf.Close()
}

func TestPfirqSeekOnStart(t *testing.T) {
	var dir string
	withPfirq(t, 100, func(p *Pfirq, d string) {
		dir = d
		p.Write([]byte{0x01}, []byte{0x02})
		p.Write([]byte{0x03}, []byte{0x04})
		p.Read(func(key []byte, val []byte) (bool, []byte, []byte) {
			return true, key, val
		})
	})
	buf := New(dir, 100, 100, 100)
	buf.Read(func(key []byte, val []byte) (bool, []byte, []byte) {
		if !bytes.Equal(key, []byte{0x03}) || !bytes.Equal(val, []byte{0x04}) {
			t.Errorf("Bad key %v, val %v", key, val)
		}
		return true, key, val
	})
	buf.Close()
}

func TestDeleteOldSegments(t *testing.T) {
	var dir string
	var head string
	withPfirq(t, 100, func(p *Pfirq, d string) {
		dir = d
		head, _, _, _ = readOffset(d)
		p.Write([]byte{0x01}, []byte{0x02})
		p.Write([]byte{0x03}, []byte{0x04})
		p.Write([]byte{0x03}, []byte{0x04})
	})
	buf := New(dir, 100, 100, 100)
	buf.Read(func(key []byte, val []byte) (bool, []byte, []byte) { return true, key, val })
	buf.Read(func(key []byte, val []byte) (bool, []byte, []byte) { return true, key, val })
	buf.Read(func(key []byte, val []byte) (bool, []byte, []byte) { return true, key, val })
	buf.Close()
	if _, err := os.Stat(path.Join(dir, head)); !os.IsNotExist(err) {
		t.Errorf("Old segment has to be deleted: %v", err)
	}
}

func totalAlloc() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.TotalAlloc
}

func BenchmarkQueue(b *testing.B) {
	tmp, err := ioutil.TempDir("", "pfirq")
	if err != nil {
		b.Fatal(err)
	}
	p := New(tmp, 50*1024*1024, 10*1024*1024, 90*1024*1024)
	k := make([]byte, 1024)
	v := make([]byte, 1024*1024*450)
	for i := 0; i < 5000; i++ {
		key := k[:rand.Intn(1024)+1]
		val := v[:rand.Intn(1024*450)+1]
		p.Write(key, val)
	}
	b.ResetTimer()
	start := time.Now()
	totalCount := 0
	totalBytes := 0
	memBefore := totalAlloc()
	for i := 0; i < b.N; i++ {
		p.Read(func(key []byte, val []byte) (bool, []byte, []byte) {
			totalCount++
			totalBytes += 2 * (len(key) + len(val))
			return true, key, val
		})
	}
	fmt.Printf("Processed %v items, %vMB (%vMB/sec), Mem alloc %vKB, Time %v, RPS %v\n",
		totalCount,
		totalBytes/1024/1024, totalBytes/1024/1024/int(math.Max(time.Since(start).Seconds(), 1)),
		(totalAlloc()-memBefore)/1024,
		time.Since(start)/1024,
		time.Duration(time.Since(start).Nanoseconds()/int64(totalCount)))
	p.Close()
	if err := os.RemoveAll(tmp); err != nil {
		b.Errorf(err.Error())
	}
}
