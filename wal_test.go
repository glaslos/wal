package wal

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestCRC(t *testing.T) {
	r := NewRecord([]uint8{1})
	if r.Checksum != 2077166632 {
		t.Fatalf("Expecting checksum %d, got %d", 2077166632, r.Checksum)
	}
}

func TestLen(t *testing.T) {
	r := NewRecord([]uint8{1})
	if r.Length != 1 {
		t.Fatalf("Expecting length %d, got %d", 1, r.Length)
	}
}

func TestWrite(t *testing.T) {
	r := NewRecord([]uint8{1})
	wal := NewWAL()
	if err := wal.Write(&r); err != nil {
		t.Error(err)
	}
}

func TestRead(t *testing.T) {
	wal := NewWAL()
	data := []byte{1, 2, 3, 4, 5}
	record := NewRecord(data)
	b := bytes.Buffer{}
	_, err := record.Write(&b)
	if err != nil {
		t.Error(err)
	}
	r := bytes.NewReader(b.Bytes())
	records, err := wal.Read(r)
	if err != nil {
		t.Error(err)
	}
	for _, rec := range records {
		if !rec.Valid() {
			t.Errorf("Got invalid CRC %d", rec.Checksum)
		}
		if int(rec.Length) != len(rec.Data) {
			t.Errorf("Expected len %d got %d", len(rec.Data), rec.Length)
		}
	}
	fmt.Printf("TestRead %+v\n", records)
}

func TestPadding(t *testing.T) {
	wal := NewWAL()
	wal.padBlock(1)
	if wal.pos != 1 {
		t.Errorf("Expected wal.pos %d got %d", 1, wal.pos)
	}
}

func TestFullBlock(t *testing.T) {
	wal := NewWAL()
	wal.blockSize = headerLen + 3
	wal.pos = 5
	record := NewRecord([]byte{1, 2, 3})
	wal.Write(&record)
}

func TestMiddle(t *testing.T) {
	wal := NewWAL()
	wal.blockSize = headerLen + 3
	record := NewRecord([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	if err := wal.Write(&record); err != nil {
		t.Error(err)
	}
}

func TestSplitRecord(t *testing.T) {
	wal := NewWAL()
	wal.blockSize = headerLen + 3
	record := NewRecord([]byte{1, 2, 3})
	wal.Write(&record)
}

func TestRocks(t *testing.T) {
	w := NewWAL()
	r, err := os.Open("data/db/rocks.log")
	if err != nil {
		t.Error(err)
	}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		t.Error(err)
	}
	nr := bytes.NewReader(b)
	rec, err := w.Read(nr)
	if err != nil {
		t.Error(err)
	}
	if !rec[0].Valid() {
		t.Error("checksum not valid")
	}
	fmt.Printf("TestRocks %+v\n", rec)
}

func BenchmarkWrite(b *testing.B) {
	p := make([]byte, 512)
	rand.Read(p)
	r := Record{Data: p}
	wal := NewWAL()
	b.SetBytes(int64(r.Length) + headerLen)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if err := wal.Write(&r); err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkRead(b *testing.B) {
	wal := NewWAL()
	wal.blockSize = headerLen + 5
	record := NewRecord([]byte{1, 2, 3, 4, 5})
	buf := bytes.Buffer{}
	_, err := record.Write(&buf)
	if err != nil {
		b.Error(err)
	}
	r := bytes.NewReader(buf.Bytes())
	b.SetBytes(int64(buf.Len()))
	r.Seek(0, 0)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if _, err := wal.Read(r); err != nil {
			b.Error(err)
		}
		r.Seek(0, 0)
	}
}
