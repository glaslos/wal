package wal

import (
	"bytes"
	"fmt"
	"testing"
)

func TestCRC(t *testing.T) {
	r := Record{Type: FULL, Data: []uint8{1}}
	r.crc()
	if r.Checksum != 801444648 {
		t.Fatalf("Expecting checksum %d, got %d", 801444648, r.Checksum)
	}
}

func TestLen(t *testing.T) {
	r := Record{Data: []uint8{1}}
	r.len()
	if r.Length != 1 {
		t.Fatalf("Expecting length %d, got %d", 1, r.Length)
	}
}

func TestWrite(t *testing.T) {
	r := Record{Data: []uint8{1}}
	wal := NewWAL()
	if err := wal.Write(&r); err != nil {
		t.Error(err)
	}
}

func TestRead(t *testing.T) {
	wal := NewWAL()
	data := []byte{1, 2, 3, 4, 5}
	record := Record{Type: FULL, Data: data}
	record.len()
	record.crc()
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
	wal.blockSize = 10
	wal.pos = 5
	record := NewRecord([]byte{1, 2, 3})
	wal.Write(&record)
}

func TestMiddle(t *testing.T) {
	wal := NewWAL()
	wal.blockSize = 10
	record := NewRecord([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	if err := wal.Write(&record); err != nil {
		t.Error(err)
	}
}

func TestReadSplit(t *testing.T) {
	wal := NewWAL()
	wal.blockSize = 10
	record := NewRecord([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	if err := wal.Write(&record); err != nil {
		t.Error(err)
	}
	records, err := wal.Read(&wal.buffer)
	if err != nil {
		t.Error(err)
	}
	for _, rec := range records {
		if int(rec.Length) != len(rec.Data) {
			t.Errorf("Expected len %d got %d", len(rec.Data), rec.Length)
		}
	}
	fmt.Printf("TestReadSplit %+v\n", records)
}

func TestSplitRecord(t *testing.T) {
	wal := NewWAL()
	wal.blockSize = 8
	record := NewRecord([]byte{1, 2, 3})
	wal.Write(&record)
}

func BenchmarkWrite(b *testing.B) {
	r := Record{Data: []uint8{1}}
	wal := NewWAL()
	for n := 0; n < b.N; n++ {
		if err := wal.Write(&r); err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkRead(b *testing.B) {
	wal := NewWAL()
	record := Record{Type: FULL, Data: []byte{1, 2, 3, 4, 5}}
	record.len()
	record.crc()
	buf := bytes.Buffer{}
	_, err := record.Write(&buf)
	if err != nil {
		b.Error(err)
	}
	r := bytes.NewReader(buf.Bytes())
	r.Seek(0, 0)
	for n := 0; n < b.N; n++ {
		if _, err := wal.Read(r); err != nil {
			b.Error(err)
		}
		r.Seek(0, 0)
	}
}
