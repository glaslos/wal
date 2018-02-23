package wal

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"testing"
)

func validCRC(record *Record) bool {
	return record.Checksum == crc32.ChecksumIEEE(append(record.Data, record.Type))
}

func TestCRC(t *testing.T) {
	r := NewRecord([]uint8{1})
	if r.Checksum != 801444648 {
		t.Fatalf("Expecting checksum %d, got %d", 801444648, r.Checksum)
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
		if !validCRC(&rec) {
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
	r := bytes.NewReader(wal.buffer.Bytes())
	records, err := wal.Read(r)
	if err != nil {
		t.Error(err)
	}
	for _, rec := range records {
		if !validCRC(&rec) {
			t.Errorf("Got invalid CRC %d", rec.Checksum)
		}
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
	wal.blockSize = 10
	record := NewRecord([]byte{1, 2, 3, 4, 5})
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
