package wal

import (
	"testing"
)

func TestCRC(t *testing.T) {
	r := Record{Type: 1, Data: []uint8{1}}
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
