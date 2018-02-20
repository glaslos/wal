package wal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
)

const (
	// FULL record
	FULL = iota + 1
	// FIRST part of a record
	FIRST
	// MIDDLE part of a record
	MIDDLE
	// LAST part of a record
	LAST
)

// WAL is a write ahead log
type WAL struct {
	buffer    bytes.Buffer
	pos       int
	blockSize int
}

// Record contains a log message
type Record struct {
	Checksum uint32
	Length   uint16
	Type     uint8
	Data     []uint8
}

// NewWAL create a new write ahead log
func NewWAL() WAL {
	return WAL{
		blockSize: 2 << 15, // 32kb
	}
}

// NewRecord creates a new WAL Record
func NewRecord(data []byte) Record {
	record := Record{Data: data}
	record.len()
	record.crc()
	return record
}

func (r *Record) crc() {
	r.Checksum = crc32.ChecksumIEEE(append(r.Data, r.Type))
}

func (r *Record) len() {
	r.Length = uint16(len(r.Data))
}

// Bytes returns the record in bytes
func (r *Record) Bytes() ([]byte, error) {
	buffer := bytes.Buffer{}
	if err := binary.Write(&buffer, binary.BigEndian, r.Checksum); err != nil {
		return []byte{}, err
	}
	if err := binary.Write(&buffer, binary.BigEndian, r.Length); err != nil {
		return []byte{}, err
	}
	if err := binary.Write(&buffer, binary.BigEndian, r.Type); err != nil {
		return []byte{}, err
	}
	if err := binary.Write(&buffer, binary.BigEndian, r.Data); err != nil {
		return []byte{}, err
	}
	return buffer.Bytes(), nil
}

func (wal *WAL) padBlock(spaceInBlock int) error {
	padding := make([]byte, spaceInBlock)
	n, err := wal.buffer.Write(padding)
	if err != nil {
		return err
	}
	wal.pos += n
	return nil
}

func (wal *WAL) Write(record *Record) error {
	spaceInBlock := wal.blockSize - (wal.pos % wal.blockSize)
	if spaceInBlock < 7 {
		wal.padBlock(spaceInBlock)
		spaceInBlock = wal.blockSize
	}
	data, err := record.Bytes()
	if err != nil {
		return err
	}
	if len(data)+7 > spaceInBlock { // body = len(data) + header
		record.Type = FIRST
	}
	n, err := wal.buffer.Write(data)
	if err != nil {
		return err
	}
	wal.pos += n
	return nil
}

func (wal *WAL) Read(r io.Reader) (Record, error) {
	record := Record{}
	if err := binary.Read(r, binary.BigEndian, &record.Checksum); err != nil {
		return record, err
	}
	if err := binary.Read(r, binary.BigEndian, &record.Length); err != nil {
		return record, err
	}
	if err := binary.Read(r, binary.BigEndian, &record.Type); err != nil {
		return record, err
	}
	data := make([]byte, record.Length)
	if err := binary.Read(r, binary.BigEndian, &data); err != nil {
		return record, err
	}
	record.Data = data
	return record, nil
}
