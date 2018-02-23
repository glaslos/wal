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
	record := Record{Type: FULL, Data: data}
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

// WriteHeader returns the record header in bytes
func (r *Record) WriteHeader(w io.Writer) (int, error) {
	if err := binary.Write(w, binary.BigEndian, r.Checksum); err != nil {
		return 0, err
	}
	if err := binary.Write(w, binary.BigEndian, r.Length); err != nil {
		return 0, err
	}
	if err := binary.Write(w, binary.BigEndian, r.Type); err != nil {
		return 0, err
	}
	return 7, nil
}

// Bytes returns the whole record as Bytes
func (r *Record) Write(w io.Writer) (int, error) {
	if _, err := r.WriteHeader(w); err != nil {
		return 0, err
	}
	return w.Write([]byte(r.Data))
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

func (wal *WAL) spaceInBlock() int {
	return wal.blockSize - (wal.pos % wal.blockSize)
}

func (wal *WAL) fitsCurrentBlock(lenData int) bool {
	return lenData+7 <= wal.spaceInBlock()
}

func (wal *WAL) Write(record *Record) error {
	written := 0
	remaining := len(record.Data) - written
	for remaining > 0 {
		spaceInBlock := wal.spaceInBlock()
		// We pad the block if the header doesn't fit
		if spaceInBlock < 7 {
			wal.padBlock(spaceInBlock)
			continue
		}
		switch record.Type {
		case FULL:
			if !wal.fitsCurrentBlock(remaining) {
				record.Type = FIRST
			}
		case FIRST:
			if !wal.fitsCurrentBlock(remaining) {
				record.Type = MIDDLE
			} else {
				record.Type = LAST
			}
		case MIDDLE:
			if wal.fitsCurrentBlock(remaining) {
				record.Type = LAST
			}
		}
		var newLen int
		if wal.fitsCurrentBlock(remaining) {
			// all remaining data
			newLen = remaining
		} else {
			newLen = wal.spaceInBlock() - 7
		}
		data := record.Data[written : written+newLen]
		record.Length = uint16(newLen)
		record.Checksum = crc32.ChecksumIEEE(append(data, record.Type))
		n, err := record.WriteHeader(&wal.buffer)
		if err != nil {
			return err
		}
		wal.pos += n
		n, err = wal.buffer.Write(data)
		if err != nil {
			return err
		}
		wal.pos += n
		written += n
		remaining = len(record.Data) - written
	}
	return nil
}

func (wal *WAL) curPos(r *bytes.Reader) (int64, error) {
	return r.Seek(0, io.SeekCurrent)
}

func (wal *WAL) Read(r *bytes.Reader) ([]Record, error) {
	records := []Record{}
	record := Record{}
	for {
		buffer := make([]byte, wal.blockSize)
		_, err := r.Read(buffer)
		if err != nil {
			break
		}
		reader := bytes.NewReader(buffer)
		for {
			if err = binary.Read(reader, binary.BigEndian, &record.Checksum); err != nil {
				break
			}
			if record.Checksum == 0 {
				break
			}
			if err = binary.Read(reader, binary.BigEndian, &record.Length); err != nil {
				break
			}
			if err = binary.Read(reader, binary.BigEndian, &record.Type); err != nil {
				break
			}
			data := make([]byte, record.Length) // substracting the length of the type field
			if err = binary.Read(reader, binary.BigEndian, &data); err != nil {
				break
			}
			record.Data = data
			records = append(records, record)
		}
	}
	return records, nil
}
