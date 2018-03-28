package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"io/ioutil"
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

const headerLen = 7

// WAL is a write ahead log
type WAL struct {
	buffer    *bufio.Writer
	pos       int
	blockSize int
	crcTable  *crc32.Table
}

// Record contains a log message
type Record struct {
	Checksum uint32
	Length   uint16
	Type     uint8
	Data     []byte
}

// RecyclableRecord contains a log message
type RecyclableRecord struct {
	Record
	LogNumber uint32
}

const maskDelta = 0xA282EAD8

func mask(crc uint32) uint32 {
	// Rotate right by 15 bits and add a constant.
	return ((crc >> 15) | (crc << 17)) + maskDelta
}

// Return the crc whose masked representation is masked_crc.
func unmask(crc uint32) uint32 {
	rot := crc - maskDelta
	return (rot >> 17) | (rot << 15)
}

// NewWAL create a new write ahead log
func NewWAL() WAL {
	w, err := ioutil.TempFile("/tmp", "bar")
	if err != nil {
		panic(err)
	}
	return WAL{
		blockSize: 2 << 15, // 32kb
		crcTable:  crc32.MakeTable(0x82f63b78),
		buffer:    bufio.NewWriterSize(w, 2<<15),
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
	r.Checksum = mask(crc32.Checksum(append([]byte{r.Type}, r.Data...), crc32.MakeTable(0x82f63b78)))
}

func (r *Record) len() {
	r.Length = uint16(len(r.Data))
}

// Valid verifies the CRC
func (r *Record) Valid() bool {
	return r.Checksum == mask(crc32.Checksum(append([]byte{r.Type}, r.Data...), crc32.MakeTable(0x82f63b78)))
}

// WriteHeader returns the record header in bytes
func (r *Record) WriteHeader(w io.Writer) (int, error) {
	if err := binary.Write(w, binary.LittleEndian, r.Checksum); err != nil {
		return 0, err
	}
	if err := binary.Write(w, binary.LittleEndian, r.Length); err != nil {
		return 0, err
	}
	if err := binary.Write(w, binary.LittleEndian, r.Type); err != nil {
		return 0, err
	}
	/*if err := binary.Write(w, binary.LittleEndian, r.LogNumber); err != nil {
		return 0, err
	}*/
	return headerLen, nil
}

// Bytes returns the whole record as Bytes
func (r *Record) Write(w io.Writer) (int, error) {
	if _, err := r.WriteHeader(w); err != nil {
		return 0, err
	}
	return w.Write(r.Data)
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
	return lenData+headerLen <= wal.spaceInBlock()
}

func write(record *Record, wal *WAL, data []byte) (int, error) {
	n, err := record.WriteHeader(wal.buffer)
	if err != nil {
		return 0, err
	}
	wal.pos += n
	n, err = wal.buffer.Write(data)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (wal *WAL) Write(record *Record) error {
	written := 0
	remaining := len(record.Data)
	for remaining > 0 {
		// We pad the block if the header doesn't fit
		if wal.spaceInBlock() < headerLen {
			wal.padBlock(wal.spaceInBlock())
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
			newLen = wal.spaceInBlock() - headerLen
		}
		data := record.Data[written : written+newLen]
		record.Length = uint16(newLen)
		record.Checksum = mask(crc32.Checksum(append([]byte{record.Type}, data...), wal.crcTable))
		n, err := write(record, wal, data)
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
			if err = binary.Read(reader, binary.LittleEndian, &record.Checksum); err != nil {
				break
			}
			if record.Checksum == 0 {
				break
			}
			if err = binary.Read(reader, binary.LittleEndian, &record.Length); err != nil {
				break
			}
			if err = binary.Read(reader, binary.LittleEndian, &record.Type); err != nil {
				break
			}
			/*if err = binary.Read(reader, binary.LittleEndian, &record.LogNumber); err != nil {
				break
			}*/
			data := make([]byte, record.Length) // substracting the length of the type field
			if err = binary.Read(reader, binary.LittleEndian, &data); err != nil {
				break
			}
			record.Data = data
			records = append(records, record)
		}
	}
	return records, nil
}
