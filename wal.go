package wal

import "hash/crc32"

// Record contains a log message
type Record struct {
	Checksum uint32
	Length   uint16
	Type     uint8
	Data     []uint8
}

func (r *Record) crc() {
	r.Checksum = crc32.ChecksumIEEE(append(r.Data, r.Type))
}

func (r *Record) len() {
	r.Length = uint16(len(r.Data))
}
