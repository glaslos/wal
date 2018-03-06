package main

import "github.com/glaslos/wal"

var (
	// VERSION is set by the makefile
	VERSION = "v0.0.0"
	// BUILDDATE is set by the makefile
	BUILDDATE = ""
)

func main() {
	record := wal.NewRecord([]byte{1})
	wal := wal.NewWAL()
	if err := wal.Write(&record); err != nil {
		panic(err)
	}
}
