package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/glaslos/wal"
	"github.com/tecbot/gorocksdb"
)

func make() {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	db, err := gorocksdb.OpenDb(opts, "app/db")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	wo := gorocksdb.NewDefaultWriteOptions()
	err = db.Put(wo, []byte("foo"), []byte("bar"))
	if err != nil {
		panic(err)
	}
}

func main() {
	w := wal.NewWAL()
	r, err := os.Open("app/db/000006.log")
	if err != nil {
		panic(err)
	}
	b, _ := ioutil.ReadAll(r)
	nr := bytes.NewReader(b)
	rec, err := w.Read(nr)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v\n", rec[0])
	println(string(rec[0].Data[:21]))
	rec[0].Data = rec[0].Data[:21]
	if !rec[0].Valid() {
		println("poop")
	}
}
