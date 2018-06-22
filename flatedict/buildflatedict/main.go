package main

import (
	"flag"
	"log"
	"os"

	"github.com/starius/sialite/cache"
	"github.com/starius/sialite/flatedict"
)

var (
	input    = flag.String("input", "", "Input dir (output of sialitebuilder)")
	output   = flag.String("output", "", "Output dir")
	nitems   = flag.Int("nitems", 100000, "Number of items to process")
	dictLen  = flag.Int("dict_len", 32*1024, "Dictionary len")
	tileLen  = flag.Int("tile_len", 4, "Tile len")
	minCount = flag.Int("min_count", 2, "Min count to include")
	memLimit = flag.Int("mem_limit", 64*1024*1024, "Memory limit, bytes")
)

func main() {
	flag.Parse()
	s, err := cache.NewServer(*input)
	if err != nil {
		log.Fatalf("cache.NewBuilder: %v", err)
	}
	fragmentLen := *dictLen
	counterLen := 4
	mapPageLen := 4096
	b, err := flatedict.NewBuilder(*dictLen, fragmentLen, *tileLen, *memLimit, counterLen, *minCount, mapPageLen, *output)
	if err != nil {
		log.Fatalf("flatedict.NewBuilder: %v", err)
	}
	for i := 0; i < *nitems; i++ {
		item, err := s.GetItem(i)
		if err == cache.ErrTooLargeIndex {
			break
		} else if err != nil {
			log.Fatalf("GetItem: %v.", err)
		}
		if err := b.Add(item.Data); err != nil {
			log.Fatalf("b.Add: %v.", err)
		}
	}
	if err := s.Close(); err != nil {
		log.Fatalf("s.Close: %v.", err)
	}
	if err := b.Close(); err != nil {
		log.Fatalf("b.Close: %v.", err)
	}
	os.Stdout.Write(b.Dict())
}
