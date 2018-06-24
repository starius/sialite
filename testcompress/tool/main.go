package main

import (
	"flag"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/golang/snappy"
	"github.com/starius/sialite/cache"
	"github.com/starius/sialite/testcompress"
)

var (
	input  = flag.String("input", "", "Input dir (output of sialitebuilder)")
	start  = flag.Int("start", 1000000, "Start item index")
	nitems = flag.Int("nitems", 0, "Number of items to process (0=all)")
)

type stat struct {
	lenSource      int
	lenCompressed  int
	compressTime   time.Duration
	decompressTime time.Duration
}

func (s stat) ratio() float64 {
	return float64(s.lenCompressed) / float64(s.lenSource)
}

func main() {
	flag.Parse()
	s, err := cache.NewServer(*input)
	if err != nil {
		log.Fatalf("cache.NewBuilder: %v", err)
	}
	m := make(map[string]stat)
	n := 0
	nMiner := 0
	var data []byte
	for i := *start; i < *start+*nitems || *nitems == 0; i++ {
		item, err := s.GetItem(i)
		if err == cache.ErrTooLargeIndex {
			break
		} else if err != nil {
			log.Fatalf("GetItem: %v.", err)
		}
		if item.Index < item.NumMinerPayouts {
			if len(item.Data) != 53 {
				panic("len(item.Data) is " + string(item.Data))
			}
			nMiner++
			continue
		}
		n++
		data, err := snappy.Decode(data, item.Data)
		if err != nil {
			panic(err)
		}
		for name, f := range testcompress.Algos {
			l, d1, d2 := f(data)
			if l > len(data) {
				l = len(data)
				d2 = 0
			}
			s0 := m[name]
			s0.compressTime += d1
			s0.decompressTime += d2
			s0.lenSource += len(data)
			s0.lenCompressed += l
			m[name] = s0
		}
		if n%10 == 0 {
			fmt.Printf("%10d %10d\r", n, nMiner)
		}
	}
	fmt.Println("")
	if err := s.Close(); err != nil {
		log.Fatalf("s.Close: %v.", err)
	}
	var names []string
	for name := range m {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		return m[names[i]].ratio() < m[names[j]].ratio()
	})
	for _, name := range names {
		s0 := m[name]
		fmt.Printf("%15s %14d %14d   %.4f %15s %15s\n", name, s0.lenCompressed/n, s0.lenSource/n, s0.ratio(), s0.compressTime/time.Duration(n), s0.decompressTime/time.Duration(n))
	}
}
