package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"sort"
	"time"

	"github.com/starius/sialite/testcompress"
)

var (
	input = flag.String("input", "", "Input file")
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
	data, err := ioutil.ReadFile(*input)
	if err != nil {
		panic(err)
	}
	m := make(map[string]stat)
	n := 1
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
