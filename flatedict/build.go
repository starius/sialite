package flatedict

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/starius/sialite/emsort"
	"github.com/starius/sialite/fastmap"
)

type counter struct {
	mapWriter   *fastmap.MapWriter
	record      []byte
	key         []byte
	counterFull []byte
	tail        []byte
	zeros       []byte
	count       uint64
	minCount    uint64
	callback    func([]byte, uint64)
}

func (c *counter) Write(b []byte) (int, error) {
	if c.count == 0 {
		copy(c.key, b)
		c.count = 1
	} else if bytes.Equal(b, c.key) {
		c.count++
	} else {
		if err := c.dump(); err != nil {
			return 0, err
		}
		copy(c.key, b)
		c.count = 1
	}
	return len(b), nil
}

func (c *counter) Close() error {
	if err := c.dump(); err != nil {
		return err
	}
	if err := c.mapWriter.Close(); err != nil {
		return err
	}
	return nil
}

func (c *counter) dump() error {
	if c.count < c.minCount {
		return nil
	}
	binary.LittleEndian.PutUint64(c.counterFull, c.count)
	if !bytes.Equal(c.tail, c.zeros) {
		return fmt.Errorf("counter overflow: %d", c.count)
	}
	if _, err := c.mapWriter.Write(c.record); err != nil {
		return fmt.Errorf("mapWriter.Write: %v", err)
	}
	c.callback(c.key, c.count)
	return nil
}

type entry struct {
	key   []byte
	count uint64
}

type entryHeap struct {
	entries []*entry
}

func (eh *entryHeap) Len() int {
	return len(eh.entries)
}

func (eh *entryHeap) Less(i, j int) bool {
	return eh.entries[i].count < eh.entries[j].count
}

func (eh *entryHeap) Swap(i, j int) {
	eh.entries[i], eh.entries[j] = eh.entries[j], eh.entries[i]
}

func (eh *entryHeap) Push(x interface{}) {
	eh.entries = append(eh.entries, x.(*entry))
}

func (eh *entryHeap) Pop() interface{} {
	n := len(eh.entries)
	x := eh.entries[n-1]
	eh.entries = eh.entries[:n-1]
	return x
}

type Builder struct {
	dictLen     int
	fragmentLen int
	tileLen     int
	mapPageLen  int
	counterLen  int
	sorted      emsort.SortedWriter
	sortTmp     *os.File
	topKeysHeap *entryHeap
	topKeys     [][]byte
	dict        []byte
	dir         string
}

func NewBuilder(dictLen, fragmentLen, tileLen, memLimit, counterLen, minCount, mapPageLen int, dir string) (*Builder, error) {
	keysNeeded := dictLen
	topKeysHeap := &entryHeap{}
	keyCallback := func(key []byte, count uint64) {
		key1 := make([]byte, tileLen)
		copy(key1, key)
		heap.Push(topKeysHeap, &entry{key1, count})
		if topKeysHeap.Len() == keysNeeded+1 {
			heap.Pop(topKeysHeap)
		}
	}
	data, err := os.Create(filepath.Join(dir, "data"))
	if err != nil {
		return nil, fmt.Errorf("failed to open 'data' file: %v", err)
	}
	prefixes, err := os.Create(filepath.Join(dir, "prefixes"))
	if err != nil {
		return nil, fmt.Errorf("failed to open 'prefixes' file: %v", err)
	}
	mapWriter, err := fastmap.NewMapWriter(mapPageLen, tileLen, counterLen, tileLen, data, prefixes)
	if err != nil {
		return nil, fmt.Errorf("NewMapWriter: %v", err)
	}
	buffer := make([]byte, tileLen+8)
	c := &counter{
		mapWriter:   mapWriter,
		record:      buffer[:tileLen+counterLen],
		key:         buffer[:tileLen],
		counterFull: buffer[tileLen:],
		tail:        buffer[tileLen+counterLen:],
		zeros:       make([]byte, 8-counterLen),
		minCount:    uint64(minCount),
		callback:    keyCallback,
	}
	sortTmp, err := os.Create(filepath.Join(dir, "sort.tmp"))
	if err != nil {
		return nil, fmt.Errorf("failed to open sort.tmp file: %v", err)
	}
	sorted, err := emsort.New(c, tileLen, emsort.BytesLess, false, memLimit, sortTmp)
	if err != nil {
		return nil, fmt.Errorf("emsort.New: %v", err)
	}
	return &Builder{
		dictLen:     dictLen,
		fragmentLen: fragmentLen,
		tileLen:     tileLen,
		mapPageLen:  mapPageLen,
		counterLen:  counterLen,
		sorted:      sorted,
		sortTmp:     sortTmp,
		topKeysHeap: topKeysHeap,
		dir:         dir,
	}, nil
}

func (b *Builder) Add(sample []byte) error {
	if len(sample) > b.fragmentLen {
		sample = sample[:b.fragmentLen]
	}
	for i := 0; i+b.tileLen < len(sample); i++ {
		tile := sample[i : i+b.tileLen]
		if _, err := b.sorted.Write(tile); err != nil {
			return fmt.Errorf("sorted.Write: %v", err)
		}
	}
	return nil
}

func (b *Builder) Close() error {
	if err := b.sorted.Close(); err != nil {
		return fmt.Errorf("sorted.Close: %v", err)
	}
	if err := b.sortTmp.Close(); err != nil {
		return fmt.Errorf("sortTmp.Close: %v", err)
	}
	if err := os.Remove(b.sortTmp.Name()); err != nil {
		return fmt.Errorf("remove(sortTmp): %v", err)
	}
	// Fill topKeys.
	b.topKeys = make([][]byte, b.topKeysHeap.Len())
	topCounts := make([]uint64, b.topKeysHeap.Len())
	for i := len(b.topKeys) - 1; i >= 0; i-- {
		e := heap.Pop(b.topKeysHeap).(*entry)
		b.topKeys[i] = e.key
		topCounts[i] = e.count
	}
	// Fill dict.
	data, err := os.Open(filepath.Join(b.dir, "data"))
	if err != nil {
		return fmt.Errorf("failed to open 'data' file: %v", err)
	}
	defer data.Close()
	stat, err := data.Stat()
	if err != nil {
		return fmt.Errorf("data.Stat: %v", err)
	}
	dataBuf, err := syscall.Mmap(int(data.Fd()), 0, int(stat.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("syscall.Mmap(data): %v", err)
	}
	prefixes, err := os.Open(filepath.Join(b.dir, "prefixes"))
	if err != nil {
		return fmt.Errorf("failed to open 'prefixes' file: %v", err)
	}
	defer prefixes.Close()
	stat, err = prefixes.Stat()
	if err != nil {
		return fmt.Errorf("prefixes.Stat: %v", err)
	}
	prefixesBuf, err := syscall.Mmap(int(prefixes.Fd()), 0, int(stat.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("syscall.Mmap(prefixes): %v", err)
	}
	s, err := fastmap.OpenMap(b.mapPageLen, b.tileLen, b.counterLen, dataBuf, prefixesBuf)
	if err != nil {
		return fmt.Errorf("fastmap.OpenMap: %v", err)
	}
	b.dict = make([]byte, b.dictLen)
	unused := b.dict
	used := make(map[string]struct{})
	const LEFT = 1
	const RIGHT = 2
	bestNeighbour := func(subkey []byte, side int) ([]byte, uint64) {
		buf := make([]byte, len(subkey)+1)
		var p *byte
		if side == LEFT {
			copy(buf[1:], subkey)
			p = &buf[0]
		} else {
			copy(buf, subkey)
			p = &buf[len(buf)-1]
		}
		countBytesFull := make([]byte, 8)
		bestKey := make([]byte, len(buf))
		bestCount := uint64(0)
		for i := 0; i < 256; i++ {
			*p = byte(i)
			if _, has := used[string(buf)]; has {
				continue
			}
			countBytes, err := s.Lookup(buf)
			if err != nil {
				panic(err)
			}
			if countBytes == nil {
				continue
			}
			copy(countBytesFull, countBytes)
			count := binary.LittleEndian.Uint64(countBytesFull)
			if count > bestCount {
				bestCount = count
				copy(bestKey, buf)
			}
		}
		return bestKey, bestCount
	}
	for i, key := range b.topKeys {
		if _, has := used[string(key)]; has {
			continue
		}
		countSum := topCounts[i]
		l := len(key)
		bestWeight := float64(countSum) / float64(l)
		used[string(key)] = struct{}{}
		leftKey := key[:len(key)-1]
		rightKey := key[1:]
		var leftBytes, rightBytes []byte
		for len(leftBytes)+len(key)+len(rightBytes) < len(unused) {
			goon := false
			bestLeftKey, bestLeftCount := bestNeighbour(leftKey, LEFT)
			bestRightKey, bestRightCount := bestNeighbour(rightKey, RIGHT)
			if bestLeftCount > bestRightCount {
				weight := float64(countSum+bestLeftCount) / float64(l+1)
				if weight >= bestWeight {
					l++
					countSum += bestLeftCount
					bestWeight = weight
					leftBytes = append(leftBytes, bestLeftKey[0])
					leftKey = bestLeftKey[:len(bestLeftKey)-1]
					used[string(bestLeftKey)] = struct{}{}
					goon = true
				}
			} else {
				weight := float64(countSum+bestRightCount) / float64(l+1)
				if weight >= bestWeight {
					l++
					countSum += bestRightCount
					bestWeight = weight
					rightBytes = append(rightBytes, bestRightKey[len(bestRightKey)-1])
					rightKey = bestRightKey[1:]
					used[string(bestRightKey)] = struct{}{}
					goon = true
				}
			}
			if !goon {
				break
			}
		}
		longKeyLen := len(leftBytes) + len(key) + len(rightBytes)
		longKey := unused[len(unused)-longKeyLen:]
		for j := 0; j < len(leftBytes); j++ {
			longKey[j] = leftBytes[len(leftBytes)-j-1]
		}
		copy(longKey[len(leftBytes):], key)
		copy(longKey[len(leftBytes)+len(key):], rightBytes)
		unused = unused[:len(unused)-longKeyLen]
		if len(unused) < len(key) {
			break
		}
	}
	// Trim unused prefix, which is too short.
	b.dict = b.dict[len(unused):]
	return nil
}

func (b *Builder) TopKeys() [][]byte {
	return b.topKeys
}

func (b *Builder) Dict() []byte {
	return b.dict
}
