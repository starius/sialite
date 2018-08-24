package fastmap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

var (
	ErrLowPrefixLen = fmt.Errorf("Prefix is too short")
)

// File structure:
// pages | prefixes | uint32(npages) | uint32(pageLen) | uint32(keyLen) | uint32(valueLen) | uint32(prefixLen)

const tailLen = 5 * 4

type MapWriter struct {
	pageLen, keyLen, valueLen, prefixLen int

	data     io.Writer
	prefixes []byte

	ffff []byte

	valuesStart int
	prevKey     []byte
	page        []byte
	prevPage    []byte
	keyStart    int
	valueStart  int
	npages      int
	hasPrevPage bool
}

func NewMapWriter(pageLen, keyLen, valueLen, prefixLen int, data io.Writer) (*MapWriter, error) {
	perPage := pageLen / (keyLen + valueLen)
	valuesStart := perPage * keyLen
	ffff := make([]byte, keyLen)
	for i := range ffff {
		ffff[i] = 0xFF
	}
	return &MapWriter{
		pageLen:     pageLen,
		keyLen:      keyLen,
		valueLen:    valueLen,
		prefixLen:   prefixLen,
		data:        data,
		ffff:        ffff,
		valuesStart: valuesStart,
		prevKey:     make([]byte, keyLen),
		page:        make([]byte, pageLen),
		prevPage:    make([]byte, pageLen),
		valueStart:  valuesStart,
	}, nil
}

func (w *MapWriter) Write(rec []byte) (int, error) {
	if len(rec) != w.keyLen+w.valueLen {
		return 0, fmt.Errorf("Wrong write size")
	}
	key := rec[:w.keyLen]
	value := rec[w.keyLen:]
	if bytes.Equal(key, w.ffff) {
		return 0, fmt.Errorf("key has all bytes FF, which is a special value")
	}
	if w.npages != 0 {
		if c := bytes.Compare(w.prevKey, key); c == 0 {
			return 0, fmt.Errorf("Input has duplicates")
		} else if c == 1 {
			return 0, fmt.Errorf("Input is not ordered")
		}
	}
	copy(w.prevKey, key)
	if w.hasPrevPage {
		// Move some records from prevPage to this page to have prefixes
		// different for last of prevPage and first of this page.
		remove := 0
		for k := w.keyStart - w.keyLen; k >= 0; k -= w.keyLen {
			if !bytes.Equal(w.prevPage[k:k+w.prefixLen], key[:w.prefixLen]) {
				break
			}
			remove++
		}
		if remove*w.keyLen == w.keyStart {
			return 0, ErrLowPrefixLen
		}
		start := w.keyStart - remove*w.keyLen
		n1 := copy(w.page, w.prevPage[start:w.keyStart])
		w.keyStart = start
		start = w.valueStart - remove*w.valueLen
		n2 := copy(w.page[w.valuesStart:], w.prevPage[start:w.valueStart])
		w.valueStart = start
		// Fill empty slots with FF.
		for i := w.keyStart; i < w.valuesStart; i++ {
			w.prevPage[i] = 0xFF
		}
		for i := w.valueStart; i < w.pageLen; i++ {
			w.prevPage[i] = 0xFF
		}
		if n, err := w.data.Write(w.prevPage); err != nil {
			return 0, err
		} else if n != w.pageLen {
			return 0, io.ErrShortWrite
		}
		w.keyStart = n1
		w.valueStart = w.valuesStart + n2
	}
	nextKeyStart := w.keyStart + w.keyLen
	nextValueStart := w.valueStart + w.valueLen
	copy(w.page[w.keyStart:nextKeyStart], key)
	copy(w.page[w.valueStart:nextValueStart], value)
	w.keyStart = nextKeyStart
	w.valueStart = nextValueStart
	if w.hasPrevPage || w.npages == 0 {
		// Add prefix.
		w.prefixes = append(w.prefixes, w.page[:w.prefixLen]...)
		w.npages++
		w.hasPrevPage = false
	}
	if w.keyStart == w.valuesStart {
		w.page, w.prevPage = w.prevPage, w.page
		w.hasPrevPage = true
	}
	return len(rec), nil
}

func (w *MapWriter) Close() error {
	if w.hasPrevPage {
		w.page = w.prevPage
	}
	if w.keyStart != 0 {
		// Partial page. Fill empty slots with FF.
		for i := w.keyStart; i < w.valuesStart; i++ {
			w.page[i] = 0xFF
		}
		for i := w.valueStart; i < w.pageLen; i++ {
			w.page[i] = 0xFF
		}
		if n, err := w.data.Write(w.page); err != nil {
			return err
		} else if n != w.pageLen {
			return io.ErrShortWrite
		}
		w.keyStart = 0
	}
	npages := len(w.prefixes) / w.prefixLen
	suffix := w.prefixes
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(npages))
	suffix = append(suffix, buf...)
	binary.LittleEndian.PutUint32(buf, uint32(w.pageLen))
	suffix = append(suffix, buf...)
	binary.LittleEndian.PutUint32(buf, uint32(w.keyLen))
	suffix = append(suffix, buf...)
	binary.LittleEndian.PutUint32(buf, uint32(w.valueLen))
	suffix = append(suffix, buf...)
	binary.LittleEndian.PutUint32(buf, uint32(w.prefixLen))
	suffix = append(suffix, buf...)
	if n, err := w.data.Write(suffix); err != nil {
		return err
	} else if n != len(suffix) {
		return io.ErrShortWrite
	}
	if c, ok := w.data.(io.Closer); ok {
		if err := c.Close(); err != nil {
			return err
		}
	}
	return nil
}

type mapParams struct {
	npages, pageLen, keyLen, valueLen, prefixLen         int
	perPage, valuesStart, dataLen, prefixesLen, totalLen int
}

func parseTail(tail []byte) (p mapParams) {
	if len(tail) != tailLen {
		panic("bad tail length")
	}
	p.npages = int(binary.LittleEndian.Uint32(tail[0:4]))
	p.pageLen = int(binary.LittleEndian.Uint32(tail[4:8]))
	p.keyLen = int(binary.LittleEndian.Uint32(tail[8:12]))
	p.valueLen = int(binary.LittleEndian.Uint32(tail[12:16]))
	p.prefixLen = int(binary.LittleEndian.Uint32(tail[16:20]))
	p.perPage = p.pageLen / (p.keyLen + p.valueLen)
	p.valuesStart = p.perPage * p.keyLen
	p.dataLen = p.npages * p.pageLen
	p.prefixesLen = p.npages * p.prefixLen
	p.totalLen = p.dataLen + p.prefixesLen + tailLen
	return
}

type Map struct {
	npages, pageLen, keyLen, valueLen, prefixLen, perPage, valuesStart int

	data, prefixes []byte
}

func OpenMap(input []byte) (*Map, error) {
	if len(input) < tailLen {
		return nil, fmt.Errorf("input is too short")
	}
	tail := input[len(input)-tailLen:]
	p := parseTail(tail)
	if len(input) != p.totalLen {
		return nil, fmt.Errorf("input has incorrect length %d, want %d", len(input), p.totalLen)
	}
	data := input[:p.dataLen]
	prefixes := input[p.dataLen : p.dataLen+p.prefixesLen]
	return &Map{
		npages:      p.npages,
		pageLen:     p.pageLen,
		keyLen:      p.keyLen,
		valueLen:    p.valueLen,
		prefixLen:   p.prefixLen,
		perPage:     p.perPage,
		valuesStart: p.valuesStart,
		data:        data,
		prefixes:    prefixes,
	}, nil
}

func (m *Map) Lookup(key []byte) ([]byte, error) {
	if len(key) != m.keyLen {
		return nil, fmt.Errorf("Bad keyLen")
	}
	// Find the right page.
	prefix := key[:m.prefixLen]
	ipage := sort.Search(m.npages, func(i int) bool {
		start := i * m.prefixLen
		candidate := m.prefixes[start : start+m.prefixLen]
		return bytes.Compare(candidate, prefix) > 0
	}) - 1
	if ipage == -1 {
		// Not found.
		return nil, nil
	}
	start := ipage * m.pageLen
	page := m.data[start : start+m.pageLen]
	inside := sort.Search(m.perPage, func(i int) bool {
		start := i * m.keyLen
		candidate := page[start : start+m.keyLen]
		return bytes.Compare(candidate, key) >= 0
	})
	if inside == m.perPage {
		// Not found.
		return nil, nil
	}
	start = inside * m.keyLen
	candidate := page[start : start+m.keyLen]
	if !bytes.Equal(key, candidate) {
		// Not found.
		return nil, nil
	}
	start = m.valuesStart + inside*m.valueLen
	value := page[start : start+m.valueLen]
	return value, nil
}

type MapReader struct {
	keyLen, valueLen, valuesStart int

	data       io.ReaderAt
	ffff       []byte
	page       []byte
	pageStart  int
	keyStart   int
	valueStart int
	dataLen    int
	pageLen    int
}

func NewMapReader(size int, data io.ReaderAt) (*MapReader, error) {
	if size < tailLen {
		return nil, fmt.Errorf("input is too short")
	}
	tail := make([]byte, tailLen)
	if n, err := data.ReadAt(tail, int64(size-tailLen)); err != nil {
		return nil, fmt.Errorf("failed to read tail: %v", err)
	} else if n != tailLen {
		return nil, fmt.Errorf("short read")
	}
	p := parseTail(tail)
	if size != p.totalLen {
		return nil, fmt.Errorf("input has incorrect length %d, want %d", size, p.totalLen)
	}
	ffff := make([]byte, p.keyLen)
	for i := range ffff {
		ffff[i] = 0xFF
	}
	return &MapReader{
		keyLen:      p.keyLen,
		valueLen:    p.valueLen,
		valuesStart: p.valuesStart,
		dataLen:     p.dataLen,
		pageLen:     p.pageLen,
		data:        data,
		ffff:        ffff,
		page:        make([]byte, p.pageLen),
		keyStart:    p.valuesStart, // This will cause page read from the first Read.
	}, nil
}

func (r *MapReader) Read(rec []byte) (int, error) {
	if len(rec) != r.keyLen+r.valueLen {
		return 0, fmt.Errorf("Wrong read size")
	}
	key := rec[:r.keyLen]
	value := rec[r.keyLen:]
	maybeKey := r.page[r.keyStart : r.keyStart+r.keyLen]
	if r.keyStart == r.valuesStart || bytes.Equal(maybeKey, r.ffff) {
		if r.pageStart == r.dataLen {
			return 0, io.EOF
		}
		// Read next page.
		if n, err := r.data.ReadAt(r.page, int64(r.pageStart)); err != nil {
			return 0, fmt.Errorf("failed to read from the underlying reader: %v", err)
		} else if n != len(r.page) {
			return 0, fmt.Errorf("short read from the underlying reader (%d != %d)", n, len(r.page))
		}
		r.pageStart += r.pageLen
		r.keyStart = 0
		r.valueStart = r.valuesStart
	}
	// Copy the key.
	nextKey := r.keyStart + r.keyLen
	copy(key, r.page[r.keyStart:nextKey])
	r.keyStart = nextKey
	// Copy the value.
	nextValue := r.valueStart + r.valueLen
	copy(value, r.page[r.valueStart:nextValue])
	r.valueStart = nextValue
	return len(rec), nil
}

func (r *MapReader) Close() error {
	if c, ok := r.data.(io.Closer); ok {
		if err := c.Close(); err != nil {
			return err
		}
	}
	return nil
}
