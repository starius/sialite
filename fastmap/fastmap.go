package fastmap

import (
	"bytes"
	"fmt"
	"io"
	"sort"
)

type Writer struct {
	pageLen, keyLen, valueLen, prefixLen int

	data, index io.Writer

	valuesStart int
	prevKey     []byte
	page        []byte
	prevPage    []byte
	keyStart    int
	valueStart  int
	npages      int
	hasPrevPage bool
}

func New(pageLen, keyLen, valueLen, prefixLen int, data, index io.Writer) (*Writer, error) {
	perPage := pageLen / (keyLen + valueLen)
	valuesStart := perPage * keyLen
	return &Writer{
		pageLen:     pageLen,
		keyLen:      keyLen,
		valueLen:    valueLen,
		prefixLen:   prefixLen,
		data:        data,
		index:       index,
		valuesStart: valuesStart,
		prevKey:     make([]byte, keyLen),
		page:        make([]byte, pageLen),
		prevPage:    make([]byte, pageLen),
		valueStart:  valuesStart,
	}, nil
}

func (w *Writer) Write(rec []byte) (int, error) {
	if len(rec) != w.keyLen+w.valueLen {
		return 0, fmt.Errorf("Wrong write size")
	}
	key := rec[:w.keyLen]
	value := rec[w.keyLen:]
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
			return 0, fmt.Errorf("Prefix is too short")
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
		// Write prefix.
		if n, err := w.index.Write(w.page[:w.prefixLen]); err != nil {
			return 0, err
		} else if n != w.prefixLen {
			return 0, io.ErrShortWrite
		}
		w.npages++
		w.hasPrevPage = false
	}
	if w.keyStart == w.valuesStart {
		w.page, w.prevPage = w.prevPage, w.page
		w.hasPrevPage = true
	}
	return len(rec), nil
}

func (w *Writer) Close() error {
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
	if c, ok := w.data.(io.Closer); ok {
		if err := c.Close(); err != nil {
			return err
		}
	}
	if c, ok := w.index.(io.Closer); ok {
		if err := c.Close(); err != nil {
			return err
		}
	}
	return nil
}

type Map struct {
	npages, pageLen, keyLen, valueLen, prefixLen, perPage, valuesStart int

	data, index []byte
}

func Open(pageLen, keyLen, valueLen int, data, index []byte) (*Map, error) {
	npages := len(data) / pageLen
	if npages*pageLen != len(data) {
		return nil, fmt.Errorf("data length is not divided by pageLen")
	}
	prefixLen := len(index) / npages
	if npages*prefixLen != len(index) {
		return nil, fmt.Errorf("index length is not divided by the number of pages")
	}
	perPage := pageLen / (keyLen + valueLen)
	valuesStart := perPage * keyLen
	return &Map{
		npages:      npages,
		pageLen:     pageLen,
		keyLen:      keyLen,
		valueLen:    valueLen,
		prefixLen:   prefixLen,
		perPage:     perPage,
		valuesStart: valuesStart,
		data:        data,
		index:       index,
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
		candidate := m.index[start : start+m.prefixLen]
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
