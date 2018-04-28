package fastmap

import (
	"bytes"
	"fmt"
	"io"
	"sort"
)

func Build(pageLen, keyLen, valueLen, prefixLen int, sortedInput io.Reader, data, index io.Writer) error {
	perPage := pageLen / (keyLen + valueLen)
	valuesStart := perPage * keyLen
	rec := make([]byte, keyLen+valueLen)
	key := rec[:keyLen]
	value := rec[keyLen:]
	prevKey := make([]byte, keyLen)
	page := make([]byte, pageLen)
	prevPage := make([]byte, pageLen)
	hasPrevPage := false
	keyStart := 0
	valueStart := valuesStart
	npages := 0
	for {
		if _, err := io.ReadFull(sortedInput, rec); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if c := bytes.Compare(prevKey, key); c == 0 {
			// Skip duplicats.
			// TODO return error
			continue
		} else if c == 1 {
			return fmt.Errorf("Input is not ordered")
		}
		copy(prevKey, key)
		if hasPrevPage {
			// Move some records from prevPage to this page to have prefixes
			// different for last of prevPage and first of this page.
			remove := 0
			for k := keyStart - keyLen; k >= 0; k -= keyLen {
				if !bytes.Equal(prevPage[k:k+prefixLen], key[:prefixLen]) {
					break
				}
				remove++
			}
			if remove*keyLen == keyStart {
				return fmt.Errorf("Prefix is too short")
			}
			start := keyStart - remove*keyLen
			n1 := copy(page, prevPage[start:keyStart])
			keyStart = start
			start = valueStart - remove*valueLen
			n2 := copy(page[valuesStart:], prevPage[start:valueStart])
			valueStart = start
			// Fill empty slots with FF.
			for i := keyStart; i < valuesStart; i++ {
				prevPage[i] = 0xFF
			}
			for i := valueStart; i < pageLen; i++ {
				prevPage[i] = 0xFF
			}
			if n, err := data.Write(prevPage); err != nil {
				return err
			} else if n != pageLen {
				return io.ErrShortWrite
			}
			keyStart = n1
			valueStart = valuesStart + n2
		}
		nextKeyStart := keyStart + keyLen
		nextValueStart := valueStart + valueLen
		copy(page[keyStart:nextKeyStart], key)
		copy(page[valueStart:nextValueStart], value)
		keyStart = nextKeyStart
		valueStart = nextValueStart
		if hasPrevPage || npages == 0 {
			// Write prefix.
			if n, err := index.Write(page[:prefixLen]); err != nil {
				return err
			} else if n != prefixLen {
				return io.ErrShortWrite
			}
			npages++
			hasPrevPage = false
		}
		if keyStart == valuesStart {
			t := prevPage
			prevPage = page
			page = t
			hasPrevPage = true
		}
	}
	if hasPrevPage {
		page = prevPage
	}
	if keyStart != 0 {
		// Partial page. Fill empty slots with FF.
		for i := keyStart; i < valuesStart; i++ {
			page[i] = 0xFF
		}
		for i := valueStart; i < pageLen; i++ {
			page[i] = 0xFF
		}
		// Page is full.
		if n, err := data.Write(page); err != nil {
			return err
		} else if n != pageLen {
			return io.ErrShortWrite
		}
	}
	return nil
}

type Map struct {
	npages, pageLen, keyLen, valueLen, prefixLen, perPage, valuesStart int

	data, index []byte
}

func Open(pageLen, keyLen, valueLen int, data, index []byte, dataLen, indexLen int) (*Map, error) {
	npages := dataLen / pageLen
	if npages*pageLen != dataLen {
		return nil, fmt.Errorf("dataLen is not divided by pageLen")
	}
	prefixLen := indexLen / npages
	if npages*prefixLen != indexLen {
		return nil, fmt.Errorf("indexLen is not divided by the number of pages")
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
