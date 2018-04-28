package fastmap

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
)

func Build(pageLen, keyLen, valueLen int, sortedInput io.Reader, data, index io.Writer, tempfile *os.File) error {
	perPage := pageLen / (keyLen + valueLen)
	valuesStart := perPage * keyLen
	rec := make([]byte, keyLen+valueLen)
	key := rec[:keyLen]
	value := rec[keyLen:]
	prevKey := make([]byte, keyLen)
	page := make([]byte, pageLen)
	keyStart := 0
	valueStart := valuesStart
	npages := 0
	prefixLen := 1
	lastKeyInPage := make([]byte, keyLen)
	for {
		if _, err := io.ReadFull(sortedInput, rec); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if c := bytes.Compare(prevKey, key); c == 0 {
			// Skip duplicats.
			continue
		} else if c == 1 {
			return fmt.Errorf("Input is not ordered")
		}
		copy(prevKey, key)
		if keyStart == 0 {
			if n, err := tempfile.WriteAt(key, int64(npages*keyLen)); err != nil {
				return err
			} else if n != keyLen {
				return io.ErrShortWrite
			}
			if npages != 0 {
				prefixNeeded := 0
				for j := 0; j < keyLen; j++ {
					if lastKeyInPage[j] != key[j] {
						prefixNeeded = j + 1
						break
					}
				}
				if prefixNeeded > prefixLen {
					prefixLen = prefixNeeded
				}
			}
			npages++
		}
		nextKeyStart := keyStart + keyLen
		nextValueStart := valueStart + valueLen
		copy(page[keyStart:nextKeyStart], key)
		copy(page[valueStart:nextValueStart], value)
		keyStart = nextKeyStart
		valueStart = nextValueStart
		if keyStart == valuesStart {
			// Page is full.
			if n, err := data.Write(page); err != nil {
				return err
			} else if n != pageLen {
				return io.ErrShortWrite
			}
			keyStart = 0
			valueStart = valuesStart
			copy(lastKeyInPage, key)
		}
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
	prefix := key[:prefixLen]
	for i := 0; i < npages; i++ {
		if n, err := tempfile.ReadAt(prefix, int64(i*keyLen)); err != nil {
			return err
		} else if n != prefixLen {
			return fmt.Errorf("short read")
		}
		if n, err := index.Write(prefix); err != nil {
			return err
		} else if n != prefixLen {
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
