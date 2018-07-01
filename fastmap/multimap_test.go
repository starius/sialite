package fastmap

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func TestMultiMap(t *testing.T) {
	type pair struct {
		key    []byte
		values [][]byte
	}
	var pairs []pair
	maxKey := 32
	maxValue := 20
	r := rand.New(rand.NewSource(0))
	for i := 0; i < 100000; i++ {
		var values [][]byte
		nvalues := 1
		if r.Intn(10) == 0 {
			nvalues = 1 + r.Intn(5)
			if r.Intn(10) == 0 {
				nvalues = 1 + r.Intn(100)
			}
		}
		buf := make([]byte, maxKey+nvalues*maxValue)
		for j := range buf {
			buf[j] = byte(r.Intn(256))
		}
		key := buf[:maxKey]
		for j := 0; j < nvalues; j++ {
			start := maxKey + j*maxValue
			value := buf[start : start+maxValue]
			values = append(values, value)
		}
		pairs = append(pairs, pair{key, values})
	}
	sort.Slice(pairs, func(i, j int) bool {
		return bytes.Compare(pairs[i].key, pairs[j].key) == -1
	})
	for i := range pairs {
		if i > 0 && bytes.Equal(pairs[i].key, pairs[i-1].key) {
			t.Fatal("Failed to prepare data for the test: duplicate key.")
		}
	}
	cases := []struct {
		pageLen, keyLen     int
		valueLen, prefixLen int
		offsetLen           int
		lowOffsetLen        bool
		withInliner         bool
	}{
		{4096, 32, 20, 5, 4, false, false},
		{4096, 32, 20, 2, 4, false, false},
		{100, 32, 20, 4, 4, false, false},
		{1024, 32, 20, 5, 4, false, false},
		{8192, 32, 20, 5, 4, false, false},
		{4096, 16, 4, 5, 4, false, false},
		{4096, 15, 4, 5, 4, false, false},
		{4096, 16, 4, 5, 4, false, true},
		{4096, 16, 3, 5, 3, false, true},
		{4096, 5, 10, 2, 4, false, false},
		{4096, 5, 10, 2, 1, true, false},
	}
next:
	for _, c := range cases {
		name := fmt.Sprintf("(%d, %d, %d, %d, %d, %d, data, prefixes, values, %v)", c.pageLen, c.keyLen, c.valueLen, c.prefixLen, c.offsetLen, c.offsetLen, c.withInliner)
		// Build.
		var data, prefixes, values bytes.Buffer
		var w *MultiMapWriter
		var err error
		if c.withInliner {
			if c.valueLen != c.offsetLen {
				t.Errorf("%s: c.valueLen != c.offsetLen", name)
			}
			w, err = NewMultiMapWriter(c.pageLen, c.keyLen, c.valueLen, c.prefixLen, c.offsetLen, 2*c.offsetLen, &data, &prefixes, &values, NewFFOOInliner(c.valueLen))
		} else {
			w, err = NewMultiMapWriter(c.pageLen, c.keyLen, c.valueLen, c.prefixLen, c.offsetLen, c.offsetLen, &data, &prefixes, &values, NoInliner{})
		}
		if err != nil {
			t.Errorf("NewMultiMapWriter%s: %v", name, err)
			continue next
		}
		record := make([]byte, c.keyLen+c.valueLen)
		key := record[:c.keyLen]
		value := record[c.keyLen:]
		var prevKey []byte
		for _, p := range pairs {
			copy(key, p.key)
			if prevKey != nil && bytes.Equal(key, prevKey) {
				t.Fatalf("%s: data has duplicates", name)
			}
			prevKey = p.key[:c.keyLen]
			for _, value1 := range p.values {
				copy(value, value1)
				if bytes.Equal(value, ffff) || bytes.Equal(value, oooo) {
					t.Fatalf("%s: value of all zeros or of all FF", name)
				}
				if n, err := w.Write(record); err != nil {
					if !c.lowOffsetLen || err != ErrLowOffsetLen {
						t.Errorf("%s.Write(): %v", name, err)
					}
					continue next
				} else if n != len(record) {
					t.Errorf("%s.Write(): short write", name)
					continue next
				}
			}
		}
		if c.lowOffsetLen {
			t.Errorf("%s: expected offset to be too short", name)
			continue next
		}
		if err := w.Close(); err != nil {
			t.Errorf("%s.Close(): %v", name, err)
			continue next
		}
		// Check the map.
		var m *MultiMap
		if c.withInliner {
			m, err = OpenMultiMap(c.pageLen, c.keyLen, c.valueLen, c.offsetLen, 2*c.offsetLen, data.Bytes(), prefixes.Bytes(), values.Bytes(), NewFFOOInliner(c.valueLen))
		} else {
			m, err = OpenMultiMap(c.pageLen, c.keyLen, c.valueLen, c.offsetLen, c.offsetLen, data.Bytes(), prefixes.Bytes(), values.Bytes(), NoUninliner{})
		}
		if err != nil {
			t.Errorf("OpenMultiMap%s: %v", name, err)
			continue next
		}
		for _, p := range pairs {
			key := p.key[:c.keyLen]
			batch, err := m.Lookup(key)
			if err != nil {
				t.Errorf("%s.Lookup(%s): %v", name, hex.EncodeToString(key), err)
			} else if len(batch) != len(p.values)*c.valueLen {
				t.Errorf("%s.Lookup(%s): the batch has length %d, want %d", name, hex.EncodeToString(key), len(batch), len(p.values)*c.valueLen)
			} else {
				for j, wantValue0 := range p.values {
					wantValue := wantValue0[:c.valueLen]
					seenValue := batch[j*c.valueLen : (j+1)*c.valueLen]
					if !bytes.Equal(seenValue, wantValue) {
						t.Errorf("%s.Lookup(%s): batch element %d is %s, want %s", name, hex.EncodeToString(key), j, hex.EncodeToString(seenValue), hex.EncodeToString(wantValue))
					}
				}
			}
		}
	}
}

func TestMultiMapFFOORefectsFFAnd00inValues(t *testing.T) {
	errNew := errors.New("NewMultiMapWriter")
	errWrite := errors.New("Write")
	errClose := errors.New("Close")
	f := func(value0 []byte) error {
		var data, prefixes, values bytes.Buffer
		w, err := NewMultiMapWriter(4096, 4, 4, 4, 4, 2*4, &data, &prefixes, &values, NewFFOOInliner(4))
		if err != nil {
			return errNew
		}
		record := make([]byte, 4+4)
		key := record[:4]
		value := record[4:]
		for i := range key {
			key[i] = 0x42
		}
		copy(value, value0)
		if _, err := w.Write(record); err != nil {
			return errWrite
		}
		if err := w.Close(); err != nil {
			return errClose
		}
		return nil
	}
	for _, value := range [][]byte{{0xFF, 0xFF, 0xFF, 0xFF}, {0x00, 0x00, 0x00, 0x00}} {
		if err := f(value); err != errWrite && err != errClose {
			t.Errorf("wanted to get and error about FF/00 in values")
		}
	}
}

func TestMultumapEmpty(t *testing.T) {
	var data, prefixes, values bytes.Buffer
	w, err := NewMultiMapWriter(4096, 4, 4, 4, 4, 2*4, &data, &prefixes, &values, NewFFOOInliner(4))
	if err != nil {
		t.Fatalf("NewMapWriter: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if data.Len() != 0 || prefixes.Len() != 0 || values.Len() != 0 {
		t.Errorf("data.Len() = %d; prefixes.Len() = %d; values.Len() = %d", data.Len(), prefixes.Len(), values.Len())
	}
	// Open the map.
	m, err := OpenMultiMap(4096, 4, 4, 4, 2*4, data.Bytes(), prefixes.Bytes(), values.Bytes(), NewFFOOInliner(4))
	if err != nil {
		t.Errorf("OpenMap: %v", err)
	}
	value, err := m.Lookup([]byte("0123"))
	if err != nil {
		t.Errorf("Lookup: %v", err)
	}
	if value != nil {
		t.Errorf("expected to get 'not found', got %v", value)
	}
}
