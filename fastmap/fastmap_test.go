package fastmap

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func TestFastmap(t *testing.T) {
	type pair struct {
		key, value []byte
	}
	var pairs []pair
	maxKey := 32
	maxValue := 20
	r := rand.New(rand.NewSource(0))
	for i := 0; i < 100000; i++ {
		buf := make([]byte, maxKey+maxValue)
		key := buf[:maxKey]
		value := buf[maxKey:]
		for j := range buf {
			buf[j] = byte(r.Intn(256))
		}
		pairs = append(pairs, pair{key, value})
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
		lowPrefixLen        bool
	}{
		{4096, 32, 20, 5, false},
		{4096, 32, 20, 2, false},
		{4096, 32, 20, 1, true},
		{100, 32, 20, 4, false},
		{100, 32, 20, 3, true},
		{1024, 32, 20, 5, false},
		{8192, 32, 20, 5, false},
		{4096, 16, 4, 5, false},
		{4096, 15, 4, 5, false},
		{4096, 4, 10, 2, false},
	}
next:
	for _, c := range cases {
		// Build.
		var data, prefixes bytes.Buffer
		w, err := NewMapWriter(c.pageLen, c.keyLen, c.valueLen, c.prefixLen, &data, &prefixes)
		name := fmt.Sprintf("(%d, %d, %d, %d, data, prefixes)", c.pageLen, c.keyLen, c.valueLen, c.prefixLen)
		if err != nil {
			t.Errorf("NewMapWriter%s: %v", name, err)
			continue next
		}
		record := make([]byte, c.keyLen+c.valueLen)
		key := record[:c.keyLen]
		value := record[c.keyLen:]
		var prevKey []byte
		for _, p := range pairs {
			copy(key, p.key)
			copy(value, p.value)
			if prevKey != nil && bytes.Equal(key, prevKey) {
				t.Fatalf("%s: data has duplicates", name)
			}
			prevKey = p.key[:c.keyLen]
			if n, err := w.Write(record); err != nil {
				if !c.lowPrefixLen || err != ErrLowPrefixLen {
					t.Errorf("%s.Write(): %v", name, err)
				}
				continue next
			} else if n != len(record) {
				t.Errorf("%s.Write(): short write", name)
				continue next
			}
		}
		if c.lowPrefixLen {
			t.Errorf("%s: expected prefix to be too short", name)
			continue next
		}
		if err := w.Close(); err != nil {
			t.Errorf("%s.Close(): %v", name, err)
			continue next
		}
		// Check the map.
		m, err := OpenMap(c.pageLen, c.keyLen, c.valueLen, data.Bytes(), prefixes.Bytes())
		if err != nil {
			t.Errorf("Open%s: %v", name, err)
			continue next
		}
		for _, p := range pairs {
			key := p.key[:c.keyLen]
			wantValue := p.value[:c.valueLen]
			seenValue, err := m.Lookup(key)
			if err != nil {
				t.Errorf("%s.Lookup(%s): %v", name, hex.EncodeToString(key), err)
			} else if !bytes.Equal(seenValue, wantValue) {
				t.Errorf("%s.Lookup(%s): returned %s, want %s", name, hex.EncodeToString(key), hex.EncodeToString(seenValue), hex.EncodeToString(wantValue))
			}
		}
	}
}

func TestFastmapRejectsFFKeys(t *testing.T) {
	var data, prefixes bytes.Buffer
	w, err := NewMapWriter(4096, 10, 10, 10, &data, &prefixes)
	if err != nil {
		t.Fatalf("NewMapWriter: %v", err)
	}
	record := make([]byte, 10+10)
	key := record[:10]
	value := record[10:]
	for i := range key {
		key[i] = 0xFF
	}
	for i := range value {
		value[i] = 0x42
	}
	if _, err := w.Write(record); err == nil {
		t.Errorf("Write(): want an error because the key is FFFF")
	}
}

func TestFastmapRejectsUnorderedKeys(t *testing.T) {
	var data, prefixes bytes.Buffer
	w, err := NewMapWriter(4096, 10, 10, 10, &data, &prefixes)
	if err != nil {
		t.Fatalf("NewMapWriter: %v", err)
	}
	record := make([]byte, 10+10)
	key := record[:10]
	value := record[10:]
	for i := range key {
		key[i] = 0x22
	}
	for i := range value {
		value[i] = 0x42
	}
	if _, err := w.Write(record); err != nil {
		t.Fatalf("Write(): %v", err)
	}
	for i := range key {
		key[i] = 0x11
	}
	if _, err := w.Write(record); err == nil {
		t.Errorf("Write(): want an error because keys are not ordered")
	}
}

func TestFastmapRejectsDuplicateKeys(t *testing.T) {
	var data, prefixes bytes.Buffer
	w, err := NewMapWriter(4096, 10, 10, 10, &data, &prefixes)
	if err != nil {
		t.Fatalf("NewMapWriter: %v", err)
	}
	record := make([]byte, 10+10)
	key := record[:10]
	value := record[10:]
	for i := range key {
		key[i] = 0x22
	}
	for i := range value {
		value[i] = 0x42
	}
	if _, err := w.Write(record); err != nil {
		t.Fatalf("Write(): %v", err)
	}
	if _, err := w.Write(record); err == nil {
		t.Errorf("Write(): want an error because keys are duplicates")
	}
}
