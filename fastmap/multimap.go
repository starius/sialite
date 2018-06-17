package fastmap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

var (
	ErrLowOffsetLen = fmt.Errorf("too large offset; increase offsetLen")
)

type Inliner interface {
	Inline(container, values, offset []byte) (bool, error)
}

type Uninliner interface {
	Uninline(container []byte) (bool, []byte, error)
}

type NoInliner struct {
}

func (n NoInliner) Inline(container, values, offset []byte) (bool, error) {
	copy(container, offset)
	return false, nil
}

type NoUninliner struct {
}

func (n NoUninliner) Uninline(container []byte) (bool, []byte, error) {
	return false, container, nil
}

var (
	oooo = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	ffff = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
)

type FFOOInliner struct {
	valueLen int // offsetLen = valueLen <= 4.
}

func NewFFOOInliner(valueLen int) *FFOOInliner {
	if valueLen < 1 || valueLen > 4 {
		panic("bad valueLen")
	}
	return &FFOOInliner{valueLen}
}

func (a *FFOOInliner) Inline(container, values, offset []byte) (bool, error) {
	for start := 0; start < len(values); start += a.valueLen {
		stop := start + a.valueLen
		value := values[start:stop]
		if bytes.Equal(value, ffff[:a.valueLen]) || bytes.Equal(value, oooo[:a.valueLen]) {
			return false, fmt.Errorf("Inline was called with value %v", value)
		}
	}
	if len(values) == a.valueLen {
		copy(container[:a.valueLen], ffff)
		copy(container[a.valueLen:], values)
		return true, nil
	} else if len(values) == 2*a.valueLen {
		copy(container, values)
		return true, nil
	} else {
		copy(container[:a.valueLen], oooo)
		copy(container[a.valueLen:], offset)
		return false, nil
	}
}

func (a *FFOOInliner) Uninline(container []byte) (bool, []byte, error) {
	if bytes.Equal(container[:a.valueLen], oooo[:a.valueLen]) {
		return false, container[a.valueLen:], nil
	} else if bytes.Equal(container[:a.valueLen], ffff[:a.valueLen]) {
		return true, container[a.valueLen:], nil
	} else {
		return true, container, nil
	}
}

type MultiMapWriter struct {
	fm               *MapWriter
	values           io.Writer
	keyLen, valueLen int
	fmRecord         []byte
	prevKey          []byte
	container        []byte
	offsetBytes      []byte
	fullOffsetBytes  []byte
	batch            []byte
	lenBuf           []byte
	offset           uint64
	offsetEnd        uint64

	inliner Inliner

	// TODO should write varints to values
}

func NewMultiMapWriter(pageLen, keyLen, valueLen, prefixLen, offsetLen, containerLen int, data, prefixes, values io.Writer, inliner Inliner) (*MultiMapWriter, error) {
	fm, err := NewMapWriter(pageLen, keyLen, containerLen, prefixLen, data, prefixes)
	if err != nil {
		return nil, err
	}
	fmRecord := make([]byte, keyLen+containerLen)
	prevKey := fmRecord[:keyLen]
	container := fmRecord[keyLen:]
	lenBuf := make([]byte, binary.MaxVarintLen64)
	fullOffsetBytes := make([]byte, 8)
	offsetBytes := fullOffsetBytes[:offsetLen]
	offsetEnd := uint64(1 << uint(8*offsetLen))
	return &MultiMapWriter{
		fm:              fm,
		values:          values,
		keyLen:          keyLen,
		valueLen:        valueLen,
		fmRecord:        fmRecord,
		prevKey:         prevKey,
		container:       container,
		offsetBytes:     offsetBytes,
		fullOffsetBytes: fullOffsetBytes,
		lenBuf:          lenBuf,
		offsetEnd:       offsetEnd,
		inliner:         inliner,
	}, nil
}

func (u *MultiMapWriter) dump() error {
	// Try to inline.
	binary.LittleEndian.PutUint64(u.fullOffsetBytes, u.offset)
	isInlined, err := u.inliner.Inline(u.container, u.batch, u.offsetBytes)
	if err != nil {
		return fmt.Errorf("inliner: %v", err)
	} else if isInlined {
		if _, err := u.fm.Write(u.fmRecord); err != nil {
			return err
		}
		u.batch = u.batch[:0]
		return nil
	}
	// No-inline case.
	if _, err := u.fm.Write(u.fmRecord); err != nil {
		return err
	}
	l := binary.PutUvarint(u.lenBuf, uint64(len(u.batch)/u.valueLen))
	if n, err := u.values.Write(u.lenBuf[:l]); err != nil {
		return err
	} else if n != l {
		return io.ErrShortWrite
	}
	if n, err := u.values.Write(u.batch); err != nil {
		return err
	} else if n != len(u.batch) {
		return io.ErrShortWrite
	}
	u.offset += uint64(l + len(u.batch))
	if u.offset > u.offsetEnd {
		return ErrLowOffsetLen
	}
	u.batch = u.batch[:0]
	return nil
}

func (u *MultiMapWriter) Write(b []byte) (int, error) {
	if len(b) != u.keyLen+u.valueLen {
		return 0, fmt.Errorf("Wrong record len (%d != %d+%d)", len(b), u.keyLen, u.valueLen)
	}
	key := b[:u.keyLen]
	value := b[u.keyLen:]
	if len(u.batch) == 0 {
		// First record.
		copy(u.prevKey, key)
	} else {
		if bytes.Equal(key, u.prevKey) {
			if bytes.Equal(value, u.batch[len(u.batch)-u.valueLen:]) {
				// Repeated value - skip.
				return len(b), nil
			}
		} else {
			if err := u.dump(); err != nil {
				return 0, err
			}
			copy(u.prevKey, key)
		}
	}
	u.batch = append(u.batch, value...)
	return len(b), nil
}

func (u *MultiMapWriter) Close() error {
	if err := u.dump(); err != nil {
		return err
	}
	if err := u.fm.Close(); err != nil {
		return err
	}
	if c, ok := u.values.(io.Closer); ok {
		if err := c.Close(); err != nil {
			return err
		}
	}
	return nil
}

type MultiMap struct {
	fm *Map

	values   []byte
	valueLen int

	uninliner Uninliner
}

func OpenMultiMap(pageLen, keyLen, valueLen, offsetLen, containerLen int, data, prefixes, values []byte, uninliner Uninliner) (*MultiMap, error) {
	fm, err := OpenMap(pageLen, keyLen, containerLen, data, prefixes)
	if err != nil {
		return nil, err
	}
	return &MultiMap{
		fm:        fm,
		values:    values,
		valueLen:  valueLen,
		uninliner: uninliner,
	}, nil
}

func (u *MultiMap) Lookup(key []byte) ([]byte, error) {
	container, err := u.fm.Lookup(key)
	if err != nil || container == nil {
		return nil, err
	}
	// Check if it is inlined.
	isInlined, uninlined, err := u.uninliner.Uninline(container)
	if err != nil {
		return nil, fmt.Errorf("uninliner: %v", err)
	} else if isInlined {
		return uninlined, nil
	}
	// No-inline case.
	var fullOffset [8]byte
	fullOffsetBytes := fullOffset[:]
	copy(fullOffsetBytes, uninlined)
	lenPos := int(binary.LittleEndian.Uint64(fullOffsetBytes))
	size0, l := binary.Uvarint(u.values[lenPos:])
	if l <= 0 {
		return nil, fmt.Errorf("Error in database: bad varint at lenPos")
	}
	dataStart := lenPos + l
	dataEnd := dataStart + int(size0)*u.valueLen
	if dataEnd > len(u.values) {
		return nil, fmt.Errorf("Error in database: too large size")
	}
	return u.values[dataStart:dataEnd], nil
}
