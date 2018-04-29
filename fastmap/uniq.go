package fastmap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type Uniq struct {
	fm               *Writer
	indices          io.Writer
	keyLen, valueLen int
	fmRecord         []byte
	prevKey          []byte
	offsetBytes      []byte
	values           []byte
	lenBuf           []byte
	offset           uint32

	// TODO should write varints to indices
}

func NewUniq(fm *Writer, indices io.Writer, keyLen, valueLen int) (*Uniq, error) {
	fmRecord := make([]byte, keyLen+valueLen)
	prevKey := fmRecord[:keyLen]
	offsetBytes := fmRecord[keyLen:]
	lenBuf := make([]byte, 4)
	return &Uniq{
		fm:          fm,
		indices:     indices,
		keyLen:      keyLen,
		valueLen:    valueLen,
		fmRecord:    fmRecord,
		prevKey:     prevKey,
		offsetBytes: offsetBytes,
		lenBuf:      lenBuf,
	}, nil
}

func (u *Uniq) dump() error {
	if _, err := u.fm.Write(u.fmRecord); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(u.lenBuf, uint32(len(u.values)/4))
	if n, err := u.indices.Write(u.lenBuf); err != nil {
		return err
	} else if n != len(u.lenBuf) {
		return io.ErrShortWrite
	}
	if n, err := u.indices.Write(u.values); err != nil {
		return err
	} else if n != len(u.values) {
		return io.ErrShortWrite
	}
	u.offset += uint32(len(u.lenBuf) + len(u.values))
	binary.LittleEndian.PutUint32(u.offsetBytes, u.offset)
	u.values = u.values[:0]
	return nil
}

func (u *Uniq) Write(b []byte) (int, error) {
	if len(b) != u.keyLen+u.valueLen {
		return 0, fmt.Errorf("Wrong record len")
	}
	key := b[:u.keyLen]
	value := b[u.keyLen:]
	if len(u.values) == 0 {
		// First record.
		copy(u.prevKey, key)
	} else {
		if bytes.Equal(key, u.prevKey) {
			if bytes.Equal(value, u.values[len(u.values)-u.valueLen:]) {
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
	u.values = append(u.values, value...)
	return len(b), nil
}

func (u *Uniq) Close() error {
	if err := u.dump(); err != nil {
		return err
	}
	if err := u.fm.Close(); err != nil {
		return err
	}
	if c, ok := u.indices.(io.Closer); ok {
		if err := c.Close(); err != nil {
			return err
		}
	}
	return nil
}
