// Package emsort provides a facility for performing disk-based external
// merge sorts.
//
// see https://en.wikipedia.org/wiki/External_sorting#External_merge_sort
// see http://faculty.simpson.edu/lydia.sinapova/www/cmsc250/LN250_Weiss/L17-ExternalSortEX2.htm
package emsort

import (
	"bufio"
	"bytes"
	"container/heap"
	"fmt"
	"io"
	"os"
	"sort"
)

// SortedWriter is an io.WriteCloser that sorts its output on writing. Each
// []byte passed to the Write method is treated as a single item to sort. Since
// these []byte are kept in memory, they must not be pooled/shared!
type SortedWriter interface {
	Write(b []byte) (int, error)

	// Close implements the method from io.Closer. It's important to call this
	// because this is where the final sorting happens.
	Close() error
}

// Less is a function that compares two byte arrays and determines whether a is
// less than b.
type Less func(a []byte, b []byte) bool

func BytesLess(a []byte, b []byte) bool {
	return bytes.Compare(a, b) == -1
}

// New constructs a new SortedWriter that wraps out, chunks data into sortable
// items using the given chunk size, compares them using the given Less and limits
// the amount of RAM used to approximately memLimit.
func New(out io.Writer, chunkSize int, less Less, memLimit int, tmpfile *os.File) (SortedWriter, error) {
	return &sorted{
		tmpfile:   tmpfile,
		out:       out,
		less:      less,
		memLimit:  memLimit,
		chunkSize: chunkSize,
	}, nil
}

type sorted struct {
	tmpfile   *os.File
	out       io.Writer
	less      Less
	memLimit  int
	chunkSize int
	sizes     []int
	vals      []byte
}

func (s *sorted) Write(b []byte) (int, error) {
	s.vals = append(s.vals, b...)
	if len(s.vals) >= s.memLimit {
		flushErr := s.flush()
		if flushErr != nil {
			return 0, flushErr
		}
	}
	return len(b), nil
}

func (s *sorted) flush() error {
	if len(s.vals)%s.chunkSize != 0 {
		return fmt.Errorf("Writes to emsort should be aligned")
	}
	sort.Sort(&inmemory{s.vals, s.less, s.chunkSize})
	if n, err := s.tmpfile.Write(s.vals); err != nil {
		return err
	} else if n != len(s.vals) {
		return io.ErrShortWrite
	}
	s.sizes = append(s.sizes, len(s.vals))
	s.vals = s.vals[:0]
	return nil
}

func (s *sorted) Close() error {
	if len(s.vals) > 0 {
		flushErr := s.flush()
		if flushErr != nil {
			return flushErr
		}
	}

	// Free memory used by last read vals
	s.vals = nil

	files := make([]*bufio.Reader, len(s.sizes))
	total := 0
	for i, size := range s.sizes {
		file := io.NewSectionReader(s.tmpfile, int64(total), int64(size))
		total += size
		files[i] = bufio.NewReaderSize(file, s.memLimit/len(s.sizes))
	}

	if err := s.finalSort(files); err != nil {
		return err
	}

	switch c := s.out.(type) {
	case io.Closer:
		return c.Close()
	default:
		return nil
	}
}

func (s *sorted) finalSort(files []*bufio.Reader) error {
	if len(files) == 0 {
		return nil
	}
	entries := &entryHeap{
		less:    s.less,
		entries: make([]*entry, len(files)),
	}
	for i, file := range files {
		e := &entry{
			file: file,
			val:  make([]byte, s.chunkSize),
		}
		has, err := e.Read()
		if err != nil {
			return err
		}
		if !has {
			return fmt.Errorf("Unexpected empty file")
		}
		entries.entries[i] = e
	}
	heap.Init(entries)
	for {
		e := heap.Pop(entries).(*entry)
		if n, err := s.out.Write(e.val); err != nil {
			return fmt.Errorf("Error writing to final output: %v", err)
		} else if n != len(e.val) {
			return io.ErrShortWrite
		}
		if has, err := e.Read(); err != nil {
			return err
		} else if has {
			heap.Push(entries, e)
		} else if entries.Len() == 0 {
			break
		}
	}
	return nil
}

type inmemory struct {
	vals      []byte
	less      func(a []byte, b []byte) bool
	chunkSize int
}

func (im *inmemory) Len() int {
	return len(im.vals) / im.chunkSize
}

func (im *inmemory) Less(i, j int) bool {
	iStart := i * im.chunkSize
	jStart := j * im.chunkSize
	return im.less(im.vals[iStart:iStart+im.chunkSize], im.vals[jStart:jStart+im.chunkSize])
}

func (im *inmemory) Swap(i, j int) {
	iStart := i * im.chunkSize
	jStart := j * im.chunkSize
	iSlice := im.vals[iStart : iStart+im.chunkSize]
	jSlice := im.vals[jStart : jStart+im.chunkSize]
	tSlice := make([]byte, im.chunkSize)
	copy(tSlice, iSlice)
	copy(iSlice, jSlice)
	copy(jSlice, tSlice)
}

type entry struct {
	file io.Reader
	val  []byte
}

func (e *entry) Read() (bool, error) {
	_, err := io.ReadFull(e.file, e.val)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

type entryHeap struct {
	entries []*entry
	less    func([]byte, []byte) bool
}

func (eh *entryHeap) Len() int {
	return len(eh.entries)
}

func (eh *entryHeap) Less(i, j int) bool {
	return eh.less(eh.entries[i].val, eh.entries[j].val)
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
