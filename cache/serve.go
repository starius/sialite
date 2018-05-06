package cache

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"syscall"

	"github.com/starius/sialite/fastmap"
)

const (
	MAX_HISTORY_SIZE = 2
)

type Server struct {
	Blockchain     []byte
	Offsets        []byte
	BlockLocations []byte

	AddressesFastmapData     []byte
	AddressesFastmapPrefixes []byte
	AddressesIndices         []byte
	addressFastmap           *fastmap.Map

	nblocks, nitems int
}

func NewServer(dir string) (*Server, error) {
	s := &Server{}
	v := reflect.ValueOf(s).Elem()
	st := v.Type()
	// Mmap all []byte fileds from files.
	for i := 0; i < st.NumField(); i++ {
		ft := st.Field(i)
		if ft.Type == reflect.TypeOf([]byte{}) {
			name := strings.ToLower(ft.Name[:1]) + ft.Name[1:]
			f, err := os.Open(path.Join(dir, name))
			if err != nil {
				return nil, err
			}
			defer f.Close()
			stat, err := f.Stat()
			if err != nil {
				return nil, err
			}
			buf, err := syscall.Mmap(int(f.Fd()), 0, int(stat.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
			if err != nil {
				return nil, err
			}
			v.Field(i).SetBytes(buf)
		}
	}
	addressFastmap, err := fastmap.Open(4096, 32, 4, s.AddressesFastmapData, s.AddressesFastmapPrefixes)
	if err != nil {
		return nil, err
	}
	s.addressFastmap = addressFastmap
	s.nblocks = len(s.BlockLocations) / 8
	if s.nblocks*8 != len(s.BlockLocations) {
		return nil, fmt.Errorf("Bad length of blockLocations")
	}
	s.nitems = len(s.Offsets) / 8
	if s.nitems*8 != len(s.Offsets) {
		return nil, fmt.Errorf("Bad length of offsets")
	}
	runtime.SetFinalizer(s, (*Server).Close)
	return s, nil
}

func (s *Server) Close() error {
	v := reflect.ValueOf(s).Elem()
	st := v.Type()
	for i := 0; i < st.NumField(); i++ {
		ft := st.Field(i)
		if ft.Type == reflect.TypeOf([]byte{}) {
			buf := v.Field(i).Interface().([]byte)
			if err := syscall.Munmap(buf); err != nil {
				return err
			}
		}
	}
	return nil
}

const (
	MINER_PAYOUT = 0
	TRANSACTION  = 1
)

type Item struct {
	Data  []byte
	Block int
	Index int
	Type  int
	// TODO: Merkle proof
}

func (s *Server) GetHistory(address []byte, start string) (history []Item, next string, err error) {
	indexBytes, err := s.addressFastmap.Lookup(address)
	if err != nil || indexBytes == nil {
		return nil, "", err
	}
	lenPos := int(binary.LittleEndian.Uint32(indexBytes))
	size0, l := binary.Uvarint(s.AddressesIndices[lenPos:])
	if l <= 0 {
		return nil, "", fmt.Errorf("Error in database: bad varint at lenPos")
	}
	indexPos := lenPos + l
	if indexPos+int(size0)*4 > len(s.AddressesIndices) {
		return nil, "", fmt.Errorf("Error in database: too large size")
	}
	size := int(size0)
	if size > MAX_HISTORY_SIZE {
		size = MAX_HISTORY_SIZE
		// TODO implement "next" logic.
	}
	for i := 0; i < size; i++ {
		indexEnd := indexPos + 4
		itemIndex := int(binary.LittleEndian.Uint32(s.AddressesIndices[indexPos:indexEnd]))
		indexPos = indexEnd
		item, err := s.getItem(itemIndex)
		if err != nil {
			return nil, "", err
		}
		history = append(history, item)
	}
	return history, "", nil
}

func (s *Server) getItem(itemIndex int) (Item, error) {
	if itemIndex >= s.nitems {
		return Item{}, fmt.Errorf("Error in database: too large item index")
	}
	start := itemIndex * 8
	dataStart := int(binary.LittleEndian.Uint64(s.Offsets[start : start+8]))
	dataEnd := len(s.Blockchain)
	if itemIndex != s.nitems-1 {
		dataEnd = int(binary.LittleEndian.Uint64(s.Offsets[start+8 : start+16]))
	}
	data := s.Blockchain[dataStart:dataEnd]
	// Find the block.
	blockIndex := sort.Search(s.nblocks, func(i int) bool {
		payoutsStart, _ := s.getBlockLocation(i)
		return payoutsStart > itemIndex
	}) - 1
	payoutsStart, txsStart := s.getBlockLocation(blockIndex)
	if itemIndex < txsStart {
		return Item{
			Data:  data,
			Block: blockIndex,
			Type:  MINER_PAYOUT,
			Index: itemIndex - payoutsStart,
		}, nil
	} else {
		return Item{
			Data:  data,
			Block: blockIndex,
			Type:  TRANSACTION,
			Index: itemIndex - txsStart,
		}, nil
	}
}

func (s *Server) getBlockLocation(index int) (int, int) {
	p := index * 8
	payoutsStart := int(binary.LittleEndian.Uint32(s.BlockLocations[p : p+4]))
	txsStart := int(binary.LittleEndian.Uint32(s.AddressesIndices[p+4 : p+8]))
	return payoutsStart, txsStart
}
