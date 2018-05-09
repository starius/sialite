package cache

import (
	"encoding/binary"
	"encoding/json"
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
	addressMap               *fastmap.UniqMap

	offsetLen        int
	offsetIndexLen   int
	addressPrefixLen int

	nblocks, nitems int
}

func NewServer(dir string) (*Server, error) {
	// Read parameters.json.
	jf, err := os.Open(path.Join(dir, "parameters.json"))
	if err != nil {
		return nil, err
	}
	defer jf.Close()
	var par parameters
	if err := json.NewDecoder(jf).Decode(&par); err != nil {
		return nil, err
	}
	s := &Server{
		offsetLen:        par.OffsetLen,
		offsetIndexLen:   par.OffsetIndexLen,
		addressPrefixLen: par.AddressPrefixLen,
	}
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
	addressMap, err := fastmap.OpenUniq(par.AddressPageLen, par.AddressPrefixLen, par.OffsetIndexLen, par.AddressOffsetLen, s.AddressesFastmapData, s.AddressesFastmapPrefixes, s.AddressesIndices)
	if err != nil {
		return nil, err
	}
	s.addressMap = addressMap
	s.nblocks = len(s.BlockLocations) / (2 * par.OffsetIndexLen)
	if s.nblocks*(2*par.OffsetIndexLen) != len(s.BlockLocations) {
		return nil, fmt.Errorf("Bad length of blockLocations")
	}
	s.nitems = len(s.Offsets) / par.OffsetLen
	if s.nitems*par.OffsetLen != len(s.Offsets) {
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
	addressPrefix := address[:s.addressPrefixLen]
	values, err := s.addressMap.Lookup(addressPrefix)
	if err != nil || values == nil {
		return nil, "", err
	}
	size := len(values) / s.offsetIndexLen
	if size > MAX_HISTORY_SIZE {
		size = MAX_HISTORY_SIZE
		// TODO implement "next" logic.
	}
	indexPos := 0
	var tmp [8]byte
	tmpBytes := tmp[:]
	for i := 0; i < size; i++ {
		indexEnd := indexPos + s.offsetIndexLen
		copy(tmpBytes, values[indexPos:indexEnd])
		itemIndex := int(binary.LittleEndian.Uint64(tmpBytes))
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
	var tmp [8]byte
	tmpBytes := tmp[:]
	if itemIndex >= s.nitems {
		return Item{}, fmt.Errorf("Error in database: too large item index")
	}
	start := itemIndex * s.offsetLen
	copy(tmpBytes, s.Offsets[start:start+s.offsetLen])
	dataStart := int(binary.LittleEndian.Uint64(tmpBytes))
	dataEnd := len(s.Blockchain)
	if itemIndex != s.nitems-1 {
		copy(tmpBytes, s.Offsets[start+s.offsetLen:start+2*s.offsetLen])
		dataEnd = int(binary.LittleEndian.Uint64(tmpBytes))
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
	var tmp [8]byte
	tmpBytes := tmp[:]
	p := index * (2 * s.offsetIndexLen)
	copy(tmpBytes, s.BlockLocations[p:p+s.offsetIndexLen])
	payoutsStart := int(binary.LittleEndian.Uint64(tmpBytes))
	copy(tmpBytes, s.BlockLocations[p+s.offsetIndexLen:p+2*s.offsetIndexLen])
	txsStart := int(binary.LittleEndian.Uint64(tmpBytes))
	return payoutsStart, txsStart
}
