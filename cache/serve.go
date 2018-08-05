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
	"strconv"
	"strings"
	"syscall"

	"github.com/golang/snappy"
	"github.com/starius/sialite/fastmap"
	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/merkletree"
)

const (
	MAX_HISTORY_SIZE = 20
)

type Server struct {
	Blockchain     []byte
	Offsets        []byte
	BlockLocations []byte
	LeavesHashes   []byte
	Headers        []byte

	AddressesFastmapData     []byte
	AddressesFastmapPrefixes []byte
	AddressesIndices         []byte
	addressMap               *fastmap.MultiMap

	ContractsFastmapData     []byte
	ContractsFastmapPrefixes []byte
	ContractsIndices         []byte
	contractMap              *fastmap.MultiMap

	offsetLen         int
	offsetIndexLen    int
	addressPrefixLen  int
	contractPrefixLen int

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
		offsetLen:         par.OffsetLen,
		offsetIndexLen:    par.OffsetIndexLen,
		addressPrefixLen:  par.AddressPrefixLen,
		contractPrefixLen: par.ContractPrefixLen,
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
			if stat.Size() == 0 {
				continue
			}
			buf, err := syscall.Mmap(int(f.Fd()), 0, int(stat.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
			if err != nil {
				return nil, err
			}
			v.Field(i).SetBytes(buf)
		}
	}
	var addressUninliner fastmap.Uninliner = fastmap.NoUninliner{}
	addressContainerLen := par.OffsetIndexLen
	if par.AddressOffsetLen == par.OffsetIndexLen {
		addressUninliner = fastmap.NewFFOOInliner(par.OffsetIndexLen)
		addressContainerLen = 2 * par.OffsetIndexLen
	}
	addressMap, err := fastmap.OpenMultiMap(par.AddressPageLen, par.AddressPrefixLen, par.OffsetIndexLen, par.AddressOffsetLen, addressContainerLen, s.AddressesFastmapData, s.AddressesFastmapPrefixes, s.AddressesIndices, addressUninliner)
	if err != nil {
		return nil, err
	}
	s.addressMap = addressMap
	var contractUninliner fastmap.Uninliner = fastmap.NoUninliner{}
	contractContainerLen := par.OffsetIndexLen
	if par.ContractOffsetLen == par.OffsetIndexLen {
		contractUninliner = fastmap.NewFFOOInliner(par.OffsetIndexLen)
		contractContainerLen = 2 * par.OffsetIndexLen
	}
	contractMap, err := fastmap.OpenMultiMap(par.ContractPageLen, par.ContractPrefixLen, par.OffsetIndexLen, par.ContractOffsetLen, contractContainerLen, s.ContractsFastmapData, s.ContractsFastmapPrefixes, s.ContractsIndices, contractUninliner)
	if err != nil {
		return nil, err
	}
	s.contractMap = contractMap
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
			if buf == nil {
				continue
			}
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

const (
	NO_COMPRESSION = 0
	SNAPPY         = 1
)

type Item struct {
	Data            []byte
	Compression     int
	Block           int
	Index           int
	NumLeaves       int
	NumMinerPayouts int
	MerkleProof     []byte
}

func (i *Item) SourceData(dst []byte) ([]byte, error) {
	if i.Compression == NO_COMPRESSION {
		return i.Data, nil
	} else if i.Compression == SNAPPY {
		return snappy.Decode(dst, i.Data)
	} else {
		return nil, ErrUnknownCompression
	}
}

func (s *Server) AddressHistory(address []byte, start string) (history []Item, next string, err error) {
	if len(address) != crypto.HashSize {
		return nil, "", fmt.Errorf("size of address: want %d, got %d", crypto.HashSize, len(address))
	}
	addressPrefix := address[:s.addressPrefixLen]
	return s.getHistory(addressPrefix, s.addressMap, start)
}

func (s *Server) ContractHistory(contract []byte, start string) (history []Item, next string, err error) {
	if len(contract) != crypto.HashSize {
		return nil, "", fmt.Errorf("size of contract ID: want %d, got %d", crypto.HashSize, len(contract))
	}
	contractPrefix := contract[:s.contractPrefixLen]
	return s.getHistory(contractPrefix, s.contractMap, start)
}

func (s *Server) getHistory(prefix []byte, m *fastmap.MultiMap, start string) (history []Item, next string, err error) {
	var tmp [8]byte
	tmpBytes := tmp[:]
	tmpBytesSuffix := tmpBytes[8-s.offsetIndexLen:]
	values, err := m.Lookup(prefix)
	if err != nil || len(values) == 0 {
		return nil, "", err
	}
	size := len(values) / s.offsetIndexLen
	getOffset := func(i int) int {
		begin := i * s.offsetIndexLen
		end := begin + s.offsetIndexLen
		copy(tmpBytesSuffix, values[begin:end])
		return int(binary.BigEndian.Uint64(tmpBytes))
	}
	firstOffset := 0
	if start != "" {
		firstOffset, err = strconv.Atoi(start)
		if err != nil {
			return nil, "", fmt.Errorf("failed parsing 'start': %v", err)
		}
	}
	if firstOffset < 0 || firstOffset > getOffset(size-1) {
		return nil, "", ErrTooLargeIndex
	}
	firstIndex := sort.Search(size, func(i int) bool {
		return getOffset(i) >= firstOffset
	})
	endIndex := firstIndex + MAX_HISTORY_SIZE
	if endIndex > size {
		endIndex = size
	}
	if endIndex == size {
		next = ""
	} else {
		next = strconv.Itoa(getOffset(endIndex-1) + 1)
	}
	for i := firstIndex; i < endIndex; i++ {
		// Value 0 is special on wire, so all indices are shifted.
		item, err := s.GetItem(getOffset(i) - 1)
		if err != nil {
			return nil, "", err
		}
		history = append(history, item)
	}
	return history, next, nil
}

var (
	ErrTooLargeIndex      = fmt.Errorf("Error in database: too large item index")
	ErrUnknownCompression = fmt.Errorf("unknown compression")
)

func (s *Server) GetItem(itemIndex int) (Item, error) {
	var tmp [8]byte
	tmpBytes := tmp[:]
	if itemIndex >= s.nitems {
		return Item{}, ErrTooLargeIndex
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
		payoutsStart := s.getPayoutsStart(i)
		return payoutsStart > itemIndex
	}) - 1
	payoutsStart, txsStart, nleaves := s.getBlockLocation(blockIndex)
	numMinerPayouts := txsStart - payoutsStart
	item := Item{
		Data:            data,
		Block:           blockIndex,
		NumLeaves:       nleaves,
		NumMinerPayouts: numMinerPayouts,
		Index:           itemIndex - payoutsStart,
	}
	if itemIndex < txsStart {
		item.Compression = NO_COMPRESSION
	} else {
		item.Compression = SNAPPY
	}
	// Build MerkleProof.
	hstart := payoutsStart * crypto.HashSize
	hstop := hstart + nleaves*crypto.HashSize
	leavesHashes := s.LeavesHashes[hstart:hstop]
	tree := merkletree.NewCachedTree(crypto.NewHash(), 0)
	if err := tree.SetIndex(uint64(item.Index)); err != nil {
		return Item{}, fmt.Errorf("tree.SetIndex(%d): %v", item.Index, err)
	}
	for i := 0; i < nleaves; i++ {
		start := i * crypto.HashSize
		stop := start + crypto.HashSize
		tree.Push(leavesHashes[start:stop])
	}
	_, proofSet, _, _ := tree.Prove(nil)
	proof := make([]byte, 0, len(proofSet)*crypto.HashSize)
	for _, h := range proofSet {
		if len(h) != crypto.HashSize {
			panic("len(h)=" + string(len(h)))
		}
		proof = append(proof, h...)
	}
	item.MerkleProof = proof
	return item, nil
}

func (s *Server) getBlockLocation(index int) (int, int, int) {
	var tmp [8]byte
	tmpBytes := tmp[:]
	p1 := index * (2 * s.offsetIndexLen)
	p2 := p1 + s.offsetIndexLen
	p3 := p2 + s.offsetIndexLen
	p4 := p3 + s.offsetIndexLen
	copy(tmpBytes, s.BlockLocations[p1:p2])
	payoutsStart := int(binary.LittleEndian.Uint64(tmpBytes))
	copy(tmpBytes, s.BlockLocations[p2:p3])
	txsStart := int(binary.LittleEndian.Uint64(tmpBytes))
	nextStart := s.nitems
	if index != s.nblocks-1 {
		copy(tmpBytes, s.BlockLocations[p3:p4])
		nextStart = int(binary.LittleEndian.Uint64(tmpBytes))
	}
	nleaves := nextStart - payoutsStart
	return payoutsStart, txsStart, nleaves
}

func (s *Server) getPayoutsStart(index int) int {
	var tmp [8]byte
	tmpBytes := tmp[:]
	p1 := index * (2 * s.offsetIndexLen)
	p2 := p1 + s.offsetIndexLen
	copy(tmpBytes, s.BlockLocations[p1:p2])
	payoutsStart := int(binary.LittleEndian.Uint64(tmpBytes))
	return payoutsStart
}
