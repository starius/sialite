package cache

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path"

	"github.com/NebulousLabs/Sia/crypto"
	"github.com/NebulousLabs/Sia/encoding"
	"github.com/NebulousLabs/Sia/types"
	"github.com/golang/snappy"
	"github.com/starius/sialite/emsort"
	"github.com/starius/sialite/fastmap"
)

type parameters struct {
	OffsetLen      int
	OffsetIndexLen int

	AddressPageLen          int
	AddressPrefixLen        int
	AddressFastmapPrefixLen int
	AddressOffsetLen        int

	ContractPageLen          int
	ContractPrefixLen        int
	ContractFastmapPrefixLen int
	ContractOffsetLen        int
}

type blockHeader struct {
	Nonce      types.BlockNonce
	Timestamp  types.Timestamp
	MerkleRoot crypto.Hash
}

type Builder struct {
	blockchain      *os.File
	blockchainBuf   *bufio.Writer
	blockchainLen   uint64
	dataBuf         bytes.Buffer
	compressedBuf   []byte
	leavesHashes    *os.File
	leavesHashesBuf *bufio.Writer

	siaHash    hash.Hash
	siaHashBuf []byte

	// Series of blockHeader.
	headersFile    *os.File
	headersEncoder *encoding.Encoder

	offsetIndex uint64

	// 8-byte offsets of miner payouts, and txs in blockchain
	offsets *os.File

	// list of pairs (index of first miner payout, index of first tx) in offsets
	// Indices are offsetLen byte long
	blockLocations *os.File

	// unlockhash(addressPrefixLen bytes) + addressOffsetLen byte index in offsets
	addresses    emsort.SortedWriter
	addressestmp *os.File

	// unlockhash(contractPrefixLen bytes) + contractOffsetLen byte index in offsets
	contracts    emsort.SortedWriter
	contractstmp *os.File

	tmpBuf         []byte
	tmpBufSuffix   []byte
	itemOffset     []byte
	addressLoc     []byte
	addressPrefix  []byte
	contractLoc    []byte
	contractPrefix []byte
	blockLoc       []byte
	offsetFull     []byte
	offset         []byte

	offsetEnd uint64

	offsetLen, offsetIndexLen int
	addressRecordSize         int
	contractRecordSize        int
}

func NewBuilder(dir string, memLimit, offsetLen, offsetIndexLen, addressPageLen, addressPrefixLen, addressFastmapPrefixLen, addressOffsetLen, contractPageLen, contractPrefixLen, contractFastmapPrefixLen, contractOffsetLen int) (*Builder, error) {

	addressRecordSize := addressPrefixLen + offsetIndexLen
	contractRecordSize := contractPrefixLen + offsetIndexLen
	maxRecordSize := addressRecordSize
	if contractRecordSize > maxRecordSize {
		maxRecordSize = contractRecordSize
	}
	bufferSize := 8 // Max of used buffers.
	if maxRecordSize > bufferSize {
		bufferSize = maxRecordSize
	}
	buf := make([]byte, bufferSize)
	offsetFull := buf[:8]
	offset := buf[:offsetLen]
	blockLoc := buf[:offsetIndexLen*2]
	record := buf[:maxRecordSize]
	itemOffset := record[len(record)-offsetIndexLen:]
	addressLoc := record[len(record)-addressRecordSize:]
	addressPrefix := addressLoc[:addressPrefixLen]
	contractLoc := record[len(record)-contractRecordSize:]
	contractPrefix := contractLoc[:contractPrefixLen]
	if list, err := ioutil.ReadDir(dir); err != nil {
		return nil, fmt.Errorf("ioutil.ReadDir(%q): %v", dir, err)
	} else if len(list) != 0 {
		return nil, fmt.Errorf("Output directory is not empty")
	}

	p := parameters{
		OffsetLen:                offsetLen,
		OffsetIndexLen:           offsetIndexLen,
		AddressPageLen:           addressPageLen,
		AddressPrefixLen:         addressPrefixLen,
		AddressFastmapPrefixLen:  addressFastmapPrefixLen,
		AddressOffsetLen:         addressOffsetLen,
		ContractPageLen:          contractPageLen,
		ContractPrefixLen:        contractPrefixLen,
		ContractFastmapPrefixLen: contractFastmapPrefixLen,
		ContractOffsetLen:        contractOffsetLen,
	}

	parametersJson, err := os.Create(path.Join(dir, "parameters.json"))
	if err != nil {
		return nil, fmt.Errorf("opening parameters.json: %v", err)
	}
	e := json.NewEncoder(parametersJson)
	e.SetIndent("", "\t")
	if err := e.Encode(p); err != nil {
		return nil, fmt.Errorf("JSON Encode: %v", err)
	}
	if err := parametersJson.Close(); err != nil {
		return nil, fmt.Errorf("JSON Close: %v", err)
	}

	blockchain, err := os.Create(path.Join(dir, "blockchain"))
	if err != nil {
		return nil, fmt.Errorf("opening blockchain: %v", err)
	}

	leavesHashes, err := os.Create(path.Join(dir, "leavesHashes"))
	if err != nil {
		return nil, fmt.Errorf("opening leavesHashes: %v", err)
	}

	headersFile, err := os.Create(path.Join(dir, "headers"))
	if err != nil {
		return nil, fmt.Errorf("opening headers: %v", err)
	}
	headersEncoder := encoding.NewEncoder(headersFile)

	offsets, err := os.Create(path.Join(dir, "offsets"))
	if err != nil {
		return nil, fmt.Errorf("opening offsets: %v", err)
	}

	blockLocations, err := os.Create(path.Join(dir, "blockLocations"))
	if err != nil {
		return nil, fmt.Errorf("opening blockLocations: %v", err)
	}

	addressesFastmapData, err := os.Create(path.Join(dir, "addressesFastmapData"))
	if err != nil {
		return nil, fmt.Errorf("opening addressesFastmapData: %v", err)
	}
	addressesFastmapPrefixes, err := os.Create(path.Join(dir, "addressesFastmapPrefixes"))
	if err != nil {
		return nil, fmt.Errorf("opening addressesFastmapPrefixes: %v", err)
	}
	addressesIndices, err := os.Create(path.Join(dir, "addressesIndices"))
	if err != nil {
		return nil, fmt.Errorf("opening addressesIndices: %v", err)
	}

	contractsFastmapData, err := os.Create(path.Join(dir, "contractsFastmapData"))
	if err != nil {
		return nil, fmt.Errorf("opening contractsFastmapData: %v", err)
	}
	contractsFastmapPrefixes, err := os.Create(path.Join(dir, "contractsFastmapPrefixes"))
	if err != nil {
		return nil, fmt.Errorf("opening contractsFastmapPrefixes: %v", err)
	}
	contractsIndices, err := os.Create(path.Join(dir, "contractsIndices"))
	if err != nil {
		return nil, fmt.Errorf("opening contractsIndices: %v", err)
	}

	var addressInliner fastmap.Inliner = fastmap.NoInliner{}
	addressContainerLen := offsetIndexLen
	if addressOffsetLen == offsetIndexLen {
		addressInliner = fastmap.NewFFOOInliner(offsetIndexLen)
		addressContainerLen = 2 * offsetIndexLen
	}
	addressesMultiMapWriter, err := fastmap.NewMultiMapWriter(addressPageLen, addressPrefixLen, offsetIndexLen, addressFastmapPrefixLen, addressOffsetLen, addressContainerLen, addressesFastmapData, addressesFastmapPrefixes, addressesIndices, addressInliner)
	if err != nil {
		return nil, fmt.Errorf("fastmap.NewMultiMapWriter: %v", err)
	}
	addressestmp, err := os.Create(path.Join(dir, "addresses.tmp"))
	if err != nil {
		return nil, fmt.Errorf("opening addresses.tmp: %v", err)
	}
	addresses, err := emsort.New(addressesMultiMapWriter, addressRecordSize, emsort.BytesLess, false, memLimit, addressestmp)
	if err != nil {
		return nil, fmt.Errorf("emsort.New: %v", err)
	}

	var contractInliner fastmap.Inliner = fastmap.NoInliner{}
	contractContainerLen := offsetIndexLen
	if contractOffsetLen == offsetIndexLen {
		contractInliner = fastmap.NewFFOOInliner(offsetIndexLen)
		contractContainerLen = 2 * offsetIndexLen
	}
	contractsMultiMapWriter, err := fastmap.NewMultiMapWriter(contractPageLen, contractPrefixLen, offsetIndexLen, contractFastmapPrefixLen, contractOffsetLen, contractContainerLen, contractsFastmapData, contractsFastmapPrefixes, contractsIndices, contractInliner)
	if err != nil {
		return nil, fmt.Errorf("fastmap.NewMultiMapWriter: %v", err)
	}
	contractstmp, err := os.Create(path.Join(dir, "contracts.tmp"))
	if err != nil {
		return nil, fmt.Errorf("opening contracts.tmp: %v", err)
	}
	contracts, err := emsort.New(contractsMultiMapWriter, contractRecordSize, emsort.BytesLess, false, memLimit, contractstmp)
	if err != nil {
		return nil, fmt.Errorf("emsort.New: %v", err)
	}

	if offsetLen > 8 {
		return nil, fmt.Errorf("too large offsetLen")
	}

	tmpBuf := make([]byte, 8)

	return &Builder{
		blockchain:      blockchain,
		blockchainBuf:   bufio.NewWriter(blockchain),
		leavesHashes:    leavesHashes,
		leavesHashesBuf: bufio.NewWriter(leavesHashes),
		siaHash:         crypto.NewHash(),

		headersFile:    headersFile,
		headersEncoder: headersEncoder,

		offsets:        offsets,
		blockLocations: blockLocations,
		addresses:      addresses,
		contracts:      contracts,

		addressestmp: addressestmp,
		contractstmp: contractstmp,

		tmpBuf:         tmpBuf,
		tmpBufSuffix:   tmpBuf[len(tmpBuf)-offsetIndexLen:],
		itemOffset:     itemOffset,
		addressLoc:     addressLoc,
		addressPrefix:  addressPrefix,
		contractLoc:    contractLoc,
		contractPrefix: contractPrefix,
		blockLoc:       blockLoc,
		offsetFull:     offsetFull,
		offset:         offset,

		offsetEnd: uint64((1 << uint(8*offsetLen)) - 1),

		offsetLen:          offsetLen,
		offsetIndexLen:     offsetIndexLen,
		addressRecordSize:  addressRecordSize,
		contractRecordSize: contractRecordSize,
	}, nil
}

func (s *Builder) writeAddress(uh types.UnlockHash) error {
	copy(s.addressPrefix, uh[:])
	// This function assumes that index offset is already written to itemOffset.
	if n, err := s.addresses.Write(s.addressLoc); err != nil {
		return err
	} else if n != s.addressRecordSize {
		return io.ErrShortWrite
	}
	return nil
}

func (s *Builder) writeContract(id types.FileContractID) error {
	copy(s.contractPrefix, id[:])
	if n, err := s.contracts.Write(s.contractLoc); err != nil {
		return err
	} else if n != s.contractRecordSize {
		return io.ErrShortWrite
	}
	return nil
}

func (s *Builder) Add(block *types.Block) error {
	header := blockHeader{
		Nonce:      block.Nonce,
		Timestamp:  block.Timestamp,
		MerkleRoot: block.MerkleRoot(),
	}
	if err := s.headersEncoder.Encode(header); err != nil {
		return err
	}
	firstMinerPayout := s.offsetIndex
	// See Block.MarshalSia.
	for _, mp := range block.MinerPayouts {
		binary.LittleEndian.PutUint64(s.offsetFull, s.blockchainLen)
		if n, err := s.offsets.Write(s.offset); err != nil {
			return err
		} else if n != s.offsetLen {
			return io.ErrShortWrite
		}
		wireOffsetIndex := s.offsetIndex + 1 // To avoid special 0 value on wire.
		binary.BigEndian.PutUint64(s.tmpBuf, wireOffsetIndex)
		copy(s.itemOffset, s.tmpBufSuffix)
		if err := s.writeAddress(mp.UnlockHash); err != nil {
			return err
		}
		s.offsetIndex++
		if err := mp.MarshalSia(&s.dataBuf); err != nil {
			return err
		}
		s.siaHash.Reset()
		_, _ = s.siaHash.Write([]byte{0x00})
		_, _ = s.siaHash.Write(s.dataBuf.Bytes())
		s.siaHashBuf = s.siaHash.Sum(s.siaHashBuf[:0])
		if _, err := s.leavesHashesBuf.Write(s.siaHashBuf); err != nil {
			return err
		}
		s.blockchainLen += uint64(s.dataBuf.Len())
		if _, err := s.dataBuf.WriteTo(s.blockchainBuf); err != nil {
			return err
		}
	}
	firstTransaction := s.offsetIndex
	for i, tx := range block.Transactions {
		binary.LittleEndian.PutUint64(s.offsetFull, s.blockchainLen)
		if n, err := s.offsets.Write(s.offset); err != nil {
			return err
		} else if n != s.offsetLen {
			return io.ErrShortWrite
		}
		wireOffsetIndex := s.offsetIndex + 1 // To avoid special 0 value on wire.
		binary.BigEndian.PutUint64(s.tmpBuf, wireOffsetIndex)
		copy(s.itemOffset, s.tmpBufSuffix)
		for _, si := range tx.SiacoinInputs {
			if err := s.writeAddress(si.UnlockConditions.UnlockHash()); err != nil {
				return err
			}
		}
		for _, si := range tx.SiafundInputs {
			if err := s.writeAddress(si.UnlockConditions.UnlockHash()); err != nil {
				return err
			}
			if err := s.writeAddress(si.ClaimUnlockHash); err != nil {
				return err
			}
		}
		for _, so := range tx.SiacoinOutputs {
			if err := s.writeAddress(so.UnlockHash); err != nil {
				return err
			}
		}
		for _, so := range tx.SiafundOutputs {
			if err := s.writeAddress(so.UnlockHash); err != nil {
				return err
			}
		}
		for j, contract := range tx.FileContracts {
			if err := s.writeContract(tx.FileContractID(uint64(j))); err != nil {
				return err
			}
			for _, so := range contract.ValidProofOutputs {
				if err := s.writeAddress(so.UnlockHash); err != nil {
					return err
				}
			}
			for _, so := range contract.MissedProofOutputs {
				if err := s.writeAddress(so.UnlockHash); err != nil {
					return err
				}
			}
		}
		for _, rev := range tx.FileContractRevisions {
			if err := s.writeContract(rev.ParentID); err != nil {
				return err
			}
			for _, so := range rev.NewValidProofOutputs {
				if err := s.writeAddress(so.UnlockHash); err != nil {
					return err
				}
			}
			for _, so := range rev.NewMissedProofOutputs {
				if err := s.writeAddress(so.UnlockHash); err != nil {
					return err
				}
			}
		}
		for _, proof := range tx.StorageProofs {
			if err := s.writeContract(proof.ParentID); err != nil {
				return err
			}
		}
		s.offsetIndex++
		if err := block.Transactions[i].MarshalSia(&s.dataBuf); err != nil {
			return err
		}
		s.siaHash.Reset()
		_, _ = s.siaHash.Write([]byte{0x00})
		_, _ = s.siaHash.Write(s.dataBuf.Bytes())
		s.siaHashBuf = s.siaHash.Sum(s.siaHashBuf[:0])
		if _, err := s.leavesHashesBuf.Write(s.siaHashBuf); err != nil {
			return err
		}
		s.compressedBuf = snappy.Encode(s.compressedBuf, s.dataBuf.Bytes())
		s.dataBuf.Reset()
		s.blockchainLen += uint64(len(s.compressedBuf))
		if _, err := s.blockchainBuf.Write(s.compressedBuf); err != nil {
			return err
		}
	}
	binary.LittleEndian.PutUint64(s.tmpBuf, firstMinerPayout)
	copy(s.blockLoc[:s.offsetIndexLen], s.tmpBuf)
	binary.LittleEndian.PutUint64(s.tmpBuf, firstTransaction)
	copy(s.blockLoc[s.offsetIndexLen:], s.tmpBuf)
	if n, err := s.blockLocations.Write(s.blockLoc); err != nil {
		return err
	} else if n != len(s.blockLoc) {
		return io.ErrShortWrite
	}
	if s.blockchainLen > s.offsetEnd {
		return fmt.Errorf("too large offset (%d > %d); increase offsetLen", s.blockchainLen, s.offsetEnd)
	}
	return nil
}

func (s *Builder) Close() error {
	if err := s.blockchainBuf.Flush(); err != nil {
		return err
	}
	if err := s.blockchain.Close(); err != nil {
		return err
	}
	if err := s.leavesHashesBuf.Flush(); err != nil {
		return err
	}
	if err := s.leavesHashes.Close(); err != nil {
		return err
	}
	if err := s.headersFile.Close(); err != nil {
		return err
	}
	if err := s.offsets.Close(); err != nil {
		return err
	}
	if err := s.blockLocations.Close(); err != nil {
		return err
	}
	if err := s.addresses.Close(); err != nil {
		return err
	}
	if err := s.addressestmp.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.addressestmp.Name()); err != nil {
		return err
	}
	if err := s.contracts.Close(); err != nil {
		return err
	}
	if err := s.contractstmp.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.contractstmp.Name()); err != nil {
		return err
	}
	return nil
}
