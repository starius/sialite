package cache

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"

	"github.com/NebulousLabs/Sia/crypto"
	"github.com/NebulousLabs/Sia/encoding"
	"github.com/NebulousLabs/Sia/types"
	"github.com/starius/sialite/emsort"
	"github.com/starius/sialite/fastmap"
)

type countingWriter struct {
	impl *os.File
	n    uint64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.impl.Write(p)
	w.n += uint64(n)
	return n, err
}

func bytesLess(a []byte, b []byte) bool {
	return bytes.Compare(a, b) == -1
}

const addressPrefixLen = 16
const addressRecordSize = addressPrefixLen + 4
const bufferSize = addressRecordSize

type blockHeader struct {
	Nonce      types.BlockNonce
	Timestamp  types.Timestamp
	MerkleRoot crypto.Hash
}

type Builder struct {
	blockchain *countingWriter

	// Series of blockHeader.
	headersFile    *os.File
	headersEncoder *encoding.Encoder

	offsetIndex uint32

	// 8-byte offsets of miner payouts, and txs in blockchain
	offsets *os.File

	// list of pairs (index of first miner payout, index of first tx) in offsets
	// Indices are 4 byte long
	blockLocations *os.File

	// unlockhash(addressPrefixLen bytes) + 4 byte index in offsets
	addresses    emsort.SortedWriter
	addressestmp *os.File

	buf []byte
}

func NewBuilder(dir string, memLimit int) (*Builder, error) {
	if list, err := ioutil.ReadDir(dir); err != nil {
		return nil, err
	} else if len(list) != 0 {
		return nil, fmt.Errorf("Output directory is not empty")
	}

	blockchain, err := os.Create(path.Join(dir, "blockchain"))
	if err != nil {
		return nil, err
	}

	headersFile, err := os.Create(path.Join(dir, "headers"))
	if err != nil {
		return nil, err
	}
	headersEncoder := encoding.NewEncoder(headersFile)

	offsets, err := os.Create(path.Join(dir, "offsets"))
	if err != nil {
		return nil, err
	}

	blockLocations, err := os.Create(path.Join(dir, "blockLocations"))
	if err != nil {
		return nil, err
	}

	addressesFastmapData, err := os.Create(path.Join(dir, "addressesFastmapData"))
	if err != nil {
		return nil, err
	}
	addressesFastmapPrefixes, err := os.Create(path.Join(dir, "addressesFastmapPrefixes"))
	if err != nil {
		return nil, err
	}

	addressesFastmap, err := fastmap.New(4096, addressPrefixLen, 4, 5, addressesFastmapData, addressesFastmapPrefixes)
	if err != nil {
		return nil, err
	}

	addressesIndices, err := os.Create(path.Join(dir, "addressesIndices"))
	if err != nil {
		return nil, err
	}

	addressesUniq, err := fastmap.NewUniq(addressesFastmap, addressesIndices, addressPrefixLen, 4, 4)
	if err != nil {
		return nil, err
	}

	addressestmp, err := os.Create(path.Join(dir, "addresses.tmp"))
	if err != nil {
		return nil, err
	}
	addresses, err := emsort.New(addressesUniq, addressRecordSize, bytesLess, memLimit, addressestmp)
	if err != nil {
		return nil, err
	}

	return &Builder{
		blockchain: &countingWriter{
			impl: blockchain,
		},
		headersFile:    headersFile,
		headersEncoder: headersEncoder,

		offsets:        offsets,
		blockLocations: blockLocations,
		addresses:      addresses,

		addressestmp: addressestmp,

		buf: make([]byte, bufferSize),
	}, nil
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
	offset := s.buf[:8]
	blockLoc := s.buf[:8]
	addressLoc := s.buf[:addressRecordSize]
	addressPrefix := addressLoc[:addressPrefixLen]
	locOfAddress := addressLoc[addressPrefixLen:addressRecordSize]
	writeAddress := func(uh types.UnlockHash) error {
		copy(addressPrefix, uh[:])
		if n, err := s.addresses.Write(addressLoc); err != nil {
			return err
		} else if n != addressRecordSize {
			return io.ErrShortWrite
		}
		return nil
	}
	firstMinerPayout := s.offsetIndex
	// See Block.MarshalSia.
	for _, mp := range block.MinerPayouts {
		binary.LittleEndian.PutUint64(offset, s.blockchain.n)
		if n, err := s.offsets.Write(offset); err != nil {
			return err
		} else if n != 8 {
			return io.ErrShortWrite
		}
		binary.LittleEndian.PutUint32(locOfAddress, s.offsetIndex)
		if err := writeAddress(mp.UnlockHash); err != nil {
			return err
		}
		s.offsetIndex++
		if err := mp.MarshalSia(s.blockchain); err != nil {
			return err
		}
	}
	firstTransaction := s.offsetIndex
	for i, tx := range block.Transactions {
		binary.LittleEndian.PutUint64(offset, s.blockchain.n)
		if n, err := s.offsets.Write(offset); err != nil {
			return err
		} else if n != 8 {
			return io.ErrShortWrite
		}
		binary.LittleEndian.PutUint32(locOfAddress, s.offsetIndex)
		for _, si := range tx.SiacoinInputs {
			if err := writeAddress(si.UnlockConditions.UnlockHash()); err != nil {
				return err
			}
		}
		for _, si := range tx.SiafundInputs {
			if err := writeAddress(si.UnlockConditions.UnlockHash()); err != nil {
				return err
			}
		}
		for _, so := range tx.SiacoinOutputs {
			if err := writeAddress(so.UnlockHash); err != nil {
				return err
			}
		}
		for _, so := range tx.SiafundOutputs {
			if err := writeAddress(so.UnlockHash); err != nil {
				return err
			}
		}
		for _, contract := range tx.FileContracts {
			for _, so := range contract.ValidProofOutputs {
				if err := writeAddress(so.UnlockHash); err != nil {
					return err
				}
			}
			for _, so := range contract.MissedProofOutputs {
				if err := writeAddress(so.UnlockHash); err != nil {
					return err
				}
			}
		}
		for _, rev := range tx.FileContractRevisions {
			for _, so := range rev.NewValidProofOutputs {
				if err := writeAddress(so.UnlockHash); err != nil {
					return err
				}
			}
			for _, so := range rev.NewMissedProofOutputs {
				if err := writeAddress(so.UnlockHash); err != nil {
					return err
				}
			}
		}
		s.offsetIndex++
		if err := block.Transactions[i].MarshalSia(s.blockchain); err != nil {
			return err
		}
	}
	binary.LittleEndian.PutUint32(blockLoc[:4], firstMinerPayout)
	binary.LittleEndian.PutUint32(blockLoc[4:], firstTransaction)
	if n, err := s.blockLocations.Write(blockLoc); err != nil {
		return err
	} else if n != 8 {
		return io.ErrShortWrite
	}
	return nil
}

func (s *Builder) Build() error {
	if err := s.blockchain.impl.Close(); err != nil {
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
	return nil
}
