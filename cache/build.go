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

const addressRecordSize = 32 + 4
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

	// unlockhash(32 bytes) + 4 byte index in offsets
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

	addressesFastmap, err := fastmap.New(4096, 32, 4, 5, addressesFastmapData, addressesFastmapPrefixes)
	if err != nil {
		return nil, err
	}

	addressesIndices, err := os.Create(path.Join(dir, "addressesIndices"))
	if err != nil {
		return nil, err
	}

	addressesUniq, err := fastmap.NewUniq(addressesFastmap, addressesIndices, 32, 4)
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
	address := addressLoc[:32]
	locOfAddress := addressLoc[32:addressRecordSize]
	firstMinerPayout := s.offsetIndex
	// See Block.MarshalSia.
	for _, mp := range block.MinerPayouts {
		binary.LittleEndian.PutUint64(offset, s.blockchain.n)
		if n, err := s.offsets.Write(offset); err != nil {
			return err
		} else if n != 8 {
			return io.ErrShortWrite
		}
		copy(address, mp.UnlockHash[:])
		binary.LittleEndian.PutUint32(locOfAddress, s.offsetIndex)
		if n, err := s.addresses.Write(addressLoc); err != nil {
			return err
		} else if n != addressRecordSize {
			return io.ErrShortWrite
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
			uh := si.UnlockConditions.UnlockHash()
			copy(address, uh[:])
			if n, err := s.addresses.Write(addressLoc); err != nil {
				return err
			} else if n != addressRecordSize {
				return io.ErrShortWrite
			}
		}
		for _, si := range tx.SiafundInputs {
			uh := si.UnlockConditions.UnlockHash()
			copy(address, uh[:])
			if n, err := s.addresses.Write(addressLoc); err != nil {
				return err
			} else if n != addressRecordSize {
				return io.ErrShortWrite
			}
		}
		for _, so := range tx.SiacoinOutputs {
			copy(address, so.UnlockHash[:])
			if n, err := s.addresses.Write(addressLoc); err != nil {
				return err
			} else if n != addressRecordSize {
				return io.ErrShortWrite
			}
		}
		for _, so := range tx.SiafundOutputs {
			copy(address, so.UnlockHash[:])
			if n, err := s.addresses.Write(addressLoc); err != nil {
				return err
			} else if n != addressRecordSize {
				return io.ErrShortWrite
			}
		}
		for _, contract := range tx.FileContracts {
			for _, so := range contract.ValidProofOutputs {
				copy(address, so.UnlockHash[:])
				if n, err := s.addresses.Write(addressLoc); err != nil {
					return err
				} else if n != addressRecordSize {
					return io.ErrShortWrite
				}
			}
			for _, so := range contract.MissedProofOutputs {
				copy(address, so.UnlockHash[:])
				if n, err := s.addresses.Write(addressLoc); err != nil {
					return err
				} else if n != addressRecordSize {
					return io.ErrShortWrite
				}
			}
		}
		for _, rev := range tx.FileContractRevisions {
			for _, so := range rev.NewValidProofOutputs {
				copy(address, so.UnlockHash[:])
				if n, err := s.addresses.Write(addressLoc); err != nil {
					return err
				} else if n != addressRecordSize {
					return io.ErrShortWrite
				}
			}
			for _, so := range rev.NewMissedProofOutputs {
				copy(address, so.UnlockHash[:])
				if n, err := s.addresses.Write(addressLoc); err != nil {
					return err
				} else if n != addressRecordSize {
					return io.ErrShortWrite
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
