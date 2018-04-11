package store

import (
	"os"
	"path"

	"github.com/NebulousLabs/Sia/crypto"
	"github.com/NebulousLabs/Sia/types"
	"github.com/starius/sialite/fs2wrapper/siacoinoutput"
	"github.com/starius/sialite/fs2wrapper/transaction"
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

type blockHeader struct {
	id           types.BlockID
	parentID     types.BlockID
	nonce        types.BlockNonce
	timestamp    types.Timestamp
	minerPayouts []uint64
	transactions []uint64
}

type Storage struct {
	blockchain *countingWriter
	headers    []blockHeader

	id2block map[types.BlockID]int

	siacoinLocations *siacoinoutput.BpTree
	txLocations      *transaction.BpTree
}

func New(dir string) (*Storage, error) {
	blockchainFile := path.Join(dir, "blockchain")
	blockchain, err := os.OpenFile(blockchainFile, os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	siacoinLocationsPath := path.Join(dir, "siacoinLocations")
	siacoinLocations, err := siacoinoutput.NewBpTree(siacoinLocationsPath)
	if err != nil {
		return nil, err
	}
	txLocationsPath := path.Join(dir, "txLocations")
	txLocations, err := transaction.NewBpTree(txLocationsPath)
	if err != nil {
		return nil, err
	}
	return &Storage{
		blockchain: &countingWriter{
			impl: blockchain,
		},
		id2block:         make(map[types.BlockID]int),
		siacoinLocations: siacoinLocations,
		txLocations:      txLocations,
	}, nil
}

func (s *Storage) Add(block *types.Block) error {
	id := block.ID()
	header := blockHeader{
		id:        id,
		parentID:  block.ParentID,
		nonce:     block.Nonce,
		timestamp: block.Timestamp,
	}
	// See Block.MarshalSia.
	for i := range block.MinerPayouts {
		header.minerPayouts = append(header.minerPayouts, s.blockchain.n)
		if err := block.MinerPayouts[i].MarshalSia(s.blockchain); err != nil {
			return err
		}
	}
	for i := range block.Transactions {
		header.transactions = append(header.transactions, s.blockchain.n)
		if err := block.Transactions[i].MarshalSia(s.blockchain); err != nil {
			return err
		}
	}
	s.id2block[id] = len(s.headers)
	blockIndex := len(s.headers)
	s.headers = append(s.headers, header)
	if err := s.addSiacoinOutputs(blockIndex, block); err != nil {
		return err
	}
	if err := s.addTransactions(blockIndex, block); err != nil {
		return err
	}
	return nil
}

func (s *Storage) addSiacoinOutputs(blockIndex int, block *types.Block) error {
	for i := range block.MinerPayouts {
		id := crypto.Hash(block.MinerPayoutID(uint64(i)))
		loc := siacoinoutput.Location{
			Block:  blockIndex,
			Nature: siacoinoutput.MinerPayout,
			Index:  i,
		}
		if err := s.siacoinLocations.Add(id, loc); err != nil {
			return err
		}
	}
	for i, tx := range block.Transactions {
		for j := range tx.SiacoinOutputs {
			id := crypto.Hash(tx.SiacoinOutputID(uint64(j)))
			loc := siacoinoutput.Location{
				Block:  blockIndex,
				Tx:     i,
				Nature: siacoinoutput.SiacoinOutput,
				Index:  j,
			}
			if err := s.siacoinLocations.Add(id, loc); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Storage) addTransactions(blockIndex int, block *types.Block) error {
	for i, tx := range block.Transactions {
		id := crypto.Hash(tx.ID())
		loc := transaction.Location{
			Block: blockIndex,
			Tx:    i,
		}
		if err := s.txLocations.Add(id, loc); err != nil {
			return err
		}
	}
	return nil
}
