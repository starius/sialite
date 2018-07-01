package cache

import (
	"fmt"

	"github.com/NebulousLabs/Sia/crypto"
	"github.com/NebulousLabs/Sia/encoding"
	"github.com/NebulousLabs/Sia/types"
	"github.com/NebulousLabs/merkletree"
)

func VerifyBlockHeader(header types.BlockHeader) error {
	// Check if the block is in the extreme future. We make a distinction between
	// future and extreme future because there is an assumption that by the time
	// the extreme future arrives, this block will no longer be a part of the
	// longest fork because it will have been ignored by all of the miners.
	if header.Timestamp > types.CurrentTimestamp()+types.ExtremeFutureThreshold {
		return fmt.Errorf("Block header validation failed: ExtremeFutureTimestamp")
	}

	// Check if the block is in the near future, but too far to be acceptable.
	// This is the last check because it's an expensive check, and not worth
	// performing if the earlier checks failed.
	if header.Timestamp > types.CurrentTimestamp()+types.FutureThreshold {
		return fmt.Errorf("Block header validation failed: FutureTimestamp")
	}
	return nil
}


func getHeadersSlice(headers []byte) (headersSlice []types.BlockHeader, err error) {
	headersSlice = make([]types.BlockHeader, len(headers))
	headersSlice[0] = types.BlockHeader{
		Timestamp:  types.GenesisTimestamp,
		MerkleRoot: types.GenesisBlock.MerkleRoot(),
	}
	for i := 1; i < len(headers); i++ {
		headersSlice[i] = types.BlockHeader{
			ParentID:  headersSlice[i-1].ID(),
			Timestamp: types.Timestamp(encoding.DecUint64(headers[(i*48 + 8):(i*48 + 16)])),
		}
		copy(headersSlice[i].Nonce[:], headers[i*48:(i*48+8)])
		copy(headersSlice[i].MerkleRoot[:], headers[(i*48+16):(i*48+48)])
	}
	if len(headers) > 1 {
		if headersSlice[1].ParentID != types.GenesisID {
			return nil, fmt.Errorf("ParentID of 2nd header is not GenesisID")
		}
	}
	return headersSlice, nil
}

func VerifyProof(merkleRoot, data, proof []byte, proofIndex int, numLeaves int) bool {
	proofSet := [][]byte{data}
	start := 0
	stop := start + crypto.HashSize
	for stop <= len(proof) {
		proofSet = append(proofSet, proof[start:stop])
		start = stop
		stop = start + crypto.HashSize
	}
	if start != len(proof) {
		return false
	}
	return merkletree.VerifyProof(crypto.NewHash(), merkleRoot, proofSet, uint64(proofIndex), uint64(numLeaves))
}
