package cache

import (
	"fmt"
	"sort"

	"github.com/NebulousLabs/Sia/crypto"
	"github.com/NebulousLabs/Sia/encoding"
	"github.com/NebulousLabs/Sia/types"
	"github.com/NebulousLabs/merkletree"
)

func verifyBlockHeader(header types.BlockHeader, minTimestamp types.Timestamp) error {
	// Check that the timestamp is not too far in the past to be acceptable.
	if minTimestamp > header.Timestamp {
		return fmt.Errorf("Block header validation failed: EarlyTimestamp")
	}

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

// minimumValidChildTimestamp returns the earliest timestamp that a child node
// can have while still being valid.
func minimumValidChildTimestamp(headers []types.BlockHeader, headerIndex int) (minTimestamp types.Timestamp, err error) {
	// Get the previous MedianTimestampWindow timestamps.
	windowTimes := make(types.TimestampSlice, types.MedianTimestampWindow)
	windowTimes[0] = headers[headerIndex].Timestamp
	parent := headers[headerIndex].ParentID
	for i := 1; i < int(types.MedianTimestampWindow); i++ {
		// If the genesis block is 'parent', use the genesis block timestamp
		// for all remaining times.
		if parent == (types.BlockID{}) {
			windowTimes[i] = windowTimes[i-1]
			continue
		}

		if (headerIndex - i) < 0 {
			return 0, fmt.Errorf(
				"minimumValidChildTimestamp: headers are not sorted properly or 1st header is not genesis header",
			)
		}
		parent = headers[headerIndex-i].ParentID
		windowTimes[i] = headers[headerIndex-i].Timestamp
	}
	sort.Sort(windowTimes)

	// Return the median of the sorted timestamps.
	return windowTimes[len(windowTimes)/2], nil
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

func VerifyBlockHeaders(headers []byte) error {
	headersSlice, err := getHeadersSlice(headers)
	if err != nil {
		return err
	}
	if len(headers) < 1 {
		return fmt.Errorf("Can't verify list of 0 headers")
	}
	minTimestamp := headersSlice[0].Timestamp
	for i, header := range headersSlice {
		err = verifyBlockHeader(header, minTimestamp)
		if err != nil {
			return err
		}
		minTimestamp, err = minimumValidChildTimestamp(headersSlice, i)
		if err != nil {
			return err
		}
	}
	return nil
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
