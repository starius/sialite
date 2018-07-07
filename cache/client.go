package cache

import (
	"bytes"
	"fmt"
	"math/big"
	"sort"

	"github.com/NebulousLabs/Sia/crypto"
	"github.com/NebulousLabs/Sia/encoding"
	"github.com/NebulousLabs/Sia/types"
	"github.com/NebulousLabs/merkletree"
)

func verifyBlockHeader(header types.BlockHeader, minTimestamp types.Timestamp) error {
	// Check that the timestamp is not too far in the past to be acceptable.
	if header.Timestamp < minTimestamp {
		return fmt.Errorf("Block header validation failed: EarlyTimestamp")
	}

	// Check if the block is in the extreme future is omitted because it does
	// included in the further check (FutureThreshold check) and does not make
	// sense as optimization in our case.
	//if header.Timestamp > types.CurrentTimestamp()+types.ExtremeFutureThreshold {
	//	return fmt.Errorf("Block header validation failed: ExtremeFutureTimestamp")
	//}

	// Check if the block is in the near future, but too far to be acceptable.
	if header.Timestamp > types.CurrentTimestamp()+types.FutureThreshold {
		return fmt.Errorf("Block header validation failed: FutureTimestamp")
	}
	return nil
}

// checkTarget returns true if the block's ID meets the given target.
func checkTarget(id types.BlockID, target types.Target) bool {
	return bytes.Compare(target[:], id[:]) >= 0
}

// minimumValidChildTimestamp returns the earliest timestamp that a child node
// can have while still being valid (child node is one following the last header).
func minimumValidChildTimestamp(headers []types.BlockHeader) (types.Timestamp, error) {
	// Get the previous MedianTimestampWindow timestamps.
	windowTimes := make(types.TimestampSlice, types.MedianTimestampWindow)
	headerIndex := len(headers) - 1
	windowTimes[0] = headers[headerIndex].Timestamp
	parent := headers[headerIndex].ParentID
	for i := 1; i < int(types.MedianTimestampWindow); i++ {
		// If the genesis block is 'parent', use the genesis block timestamp
		// for all remaining times.
		if parent == (types.BlockID{}) {
			windowTimes[i] = windowTimes[i-1]
			continue
		}

		if headerIndex-i < 0 {
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

func getHeadersSlice(headers []byte) ([]types.BlockHeader, error) {
	headersN := len(headers) / 48
	headersSlice := make([]types.BlockHeader, headersN)
	headersSlice[0] = types.BlockHeader{
		Timestamp:  types.GenesisTimestamp,
		MerkleRoot: types.GenesisBlock.MerkleRoot(),
	}
	for i := 1; i < headersN; i++ {
		header := headers[i*48 : (i*48 + 48)]
		headersSlice[i] = types.BlockHeader{
			ParentID:  headersSlice[i-1].ID(),
			Timestamp: types.Timestamp(encoding.DecUint64(header[8:16])),
		}
		copy(headersSlice[i].Nonce[:], header[:8])
		copy(headersSlice[i].MerkleRoot[:], header[16:48])
	}
	if headersN > 1 && headersSlice[1].ParentID != types.GenesisID {
		return nil, fmt.Errorf("ParentID of 2nd header is not GenesisID")
	}
	return headersSlice, nil
}

// calculateBlockTotals computes the new total time and total target
// for the current block.
func calculateBlockTotals(
	currentHeight types.BlockHeight,
	currentBlockID types.BlockID,
	prevTotalTime int64,
	parentTimestamp, currentTimestamp types.Timestamp,
	prevTotalTarget, targetOfCurrentBlock types.Target,
) (newTotalTime int64, newTotalTarget types.Target) {
	// Reset the prevTotalTime to a delta of zero just before the hardfork.
	//
	// NOTICE: This code is broken, an incorrectly executed hardfork. The
	// correct thing to do was to not put in these 3 lines of code. It is
	// correct to not have them.
	//
	// This code is incorrect, and introduces an unfortunate drop in difficulty,
	// because this is an uncompreesed prevTotalTime, but really it should be
	// getting set to a compressed prevTotalTime. And, actually, a compressed
	// prevTotalTime doesn't have much meaning, so this code block shouldn't be
	// here at all. But... this is the code that was running for the block
	// 135,000 hardfork, so this code needs to stay. With the standard
	// constants, it should cause a disruptive bump that lasts only a few days.
	//
	// The disruption will be complete well before we can deploy a fix, so
	// there's no point in fixing it.
	if currentHeight == types.OakHardforkBlock-1 {
		prevTotalTime = int64(types.BlockFrequency * currentHeight)
	}

	// For each value, first multiply by the decay, and then add in the new
	// delta.
	newTotalTime = (prevTotalTime * types.OakDecayNum / types.OakDecayDenom) + (int64(currentTimestamp) - int64(parentTimestamp))
	newTotalTarget = prevTotalTarget.MulDifficulty(big.NewRat(types.OakDecayNum, types.OakDecayDenom)).AddDifficulties(targetOfCurrentBlock)

	return newTotalTime, newTotalTarget
}

func VerifyBlockHeaders(headers []byte) error {
	headersSlice, err := getHeadersSlice(headers)
	if err != nil {
		return err
	}
	if len(headers)/48 == 0 {
		return fmt.Errorf("Can't verify list of 0 headers")
	}
	minTimestamp := headersSlice[0].Timestamp
	for i, header := range headersSlice {
		err = verifyBlockHeader(header, minTimestamp)
		if err != nil {
			return err
		}
		minTimestamp, err = minimumValidChildTimestamp(headersSlice[:i+1])
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
