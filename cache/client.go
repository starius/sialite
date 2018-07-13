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

func oakAdjustment(
	parentTotalTime int64,
	parentTotalTarget types.Target,
	currentTarget types.Target,
	parentHeight types.BlockHeight,
	parentTimestamp types.Timestamp,
) types.Target {
	// Determine the delta of the current total time vs. the desired total time.
	// The desired total time is the difference between the genesis block
	// timestamp and the current block timestamp.
	var delta int64
	if parentHeight < types.OakHardforkFixBlock {
		// This is the original code. It is incorrect, because it is comparing
		// 'expectedTime', an absolute value, to 'parentTotalTime', a value
		// which gets compressed every block. The result is that 'expectedTime'
		// is substantially larger than 'parentTotalTime' always, and that the
		// shifter is always reading that blocks have been coming out far too
		// quickly.
		expectedTime := int64(types.BlockFrequency * parentHeight)
		delta = expectedTime - parentTotalTime
	} else {
		// This is the correct code. The expected time is an absolute time based
		// on the genesis block, and the delta is an absolute time based on the
		// timestamp of the parent block.
		//
		// Rules elsewhere in consensus ensure that the timestamp of the parent
		// block has not been manipulated by more than a few hours, which is
		// accurate enough for this logic to be safe.
		expectedTime := int64(types.BlockFrequency*parentHeight) + int64(types.GenesisTimestamp)
		delta = expectedTime - int64(parentTimestamp)
	}
	// Convert the delta in to a target block time.
	square := delta * delta
	if delta < 0 {
		// If the delta is negative, restore the negative value.
		square *= -1
	}
	shift := square / 10e6 // 10e3 second delta leads to 10 second shift.
	targetBlockTime := int64(types.BlockFrequency) + shift

	// Clamp the block time to 1/3 and 3x the target block time.
	if targetBlockTime < int64(types.BlockFrequency)/types.OakMaxBlockShift {
		targetBlockTime = int64(types.BlockFrequency) / types.OakMaxBlockShift
	}
	if targetBlockTime > int64(types.BlockFrequency)*types.OakMaxBlockShift {
		targetBlockTime = int64(types.BlockFrequency) * types.OakMaxBlockShift
	}

	// Determine the hashrate using the total time and total target. Set a
	// minimum total time of 1 to prevent divide by zero and underflows.
	if parentTotalTime < 1 {
		parentTotalTime = 1
	}
	visibleHashrate := parentTotalTarget.Difficulty().Div64(uint64(parentTotalTime)) // Hashes per second.
	// Handle divide by zero risks.
	if visibleHashrate.IsZero() {
		visibleHashrate = visibleHashrate.Add(types.NewCurrency64(1))
	}
	if targetBlockTime == 0 {
		// This code can only possibly be triggered if the block frequency is
		// less than 3, but during testing the block frequency is 1.
		targetBlockTime = 1
	}

	// Determine the new target by multiplying the visible hashrate by the
	// target block time. Clamp it to a 0.4% difficulty adjustment.
	maxNewTarget := currentTarget.MulDifficulty(types.OakMaxRise) // Max = difficulty increase (target decrease)
	minNewTarget := currentTarget.MulDifficulty(types.OakMaxDrop) // Min = difficulty decrease (target increase)
	newTarget := types.RatToTarget(new(big.Rat).SetFrac(types.RootDepth.Int(), visibleHashrate.Mul64(uint64(targetBlockTime)).Big()))
	if newTarget.Cmp(maxNewTarget) < 0 {
		newTarget = maxNewTarget
	}
	if newTarget.Cmp(minNewTarget) > 0 {
		// This can only possibly trigger if the BlockFrequency is less than 3
		// seconds, but during testing it is 1 second.
		newTarget = minNewTarget
	}
	return newTarget
}

// targetAdjustmentBase returns the magnitude that the target should be
// adjusted by before a clamp is applied.
func targetAdjustmentBase(headers []types.BlockHeader) *big.Rat {
	currentHeight := types.BlockHeight(len(headers) - 1)
	currentHeader := headers[currentHeight]
	// Grab the block that was generated 'TargetWindow' blocks prior to the
	// parent. If there are not 'TargetWindow' blocks yet, stop at the genesis
	// block.
	var windowSize types.BlockHeight
	if currentHeight > types.TargetWindow {
		windowSize = types.TargetWindow
	} else {
		windowSize = currentHeight
	}

	timestamp := headers[currentHeight-windowSize].Timestamp

	// The target of a child is determined by the amount of time that has
	// passed between the generation of its immediate parent and its
	// TargetWindow'th parent. The expected amount of seconds to have passed is
	// TargetWindow*BlockFrequency. The target is adjusted in proportion to how
	// time has passed vs. the expected amount of time to have passed.
	//
	// The target is converted to a big.Rat to provide infinite precision
	// during the calculation. The big.Rat is just the int representation of a
	// target.
	timePassed := currentHeader.Timestamp - timestamp
	expectedTimePassed := types.BlockFrequency * windowSize
	return big.NewRat(int64(timePassed), int64(expectedTimePassed))
}

// clampTargetAdjustment returns a clamped version of the base adjustment
// value. The clamp keeps the maximum adjustment to ~7x every 2000 blocks. This
// ensures that raising and lowering the difficulty requires a minimum amount
// of total work, which prevents certain classes of difficulty adjusting
// attacks.
func clampTargetAdjustment(base *big.Rat) *big.Rat {
	if base.Cmp(types.MaxTargetAdjustmentUp) > 0 {
		return types.MaxTargetAdjustmentUp
	} else if base.Cmp(types.MaxTargetAdjustmentDown) < 0 {
		return types.MaxTargetAdjustmentDown
	}
	return base
}

func oldTargetAdjustment(
	headers []types.BlockHeader,
	currentHeight types.BlockHeight,
	currentTarget types.Target,
) types.Target {
	if currentHeight%(types.TargetWindow/2) != 0 {
		return currentTarget
	}
	adjustment := clampTargetAdjustment(targetAdjustmentBase(headers))
	adjustedRatTarget := new(big.Rat).Mul(currentTarget.Rat(), adjustment)
	return types.RatToTarget(adjustedRatTarget)
}

func calculateChildTarget(
	headers []types.BlockHeader,
	currentTarget types.Target,
	parentTotalTime int64,
	parentTotalTarget types.Target,
	parentHeight types.BlockHeight,
	parentTimestamp types.Timestamp,
) types.Target {
	if parentHeight < types.OakHardforkBlock {
		return oldTargetAdjustment(headers, parentHeight+1, currentTarget)
	} else {
		return oakAdjustment(parentTotalTime, parentTotalTarget, currentTarget, parentHeight, parentTimestamp)
	}
}

func getTargets(headers []types.BlockHeader) []types.Target {
	targets := make([]types.Target, len(headers))
	// Set the base values.
	// Parent timestamp for genesis block is GenesisTimestamp as well.
	parentTimestamp := types.GenesisTimestamp
	// Parent height for genesis block is 0.
	parentHeight := types.BlockHeight(0)
	// Block totals 'before' the genesis block.
	totalTime := int64(0)
	totalTarget := types.RootDepth
	// The first target is root target.
	targets[0] = types.RootTarget
	for i := types.BlockHeight(0); i < types.BlockHeight(len(headers)); i++ {
		blockHeader := headers[i]
		// The algorithm computes the target of a child.
		// That's why we set i+1s target here.
		targets[i+1] = calculateChildTarget(headers[:i+1], targets[i], totalTime, totalTarget, parentHeight, parentTimestamp)
		// Calculate the new block totals.
		totalTime, totalTarget = calculateBlockTotals(i, blockHeader.ID(), totalTime, parentTimestamp, blockHeader.Timestamp, totalTarget, targets[i])
		// Update the parents values.
		parentTimestamp = blockHeader.Timestamp
		parentHeight = i
	}
	return targets
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
