package cache

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/big"
	"sort"

	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/Sia/encoding"
	"gitlab.com/NebulousLabs/Sia/types"
	"gitlab.com/NebulousLabs/merkletree"
)

// BlockInfo is block header and ID of the current block.
type BlockInfo struct {
	types.BlockHeader

	CurrentID types.BlockID
}

// BlockHeadersSet represents a set of block headers.
type BlockHeadersSet interface {
	// Get i-th block header in the set.
	Index(i int) BlockInfo

	// Get given set's length.
	Length() int
}

type BlockHeadersSetImpl struct {
	headersBytes []byte
	ids          []types.BlockID
}

func (s *BlockHeadersSetImpl) Length() int {
	return len(s.ids)
}

func (s *BlockHeadersSetImpl) Index(i int) BlockInfo {
	info := BlockInfo{
		CurrentID: s.ids[i],
	}
	info.BlockHeader = headerAt(s.headersBytes, i)
	if i != 0 {
		info.ParentID = s.ids[i-1]
	}
	return info
}

func headerAt(headersBytes []byte, i int) (header types.BlockHeader) {
	headerBytes := headersBytes[i*48 : (i*48 + 48)]
	header.Timestamp = types.Timestamp(encoding.DecUint64(headerBytes[8:16]))
	copy(header.Nonce[:], headerBytes[:8])
	copy(header.MerkleRoot[:], headerBytes[16:48])
	return
}

func ParseHeaders(headersBytes []byte) (*BlockHeadersSetImpl, error) {
	headersN := len(headersBytes) / 48
	if int(headersN*48) != len(headersBytes) {
		return nil, fmt.Errorf("bad length of headers: %d", len(headersBytes))
	}
	var id types.BlockID
	var ids []types.BlockID
	for i := 0; i < headersN; i++ {
		header := headerAt(headersBytes, i)
		header.ParentID = id
		id = header.ID()
		ids = append(ids, id)
	}
	return &BlockHeadersSetImpl{
		headersBytes: headersBytes,
		ids:          ids,
	}, nil
}

func verifyBlockHeader(
	info BlockInfo,
	minTimestamp types.Timestamp,
	target types.Target,
) error {
	if !checkTarget(info.CurrentID, target) {
		return fmt.Errorf("Block header validation failed: block is unsolved, id %s, target %s", info.CurrentID, hex.EncodeToString(target[:]))
	}
	// Check that the timestamp is not too far in the past to be acceptable.
	if info.Timestamp < minTimestamp {
		return fmt.Errorf("Block header validation failed: EarlyTimestamp")
	}

	// Check if the block is in the extreme future is omitted because it does
	// included in the further check (FutureThreshold check) and does not make
	// sense as optimization in our case.
	//if info.Timestamp > types.CurrentTimestamp()+types.ExtremeFutureThreshold {
	//	return fmt.Errorf("Block header validation failed: ExtremeFutureTimestamp")
	//}

	// Check if the block is in the near future, but too far to be acceptable.
	if info.Timestamp > types.CurrentTimestamp()+types.FutureThreshold {
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
func minimumValidChildTimestamp(headers BlockHeadersSet, headerIndex int) (types.Timestamp, error) {
	// Get the previous MedianTimestampWindow timestamps.
	windowTimes := make(types.TimestampSlice, types.MedianTimestampWindow)
	header := headers.Index(headerIndex)
	windowTimes[0] = header.Timestamp
	parentID := header.ParentID
	for i := 1; i < int(types.MedianTimestampWindow); i++ {
		// If the genesis block is 'parent', use the genesis block timestamp
		// for all remaining times.
		if parentID == (types.BlockID{}) {
			windowTimes[i] = windowTimes[i-1]
			continue
		}

		if i > headerIndex {
			return 0, fmt.Errorf(
				"minimumValidChildTimestamp: headers are not sorted properly or 1st header is not genesis header",
			)
		}
		header := headers.Index(headerIndex - i)
		parentID = header.ParentID
		windowTimes[i] = header.Timestamp
	}
	sort.Sort(windowTimes)

	// Return the median of the sorted timestamps.
	return windowTimes[len(windowTimes)/2], nil
}

// calculateBlockTotals computes the new total time and total target
// for the current block.
func calculateBlockTotals(
	currentHeight int,
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
	if currentHeight == int(types.OakHardforkBlock-1) {
		prevTotalTime = int64(int(types.BlockFrequency) * currentHeight)
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
	parentHeight int,
	parentTimestamp types.Timestamp,
) types.Target {
	// Determine the delta of the current total time vs. the desired total time.
	// The desired total time is the difference between the genesis block
	// timestamp and the current block timestamp.
	var delta int64
	if parentHeight < int(types.OakHardforkFixBlock) {
		// This is the original code. It is incorrect, because it is comparing
		// 'expectedTime', an absolute value, to 'parentTotalTime', a value
		// which gets compressed every block. The result is that 'expectedTime'
		// is substantially larger than 'parentTotalTime' always, and that the
		// shifter is always reading that blocks have been coming out far too
		// quickly.
		expectedTime := int64(int(types.BlockFrequency) * parentHeight)
		delta = expectedTime - parentTotalTime
	} else {
		// This is the correct code. The expected time is an absolute time based
		// on the genesis block, and the delta is an absolute time based on the
		// timestamp of the parent block.
		//
		// Rules elsewhere in consensus ensure that the timestamp of the parent
		// block has not been manipulated by more than a few hours, which is
		// accurate enough for this logic to be safe.
		expectedTime := int64(int(types.BlockFrequency)*parentHeight) + int64(types.GenesisTimestamp)
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
func targetAdjustmentBase(headers BlockHeadersSet, currentHeight int) *big.Rat {
	currentHeader := headers.Index(currentHeight)
	// Grab the block that was generated 'TargetWindow' blocks prior to the
	// parent. If there are not 'TargetWindow' blocks yet, stop at the genesis
	// block.
	var windowSize int
	if currentHeight > int(types.TargetWindow) {
		windowSize = int(types.TargetWindow)
	} else {
		windowSize = currentHeight
	}

	timestamp := headers.Index(currentHeight - windowSize).Timestamp

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
	expectedTimePassed := int(types.BlockFrequency) * windowSize
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
	headers BlockHeadersSet,
	currentHeight int,
	currentTarget types.Target,
) types.Target {
	if currentHeight%(int(types.TargetWindow)/2) != 0 {
		return currentTarget
	}
	adjustment := clampTargetAdjustment(targetAdjustmentBase(headers, currentHeight))
	adjustedRatTarget := new(big.Rat).Mul(currentTarget.Rat(), adjustment)
	return types.RatToTarget(adjustedRatTarget)
}

func calculateChildTarget(
	headers BlockHeadersSet,
	currentTarget types.Target,
	parentTotalTime int64,
	parentTotalTarget types.Target,
	parentHeight int,
	parentTimestamp types.Timestamp,
) types.Target {
	if parentHeight < int(types.OakHardforkBlock) {
		return oldTargetAdjustment(headers, parentHeight+1, currentTarget)
	} else {
		return oakAdjustment(parentTotalTime, parentTotalTarget, currentTarget, parentHeight, parentTimestamp)
	}
}

func getTargets(headers BlockHeadersSet) []types.Target {
	targets := make([]types.Target, headers.Length())
	// Set the base values.
	// Parent timestamp for genesis block is GenesisTimestamp as well.
	parentTimestamp := types.GenesisTimestamp
	// Parent height for genesis block is 0.
	parentHeight := 0
	// Block totals 'before' the genesis block.
	totalTime := int64(0)
	totalTarget := types.RootDepth
	// The first target is root target.
	targets[0] = types.RootTarget
	for i := 0; i < headers.Length()-1; i++ {
		blockHeader := headers.Index(i)
		// The algorithm computes the target of a child.
		// That's why we set i+1s target here.
		targets[i+1] = calculateChildTarget(headers, targets[i], totalTime, totalTarget, parentHeight, parentTimestamp)
		// Calculate the new block totals.
		totalTime, totalTarget = calculateBlockTotals(i, blockHeader.CurrentID, totalTime, parentTimestamp, blockHeader.Timestamp, totalTarget, targets[i])
		// Update the parents values.
		parentTimestamp = blockHeader.Timestamp
		parentHeight = i
	}
	return targets
}

func VerifyBlockHeaders(headers BlockHeadersSet) error {
	if headers.Length() == 0 {
		return fmt.Errorf("number of block headers is 0")
	}
	targets := getTargets(headers)
	first := headers.Index(0)
	minTimestamp := first.Timestamp
	if first.CurrentID != types.GenesisID {
		return fmt.Errorf("bad genesis block")
	}
	var err error
	for i := 1; i < headers.Length(); i++ {
		header := headers.Index(i)
		if err = verifyBlockHeader(header, minTimestamp, targets[i]); err != nil {
			return fmt.Errorf("verifyBlockHeader of block %d: %v", i, err)
		}
		minTimestamp, err = minimumValidChildTimestamp(headers, i)
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
