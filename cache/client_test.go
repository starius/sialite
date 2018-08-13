package cache

import (
	"testing"

	"gitlab.com/NebulousLabs/Sia/types"
)

type headersOfBlocks struct {
	blocks []*types.Block
}

func (b *headersOfBlocks) Length() int {
	return len(b.blocks)
}

func (b *headersOfBlocks) Index(i int) BlockInfo {
	return BlockInfo{
		CurrentID:   b.blocks[i].ID(),
		BlockHeader: b.blocks[i].Header(),
	}
}

func TestVerify1000Blocks(t *testing.T) {
	blocks, err := read1000Blocks()
	if err != nil {
		t.Fatalf("read1000Blocks: %v", err)
	}
	headers := &headersOfBlocks{blocks}
	if err := VerifyBlockHeaders(headers); err != nil {
		t.Errorf("VerifyBlockHeaders(first 1000 blocks): %v.", err)
	}
}
