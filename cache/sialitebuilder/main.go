package main

import (
	"context"
	"flag"
	"log"
	"sync"

	"github.com/NebulousLabs/Sia/types"
	"github.com/starius/sialite/cache"
	"github.com/starius/sialite/netlib"
)

var (
	blockchain = flag.String("blockchain", "", "Input file with blockchain")
	source     = flag.String("source", "", "Source of data (siad node)")
	files      = flag.String("files", "", "Dir to write files")
	memLimit   = flag.Int("memlimit", 64*1024*1024, "Memory limit, bytes")
	nblocks    = flag.Int("nblocks", 0, "Approximate max number of blocks (0 = all)")

	offsetLen               = flag.Int("offset_len", 8, "sizeof(offset in blockchain file)")
	offsetIndexLen          = flag.Int("offset_index_len", 4, "sizeof(index in offsets file)")
	addressPageLen          = flag.Int("address_page_len", 4096, "sizeof(page in addressesFastmapData)")
	addressPrefixLen        = flag.Int("address_prefix_len", 16, "sizeof(prefix of address to store)")
	addressFastmapPrefixLen = flag.Int("address_fastmap_prefix_len", 5, "sizeof(prefix of address to store in addressesFastmapPrefixes)")
	addressOffsetLen        = flag.Int("address_offset_len", 4, "sizeof(offset in addressesIndices file)")
)

func main() {
	flag.Parse()
	ctx := context.Background()
	b, err := cache.NewBuilder(*files, *memLimit, *offsetLen, *offsetIndexLen, *addressPageLen, *addressPrefixLen, *addressFastmapPrefixLen, *addressOffsetLen)
	if err != nil {
		log.Fatalf("cache.NewBuilder: %v", err)
	}
	_, f, err := netlib.OpenOrConnect(ctx, *blockchain, *source)
	if err != nil {
		panic(err)
	}
	bchan := make(chan *types.Block, 2)
	bchan <- &types.GenesisBlock
	var wg sync.WaitGroup
	wg.Add(1)
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer wg.Done()
		if err := netlib.DownloadAllBlocks(ctx, bchan, f); err != nil {
			if err != context.Canceled {
				panic(err)
			}
		}
		close(bchan)
	}()
	i := 0
	for block := range bchan {
		i++
		if *nblocks != 0 && i > *nblocks {
			log.Printf("processBlocks got %d blocks", *nblocks)
			break
		}
		if err := b.Add(block); err != nil {
			panic(err)
		}
	}
	cancel()
	for range bchan {
	}
	wg.Wait()
	if err := b.Build(); err != nil {
		panic(err)
	}
}
