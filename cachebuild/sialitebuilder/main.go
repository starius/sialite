package main

import (
	"context"
	"flag"
	"log"
	"sync"

	"github.com/NebulousLabs/Sia/types"
	"github.com/starius/sialite/cachebuild"
	"github.com/starius/sialite/netlib"
)

var (
	blockchain = flag.String("blockchain", "", "Input file with blockchain")
	source     = flag.String("source", "", "Source of data (siad node)")
	files      = flag.String("files", "", "Dir to write files")
	memLimit   = flag.Int("memlimit", 64*1024*1024, "Memory limit, bytes")
	nblocks    = flag.Int("nblocks", 0, "Approximate max number of blocks (0 = all)")
)

func main() {
	flag.Parse()
	ctx := context.Background()
	cache, err := cachebuild.New(*files, *memLimit)
	if err != nil {
		log.Fatalf("cachebuild.New: %v", err)
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
		if err := cache.Add(block); err != nil {
			panic(err)
		}
	}
	cancel()
	for range bchan {
	}
	wg.Wait()
	if err := cache.Build(); err != nil {
		panic(err)
	}
}
