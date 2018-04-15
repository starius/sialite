package main

import (
	"encoding/json"
	"flag"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"

	"github.com/NebulousLabs/Sia/types"
	"github.com/starius/sialite/human"
)

const (
	blockLimit = 100
	txLimit    = 1000
)

var (
	addr = flag.String("addr", "http://127.0.0.1:8080", "HTTP API address of sialite")
	out  = flag.String("out", "sample", "Directory to write sample")
)

func doList() []types.BlockID {
	resp, err := http.Get(*addr + "/blocks")
	if err != nil {
		panic(err)
	}
	if resp.StatusCode != http.StatusOK {
		panic(resp.StatusCode)
	}
	dec := json.NewDecoder(resp.Body)
	var ids []types.BlockID
	if err := dec.Decode(&ids); err != nil {
		panic(err)
	}
	resp.Body.Close()
	o, err := os.Create(filepath.Join(*out, "blocks.json"))
	if err != nil {
		panic(err)
	}
	enc := json.NewEncoder(o)
	enc.SetIndent("", "    ")
	if err := enc.Encode(ids); err != nil {
		panic(err)
	}
	o.Close()
	return ids
}

func doBlocks(ids []types.BlockID) (txs []types.TransactionID) {
	for _, blockID := range ids {
		resp, err := http.Get(*addr + "/block/" + blockID.String())
		if err != nil {
			panic(err)
		}
		if resp.StatusCode != http.StatusOK {
			panic(resp.StatusCode)
		}
		dec := json.NewDecoder(resp.Body)
		var block human.Block
		if err := dec.Decode(&block); err != nil {
			panic(err)
		}
		resp.Body.Close()
		o, err := os.Create(filepath.Join(*out, "blocks", blockID.String()+".json"))
		if err != nil {
			panic(err)
		}
		enc := json.NewEncoder(o)
		enc.SetIndent("", "    ")
		if err := enc.Encode(block); err != nil {
			panic(err)
		}
		o.Close()
		// Find transaction ids.
		for _, tx := range block.Transactions {
			txs = append(txs, tx.ID)
		}
	}
	return
}

func doTxs(ids []types.TransactionID) {
	for _, txID := range ids {
		resp, err := http.Get(*addr + "/tx/" + txID.String())
		if err != nil {
			panic(err)
		}
		if resp.StatusCode != http.StatusOK {
			panic(resp.StatusCode)
		}
		dec := json.NewDecoder(resp.Body)
		var tx human.Transaction
		if err := dec.Decode(&tx); err != nil {
			panic(err)
		}
		resp.Body.Close()
		o, err := os.Create(filepath.Join(*out, "txs", txID.String()+".json"))
		if err != nil {
			panic(err)
		}
		enc := json.NewEncoder(o)
		enc.SetIndent("", "    ")
		if err := enc.Encode(tx); err != nil {
			panic(err)
		}
		o.Close()
	}
}

func main() {
	flag.Parse()
	os.Mkdir(*out, 0755)
	os.Mkdir(filepath.Join(*out, "blocks"), 0755)
	os.Mkdir(filepath.Join(*out, "txs"), 0755)
	blocks := doList()
	r := rand.New(rand.NewSource(42)) // Deterministic random.
	r.Shuffle(len(blocks), func(i, j int) {
		// Swap.
		t := blocks[i]
		blocks[i] = blocks[j]
		blocks[j] = t
	})
	someBlocks := blocks[:blockLimit]
	txs := doBlocks(someBlocks)
	r = rand.New(rand.NewSource(42)) // Deterministic random.
	r.Shuffle(len(txs), func(i, j int) {
		// Swap.
		t := txs[i]
		txs[i] = txs[j]
		txs[j] = t
	})
	someTxs := txs[:txLimit]
	doTxs(someTxs)
	// TODO: contracts, addresses, outputs.
}
