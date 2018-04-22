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
	blockLimit     = 100
	txLimit        = 1000
	addressesLimit = 1000
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

func doBlocks(ids []types.BlockID) (txs []types.TransactionID, addresses []types.UnlockHash) {
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
		// Find transaction ids and addresses.
		for _, so := range block.MinerPayouts {
			addresses = append(addresses, so.UnlockHash)
		}
		for _, tx := range block.Transactions {
			txs = append(txs, tx.ID)
			for _, si := range tx.SiacoinInputs {
				addresses = append(addresses, si.UnlockConditions.UnlockHash())
			}
			for _, so := range tx.SiacoinOutputs {
				addresses = append(addresses, so.UnlockHash)
			}
			for _, si := range tx.SiafundInputs {
				addresses = append(addresses, si.UnlockConditions.UnlockHash())
			}
			for _, so := range tx.SiafundOutputs {
				addresses = append(addresses, so.UnlockHash)
			}
			for _, contract := range tx.FileContracts {
				for _, so := range contract.ValidProofOutputs {
					addresses = append(addresses, so.UnlockHash)
				}
				for _, so := range contract.MissedProofOutputs {
					addresses = append(addresses, so.UnlockHash)
				}
			}
			for _, rev := range tx.FileContractRevisions {
				for _, so := range rev.NewValidProofOutputs {
					addresses = append(addresses, so.UnlockHash)
				}
				for _, so := range rev.NewMissedProofOutputs {
					addresses = append(addresses, so.UnlockHash)
				}
			}
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

func doAddresses(addresses []types.UnlockHash) {
	for _, address := range addresses {
		resp, err := http.Get(*addr + "/address/" + address.String())
		if err != nil {
			panic(err)
		}
		if resp.StatusCode != http.StatusOK {
			panic(resp.StatusCode)
		}
		dec := json.NewDecoder(resp.Body)
		var history human.AddressHistory
		if err := dec.Decode(&history); err != nil {
			panic(err)
		}
		resp.Body.Close()
		o, err := os.Create(filepath.Join(*out, "addresses", address.String()+".json"))
		if err != nil {
			panic(err)
		}
		enc := json.NewEncoder(o)
		enc.SetIndent("", "    ")
		if err := enc.Encode(history); err != nil {
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
	os.Mkdir(filepath.Join(*out, "addresses"), 0755)
	blocks := doList()
	r := rand.New(rand.NewSource(42)) // Deterministic random.
	if len(blocks) > blockLimit {
		r.Shuffle(len(blocks), func(i, j int) {
			// Swap.
			t := blocks[i]
			blocks[i] = blocks[j]
			blocks[j] = t
		})
		blocks = blocks[:blockLimit]
	}
	txs, addresses := doBlocks(blocks)
	if len(txs) > txLimit {
		r.Shuffle(len(txs), func(i, j int) {
			// Swap.
			t := txs[i]
			txs[i] = txs[j]
			txs[j] = t
		})
		txs = txs[:txLimit]
	}
	if len(addresses) > addressesLimit {
		r.Shuffle(len(addresses), func(i, j int) {
			// Swap.
			t := addresses[i]
			addresses[i] = addresses[j]
			addresses[j] = t
		})
		addresses = addresses[:addressesLimit]
	}
	doTxs(txs)
	doAddresses(addresses)
	// TODO: contracts, outputs.
}
