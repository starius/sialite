package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"

	"github.com/starius/sialite/human"
	"gitlab.com/NebulousLabs/Sia/types"
)

const (
	blockLimit     = 100
	txLimit        = 1000
	addressesLimit = 1000
	scosLimit      = 1000
	sfosLimit      = 1000
	contractsLimit = 1000
)

var (
	addr = flag.String("addr", "http://127.0.0.1:8080", "HTTP API address of sialite")
	out  = flag.String("out", "sample", "Directory to write sample")
)

func doList() (ids []types.BlockID) {
	startWith := ""
	for {
		n := len(ids)
		resp, err := http.Get(*addr + "/blocks?startwith=" + startWith)
		if err != nil {
			panic(err)
		}
		if resp.StatusCode != http.StatusOK {
			panic(resp.StatusCode)
		}
		dec := json.NewDecoder(resp.Body)
		var headers human.BlockHeaders
		if err := dec.Decode(&headers); err != nil {
			panic(err)
		}
		resp.Body.Close()
		fname := fmt.Sprintf("blocks%07d.json", n)
		o, err := os.Create(filepath.Join(*out, fname))
		if err != nil {
			panic(err)
		}
		enc := json.NewEncoder(o)
		enc.SetIndent("", "    ")
		if err := enc.Encode(headers); err != nil {
			panic(err)
		}
		o.Close()
		for _, h := range headers.Headers {
			ids = append(ids, h.ID)
		}
		if headers.Next == "" {
			break
		}
		startWith = headers.Next
	}
	return
}

func doBlocks(ids []types.BlockID) (txs []types.TransactionID, addresses []types.UnlockHash, scos []types.SiacoinOutputID, sfos []types.SiafundOutputID, contracts []types.FileContractID) {
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
			scos = append(scos, so.ID)
		}
		for _, tx := range block.Transactions {
			txs = append(txs, tx.ID)
			for _, si := range tx.SiacoinInputs {
				scos = append(scos, si.ParentID)
				addresses = append(addresses, si.Parent.UnlockHash)
			}
			for _, so := range tx.SiacoinOutputs {
				addresses = append(addresses, so.UnlockHash)
				scos = append(scos, so.ID)
			}
			for _, si := range tx.SiafundInputs {
				sfos = append(sfos, si.ParentID)
				addresses = append(addresses, si.Parent.UnlockHash)
			}
			for _, so := range tx.SiafundOutputs {
				addresses = append(addresses, so.UnlockHash)
				sfos = append(sfos, so.ID)
			}
			for _, c := range tx.FileContracts {
				contract := c.History.Contract
				contracts = append(contracts, contract.ID)
				for _, so := range contract.ValidProofOutputs {
					addresses = append(addresses, so.UnlockHash)
					scos = append(scos, so.ID)
				}
				for _, so := range contract.MissedProofOutputs {
					addresses = append(addresses, so.UnlockHash)
					scos = append(scos, so.ID)
				}
			}
			for _, r := range tx.FileContractRevisions {
				rev := r.History.Revisions[r.Index]
				contracts = append(contracts, rev.ParentID)
				for _, so := range rev.NewValidProofOutputs {
					addresses = append(addresses, so.UnlockHash)
					scos = append(scos, so.ID)
				}
				for _, so := range rev.NewMissedProofOutputs {
					addresses = append(addresses, so.UnlockHash)
					scos = append(scos, so.ID)
				}
			}
		}
	}
	return
}

func findRare(ids []types.BlockID) (scos []types.SiacoinOutputID, sfos []types.SiafundOutputID) {
	for _, blockID := range ids {
		if len(scos) != 0 && len(sfos) != 0 {
			break
		}
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
		for _, tx := range block.Transactions {
			for _, si := range tx.SiacoinInputs {
				if si.Source.Nature == "sia_claim_output" {
					scos = append(scos, si.ParentID)
				}
			}
			for _, si := range tx.SiafundInputs {
				sfos = append(sfos, si.ParentID)
			}
			for _, so := range tx.SiafundOutputs {
				sfos = append(sfos, so.ID)
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

func doScos(scos []types.SiacoinOutputID) {
	for _, id := range scos {
		resp, err := http.Get(*addr + "/siacoin-output/" + id.String())
		if err != nil {
			panic(err)
		}
		if resp.StatusCode != http.StatusOK {
			panic(resp.Status + " " + id.String())
		}
		dec := json.NewDecoder(resp.Body)
		var record human.SiacoinRecord
		if err := dec.Decode(&record); err != nil {
			panic(err)
		}
		resp.Body.Close()
		o, err := os.Create(filepath.Join(*out, "siacoin-output", id.String()+".json"))
		if err != nil {
			panic(err)
		}
		enc := json.NewEncoder(o)
		enc.SetIndent("", "    ")
		if err := enc.Encode(record); err != nil {
			panic(err)
		}
		o.Close()
	}
}

func doSfos(sfos []types.SiafundOutputID) {
	for _, id := range sfos {
		resp, err := http.Get(*addr + "/siafund-output/" + id.String())
		if err != nil {
			panic(err)
		}
		if resp.StatusCode != http.StatusOK {
			panic(resp.StatusCode)
		}
		dec := json.NewDecoder(resp.Body)
		var record human.SiafundRecord
		if err := dec.Decode(&record); err != nil {
			panic(err)
		}
		resp.Body.Close()
		o, err := os.Create(filepath.Join(*out, "siafund-output", id.String()+".json"))
		if err != nil {
			panic(err)
		}
		enc := json.NewEncoder(o)
		enc.SetIndent("", "    ")
		if err := enc.Encode(record); err != nil {
			panic(err)
		}
		o.Close()
	}
}

func doContracts(contracts []types.FileContractID) {
	for _, id := range contracts {
		resp, err := http.Get(*addr + "/contract/" + id.String())
		if err != nil {
			panic(err)
		}
		if resp.StatusCode != http.StatusOK {
			panic(resp.Status + " " + id.String())
		}
		dec := json.NewDecoder(resp.Body)
		var history human.ContractHistory
		if err := dec.Decode(&history); err != nil {
			panic(err)
		}
		resp.Body.Close()
		o, err := os.Create(filepath.Join(*out, "contracts", id.String()+".json"))
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
	os.Mkdir(filepath.Join(*out, "siacoin-output"), 0755)
	os.Mkdir(filepath.Join(*out, "siafund-output"), 0755)
	os.Mkdir(filepath.Join(*out, "contracts"), 0755)
	blocks := doList()
	blocks0 := blocks
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
	txs, addresses, scos, sfos, contracts := doBlocks(blocks)
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
	if len(scos) > scosLimit {
		r.Shuffle(len(scos), func(i, j int) {
			// Swap.
			t := scos[i]
			scos[i] = scos[j]
			scos[j] = t
		})
		scos = scos[:scosLimit]
	}
	if len(sfos) > sfosLimit {
		r.Shuffle(len(sfos), func(i, j int) {
			// Swap.
			t := sfos[i]
			sfos[i] = sfos[j]
			sfos[j] = t
		})
		sfos = sfos[:sfosLimit]
	}
	if len(contracts) > contractsLimit {
		r.Shuffle(len(contracts), func(i, j int) {
			// Swap.
			t := contracts[i]
			contracts[i] = contracts[j]
			contracts[j] = t
		})
		contracts = contracts[:contractsLimit]
	}
	// Make sure we have Siacoin inputs from claims and siafunds.
	scos2, sfos2 := findRare(blocks0)
	scos = append(scos, scos2...)
	sfos = append(sfos, sfos2...)
	doTxs(txs)
	doAddresses(addresses)
	doScos(scos)
	doSfos(sfos)
	doContracts(contracts)
}
