package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/NebulousLabs/Sia/encoding"
	"github.com/NebulousLabs/Sia/types"
	"github.com/starius/sialite/cache"
)

var (
	server  = flag.String("server", "127.0.0.1:35813", "Target address")
	address = flag.String("address", "", "Target address")
)

type fullItem struct {
	source *cache.Item
	payout *types.SiacoinOutput
	tx     *types.Transaction
}

func addressHistory(address string, headers []byte) ([]fullItem, error) {
	next := ""
	var rawItems []cache.Item
	for {
		url := fmt.Sprintf("http://%s/v1/address-history?address=%s&start=%s", *server, address, next)
		respHistory, err := http.Get(url)
		if err != nil {
			return nil, fmt.Errorf("http.Get(%q): %v", url, err)
		}
		if respHistory.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("http.Get(%q): %s", url, respHistory.Status)
		}
		var history []cache.Item
		if err := encoding.NewDecoder(respHistory.Body).DecodeAll(&next, &history); err != nil {
			return nil, fmt.Errorf("DecodeAll: %v", err)
		}
		respHistory.Body.Close()
		rawItems = append(rawItems, history...)
		if next == "" {
			break
		}
	}
	var fullItems []fullItem
	for i := 0; i < len(rawItems); i++ {
		item := &rawItems[i]
		data, err := item.SourceData(nil)
		if err != nil {
			return nil, fmt.Errorf("item.SourceData: %v", err)
		}
		if item.Block < 0 || item.Block >= len(headers)/48 {
			return nil, fmt.Errorf("bad block index: %d", item.Block)
		}
		header := headers[item.Block*48 : (item.Block+1)*48]
		merkleRoot := header[16:]
		if !cache.VerifyProof(merkleRoot, data, item.MerkleProof, item.Index, item.NumLeaves) {
			return nil, fmt.Errorf("cache.VerifyProof: bad proof")
		}
		full := fullItem{source: item}
		if item.Index < item.NumMinerPayouts {
			var payout types.SiacoinOutput
			if err := encoding.Unmarshal(data, &payout); err != nil {
				return nil, fmt.Errorf("encoding.Unmarshal payout: %v", err)
			}
			full.payout = &payout
		} else {
			var tx types.Transaction
			if err := encoding.Unmarshal(data, &tx); err != nil {
				return nil, fmt.Errorf("encoding.Unmarshal tx: %v", err)
			}
			full.tx = &tx
		}
		fullItems = append(fullItems, full)
	}
	return fullItems, nil
}

func main() {
	flag.Parse()
	respHeaders, err := http.Get("http://" + *server + "/v1/headers")
	if err != nil {
		panic(err)
	}
	headers, err := ioutil.ReadAll(respHeaders.Body)
	if err != nil {
		panic(err)
	}
	respHeaders.Body.Close()
	history, err := addressHistory(*address, headers)
	if err != nil {
		panic(err)
	}
	for _, full := range history {
		if full.payout != nil {
			fmt.Println(full.source.Block, "miner_payout")
		} else {
			fmt.Println(full.source.Block, "transaction")
		}
	}
}
