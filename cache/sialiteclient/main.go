package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/NebulousLabs/Sia/encoding"
	"github.com/starius/sialite/cache"
)

var (
	server  = flag.String("server", "127.0.0.1:35813", "Target address")
	address = flag.String("address", "", "Target address")
)

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
	next := ""
	var wholeHistory []cache.Item
	for {
		url := fmt.Sprintf("http://%s/v1/address-history?address=%s&start=%s", *server, *address, next)
		respHistory, err := http.Get(url)
		if err != nil {
			panic(err)
		}
		if respHistory.StatusCode != http.StatusOK {
			panic(respHistory.StatusCode)
		}
		var history []cache.Item
		if err := encoding.NewDecoder(respHistory.Body).DecodeAll(&next, &history); err != nil {
			panic(err)
		}
		respHistory.Body.Close()
		wholeHistory = append(wholeHistory, history...)
		if next == "" {
			break
		}
	}
	for _, item := range wholeHistory {
		data, err := item.SourceData(nil)
		if err != nil {
			panic(err)
		}
		header := headers[item.Block*48 : (item.Block+1)*48]
		merkleRoot := header[16:]
		if !cache.VerifyProof(merkleRoot, data, item.MerkleProof, item.Index, item.NumLeaves) {
			panic("bad proof")
		}
		if item.Index < item.NumMinerPayouts {
			fmt.Println(item.Block, "miner_payout")
		} else {
			fmt.Println(item.Block, "transaction")
		}
	}
}
