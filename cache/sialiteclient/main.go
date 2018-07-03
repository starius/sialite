package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/NebulousLabs/Sia/encoding"
	"github.com/golang/snappy"
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
	respHistory, err := http.Get("http://" + *server + "/v1/history?address=" + *address)
	if err != nil {
		panic(err)
	}
	var next string
	var history []cache.Item
	if err := encoding.NewDecoder(respHistory.Body).DecodeAll(&next, &history); err != nil {
		panic(err)
	}
	respHistory.Body.Close()
	err = cache.VerifyBlockHeaders(headers)
	if err != nil {
		panic(err)
	}
	for _, item := range history {
		var data []byte
		if item.Compression == cache.NO_COMPRESSION {
			data = item.Data
		} else if item.Compression == cache.SNAPPY {
			data, err = snappy.Decode(nil, item.Data)
		} else {
			panic("unknown compression")
		}
		headerBytes := headers[item.Block*48 : (item.Block+1)*48]
		merkleRoot := headerBytes[16:]
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
