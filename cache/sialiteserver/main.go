package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/NebulousLabs/Sia/crypto"
	"github.com/NebulousLabs/Sia/encoding"
	"github.com/NebulousLabs/Sia/types"
	"github.com/starius/sialite/cache"
)

var (
	files = flag.String("files", "", "Dir with output of builder")
	addr  = flag.String("addr", ":35813", "Address to run HTTP server")

	s *cache.Server
)

func encodingLen(history []cache.Item, next string) int {
	l := 8 + len(next) + 8 + len(history)*(8+8+8+8+8+8+8)
	for _, item := range history {
		l += len(item.Data) + len(item.MerkleProof)
	}
	return l
}

func handleAddressHistory(w http.ResponseWriter, r *http.Request) {
	addressHex := r.URL.Query().Get("address")
	var address types.UnlockHash
	if err := address.LoadString(addressHex); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "address.LoadString(%q): %v.\n", addressHex, err)
		log.Printf("address.LoadString(%q): %v.\n", addressHex, err)
		return
	}
	addressBytes := address[:]
	start := r.URL.Query().Get("start")
	history, next, err := s.AddressHistory(addressBytes, start)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "AddressHistory: %v.\n", err)
		log.Printf("AddressHistory: %v.\n", err)
		return
	}
	if len(history) == 0 {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "Not found.\n")
		log.Printf("Not found.\n")
		return
	}
	l := encodingLen(history, next)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", l))
	w.WriteHeader(http.StatusOK)
	e := encoding.NewEncoder(w)
	if err := e.EncodeAll(next, history); err != nil {
		return
	}
}

func handleContractHistory(w http.ResponseWriter, r *http.Request) {
	contractHex := r.URL.Query().Get("contract")
	var id crypto.Hash
	if err := id.LoadString(contractHex); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "LoadString(%q): %v.\n", contractHex, err)
		log.Printf("LoadString(%q): %v.\n", contractHex, err)
		return
	}
	contractBytes := id[:]
	start := r.URL.Query().Get("start")
	history, next, err := s.ContractHistory(contractBytes, start)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "ContractHistory: %v.\n", err)
		log.Printf("ContractHistory: %v.\n", err)
		return
	}
	if len(history) == 0 {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "Not found.\n")
		log.Printf("Not found.\n")
		return
	}
	l := encodingLen(history, next)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", l))
	w.WriteHeader(http.StatusOK)
	e := encoding.NewEncoder(w)
	if err := e.EncodeAll(next, history); err != nil {
		return
	}
}

func handleHeaders(w http.ResponseWriter, r *http.Request) {
	reader := bytes.NewReader(s.Headers)
	modTime := time.Now() // FIXME: set to last block timestamp.
	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeContent(w, r, "headers", modTime, reader)
}

func main() {
	flag.Parse()
	s1, err := cache.NewServer(*files)
	if err != nil {
		log.Fatalf("cache.NewServer: %v", err)
	}
	s = s1
	http.HandleFunc("/v1/address-history", handleAddressHistory)
	http.HandleFunc("/v1/contract-history", handleContractHistory)
	http.HandleFunc("/v1/headers", handleHeaders)
	log.Fatal(http.ListenAndServe(*addr, nil))
}
