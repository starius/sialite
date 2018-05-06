package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/NebulousLabs/Sia/encoding"
	"github.com/NebulousLabs/Sia/types"
	"github.com/starius/sialite/cache"
)

var (
	files = flag.String("files", "", "Dir with output of builder")
	addr  = flag.String("addr", ":35813", "Address to run HTTP server")

	s *cache.Server
)

func handler(w http.ResponseWriter, r *http.Request) {
	addressHex := r.URL.Query().Get("address")
	var address types.UnlockHash
	if err := address.LoadString(addressHex); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "address.LoadString(%q): %v.\n", addressHex, err)
		return
	}
	addressBytes := address[:]
	history, next, err := s.GetHistory(addressBytes, "")
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "GetHistory: %v.\n", err)
		return
	}
	if len(history) == 0 {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "Not found.\n")
		return
	}
	l := 8 + len(next) + 8 + len(history)*(8+8+8+8)
	for _, item := range history {
		l += len(item.Data)
	}
	w.Header().Set("Content-Length", fmt.Sprintf("%d", l))
	w.WriteHeader(http.StatusOK)
	e := encoding.NewEncoder(w)
	if err := e.EncodeAll(next, history); err != nil {
		return
	}
}

func main() {
	flag.Parse()
	s1, err := cache.NewServer(*files)
	if err != nil {
		log.Fatalf("cache.NewBuilder: %v", err)
	}
	s = s1
	http.HandleFunc("/v1/history", handler)
	log.Fatal(http.ListenAndServe(*addr, nil))
}
