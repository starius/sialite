package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/julienschmidt/httprouter"
	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/Sia/types"
)

func (db *Database) handleBlocks(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	startWith := r.URL.Query().Get("startwith")
	headers, err := db.headers(startWith)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "db.headers: %v.\n", err)
		return
	}
	enc := json.NewEncoder(w)
	enc.Encode(headers)
}

func (db *Database) handleBlock(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	idhex := ps.ByName("idhex")
	var idhash crypto.Hash
	if err := idhash.LoadString(idhex); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "id.LoadString: %v.\n", err)
		return
	}
	id := types.BlockID(idhash)
	block, has := db.id2block[id]
	if !has {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "no block with id %q.\n", idhex)
		return
	}
	enc := json.NewEncoder(w)
	enc.Encode(db.wrapBlock(block))
}

func (db *Database) handleBlocki(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	index, err := strconv.Atoi(ps.ByName("i"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "strconv.Atoi: %v.\n", err)
		return
	}
	if index < 0 || index > len(db.height2block) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "No block with height %d.\n", index)
		return
	}
	block := db.height2block[index]
	enc := json.NewEncoder(w)
	enc.Encode(db.wrapBlock(block))
}

func (db *Database) handleTx(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	idhex := ps.ByName("idhex")
	var idhash crypto.Hash
	if err := idhash.LoadString(idhex); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "id.LoadString: %v.\n", err)
		return
	}
	id := types.TransactionID(idhash)
	tx, has := db.id2tx[id]
	if !has {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "no transaction with id %q.\n", idhex)
		return
	}
	enc := json.NewEncoder(w)
	enc.Encode(db.wrapTx(tx))
}

func (db *Database) handleContract(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	idhex := ps.ByName("idhex")
	var idhash crypto.Hash
	if err := idhash.LoadString(idhex); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "id.LoadString: %v.\n", err)
		return
	}
	id := types.FileContractID(idhash)
	history, has := db.id2history[id]
	if !has {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "no contract with id %q.\n", idhex)
		return
	}
	data := db.contractHistory(history)
	enc := json.NewEncoder(w)
	enc.Encode(data)
}

func (db *Database) handleAddress(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	idhex := ps.ByName("idhex")
	startWith := r.URL.Query().Get("startwith")
	var id types.UnlockHash
	if err := id.LoadString(idhex); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "id.LoadString: %v.\n", err)
		return
	}
	history, err := db.addressHistory(id, startWith)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "db.addressHistory: %v.\n", err)
		return
	}
	enc := json.NewEncoder(w)
	enc.Encode(history)
}

func (db *Database) handleSiacoinOutput(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	idhex := ps.ByName("idhex")
	var idhash crypto.Hash
	if err := idhash.LoadString(idhex); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "id.LoadString: %v.\n", err)
		return
	}
	id := types.SiacoinOutputID(idhash)
	sco, has := db.id2sco[id]
	if !has {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "no Siacoin output with id %q.\n", idhex)
		return
	}
	data := db.siacoinOutput(sco)
	enc := json.NewEncoder(w)
	enc.Encode(data)
}

func (db *Database) handleSiafundOutput(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	idhex := ps.ByName("idhex")
	var idhash crypto.Hash
	if err := idhash.LoadString(idhex); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "id.LoadString: %v.\n", err)
		return
	}
	id := types.SiafundOutputID(idhash)
	sco, has := db.id2sfo[id]
	if !has {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "no Siafund output with id %q.\n", idhex)
		return
	}
	data := db.siafundOutput(sco)
	enc := json.NewEncoder(w)
	enc.Encode(data)
}

func (db *Database) handleHash(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	idhex := ps.ByName("idhex")
	if len(idhex) == 76 {
		db.handleAddress(w, r, ps)
		return
	}
	var id crypto.Hash
	if err := id.LoadString(idhex); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "id.LoadString: %v.\n", err)
		return
	}
	if _, has := db.id2block[types.BlockID(id)]; has {
		db.handleBlock(w, r, ps)
		return
	} else if _, has := db.id2tx[types.TransactionID(id)]; has {
		db.handleTx(w, r, ps)
		return
	} else if _, has := db.id2history[types.FileContractID(id)]; has {
		db.handleContract(w, r, ps)
		return
	} else if _, has := db.id2sco[types.SiacoinOutputID(id)]; has {
		db.handleSiacoinOutput(w, r, ps)
		return
	} else if _, has := db.id2sfo[types.SiafundOutputID(id)]; has {
		db.handleSiafundOutput(w, r, ps)
		return
	} else {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("Can't recognize the hash.\n"))
		return
	}
}

func (db *Database) addHandlers(router *httprouter.Router) {
	router.GET("/blocks", db.handleBlocks)
	router.GET("/block/:idhex", db.handleBlock)
	router.GET("/blocki/:i", db.handleBlocki)
	router.GET("/tx/:idhex", db.handleTx)
	router.GET("/contract/:idhex", db.handleContract)
	router.GET("/address/:idhex", db.handleAddress)
	router.GET("/siacoin-output/:idhex", db.handleSiacoinOutput)
	router.GET("/siafund-output/:idhex", db.handleSiafundOutput)
	router.GET("/hash/:idhex", db.handleHash)
}
