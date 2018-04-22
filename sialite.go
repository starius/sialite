package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/NebulousLabs/Sia/types"
	"github.com/julienschmidt/httprouter"
	"github.com/starius/sialite/netlib"
	"github.com/starius/sialite/store"
	"github.com/xtaci/smux"
)

var (
	addr       = flag.String("addr", ":8080", "HTTP API address")
	blockchain = flag.String("blockchain", "", "Input file with blockchain")
	source     = flag.String("source", "", "Source of data (siad node)")
	files      = flag.String("files", "", "Dir to write files")
	nblocks    = flag.Int("nblocks", 0, "Approximate max number of blocks (0 = all)")
)

const (
	// For SiacoinOutput.nature.
	siacoinOutput               = iota
	minerPayout                 = iota
	validProofOutput            = iota
	missedProofOutput           = iota
	validProofOutputInRevision  = iota
	missedProofOutputInRevision = iota
	siaClaimOutput              = iota
)

func natureStr(nature int) string {
	if nature == siacoinOutput {
		return "siacoin_output"
	} else if nature == minerPayout {
		return "miner_payout"
	} else if nature == validProofOutput {
		return "valid_proof_output"
	} else if nature == missedProofOutput {
		return "missed_proof_output"
	} else if nature == validProofOutputInRevision {
		return "valid_proof_output_in_revision"
	} else if nature == missedProofOutputInRevision {
		return "missed_proof_output_in_revision"
	} else if nature == siaClaimOutput {
		return "sia_claim_output"
	} else {
		panic("unknown nature")
	}
}

type SiacoinOutput struct {
	tx     *types.Transaction // Empty for MinerPayouts.
	block  *types.Block
	nature int
	index  int // Index of SiacoinOutput in slice.
	index0 int // Index of FileContract or FileContractRevision.
}

func (o *SiacoinOutput) ID() types.SiacoinOutputID {
	if o.nature == siacoinOutput {
		return o.tx.SiacoinOutputID(uint64(o.index))
	} else if o.nature == minerPayout {
		return o.block.MinerPayoutID(uint64(o.index))
	} else if o.nature == validProofOutput {
		return o.tx.FileContractID(uint64(o.index0)).StorageProofOutputID(types.ProofValid, uint64(o.index))
	} else if o.nature == missedProofOutput {
		return o.tx.FileContractID(uint64(o.index0)).StorageProofOutputID(types.ProofMissed, uint64(o.index))
	} else if o.nature == validProofOutputInRevision {
		return o.tx.FileContractRevisions[o.index0].ParentID.StorageProofOutputID(types.ProofValid, uint64(o.index))
	} else if o.nature == missedProofOutputInRevision {
		return o.tx.FileContractRevisions[o.index0].ParentID.StorageProofOutputID(types.ProofMissed, uint64(o.index))
	} else if o.nature == siaClaimOutput {
		return o.tx.SiafundInputs[o.index].ParentID.SiaClaimOutputID()
	} else {
		panic("Unknown nature")
	}
}

func (o *SiacoinOutput) Value(db *Database) *types.SiacoinOutput {
	if o.nature == siacoinOutput {
		return &o.tx.SiacoinOutputs[o.index]
	} else if o.nature == minerPayout {
		return &o.block.MinerPayouts[o.index]
	} else if o.nature == validProofOutput {
		return &o.tx.FileContracts[o.index0].ValidProofOutputs[o.index]
	} else if o.nature == missedProofOutput {
		return &o.tx.FileContracts[o.index0].MissedProofOutputs[o.index]
	} else if o.nature == validProofOutputInRevision {
		return &o.tx.FileContractRevisions[o.index0].NewValidProofOutputs[o.index]
	} else if o.nature == missedProofOutputInRevision {
		return &o.tx.FileContractRevisions[o.index0].NewMissedProofOutputs[o.index]
	} else if o.nature == siaClaimOutput {
		sfi := &o.tx.SiafundInputs[o.index]
		sfoid := sfi.ParentID
		sfo := db.id2sfo[sfoid]
		block1 := db.block2height[db.tx2block[sfo.tx]]
		block2 := db.block2height[db.tx2block[o.tx]] - 1
		value := types.NewCurrency64(0)
		if block2 > block1 {
			diff := db.height2sfpool[block2].Sub(db.height2sfpool[block1])
			value = diff.Mul(sfo.Value().Value).Div64(10000)
		}
		return &types.SiacoinOutput{
			UnlockHash: sfi.ClaimUnlockHash,
			Value:      value,
		}
	} else {
		panic("Unknown nature")
	}
}

type SiafundOutput struct {
	tx    *types.Transaction
	index int // Index of SiafundOutput in slice.
}

func (o *SiafundOutput) ID() types.SiafundOutputID {
	return o.tx.SiafundOutputID(uint64(o.index))
}

func (o *SiafundOutput) Value() *types.SiafundOutput {
	return &o.tx.SiafundOutputs[o.index]
}

type SiacoinInput struct {
	tx    *types.Transaction
	index int // Index of SiacoinInput in slice.
}

func (i *SiacoinInput) ID() types.SiacoinOutputID {
	return i.Value().ParentID
}

func (i *SiacoinInput) Value() types.SiacoinInput {
	return i.tx.SiacoinInputs[i.index]
}

type SiafundInput struct {
	tx    *types.Transaction
	index int // Index of SiafundInput in slice.
}

func (i *SiafundInput) ID() types.SiafundOutputID {
	return i.Value().ParentID
}

func (i *SiafundInput) Value() types.SiafundInput {
	return i.tx.SiafundInputs[i.index]
}

type Contract struct {
	tx    *types.Transaction
	index int // Index of FileContract in slice.
}

func (c *Contract) Value() *types.FileContract {
	return &c.tx.FileContracts[c.index]
}

type ContractRev struct {
	tx    *types.Transaction
	index int // Index of FileContractRevision in slice.
}

func (c *ContractRev) Value() *types.FileContractRevision {
	return &c.tx.FileContractRevisions[c.index]
}

type StorageProof struct {
	tx    *types.Transaction
	index int // Index of StorageProof in slice.
}

func (s *StorageProof) Value() *types.StorageProof {
	return &s.tx.StorageProofs[s.index]
}

type ContractHistory struct {
	contract Contract
	revs     []ContractRev
	proof    *StorageProof
}

type Database struct {
	block2height  map[*types.Block]int
	block2id      map[*types.Block]types.BlockID
	height2block  []*types.Block
	height2sfpool []types.Currency
	id2block      map[types.BlockID]*types.Block
	id2tx         map[types.TransactionID]*types.Transaction
	tx2block      map[*types.Transaction]*types.Block
	id2sco        map[types.SiacoinOutputID]*SiacoinOutput
	id2sfo        map[types.SiafundOutputID]*SiafundOutput
	id2sci        map[types.SiacoinOutputID]*SiacoinInput
	id2sfi        map[types.SiafundOutputID]*SiafundInput
	address2sco   map[types.UnlockHash][]*SiacoinOutput
	address2sfo   map[types.UnlockHash][]*SiafundOutput
	id2history    map[types.FileContractID]*ContractHistory

	mu sync.RWMutex
}

func NewDatabase() *Database {
	return &Database{
		block2height: make(map[*types.Block]int),
		block2id:     make(map[*types.Block]types.BlockID),
		id2block:     make(map[types.BlockID]*types.Block),
		id2tx:        make(map[types.TransactionID]*types.Transaction),
		tx2block:     make(map[*types.Transaction]*types.Block),
		id2sco:       make(map[types.SiacoinOutputID]*SiacoinOutput),
		id2sfo:       make(map[types.SiafundOutputID]*SiafundOutput),
		id2sci:       make(map[types.SiacoinOutputID]*SiacoinInput),
		id2sfi:       make(map[types.SiafundOutputID]*SiafundInput),
		address2sco:  make(map[types.UnlockHash][]*SiacoinOutput),
		address2sfo:  make(map[types.UnlockHash][]*SiafundOutput),
		id2history:   make(map[types.FileContractID]*ContractHistory),
	}
}

func (db *Database) addSco(o *SiacoinOutput) {
	db.id2sco[o.ID()] = o
	a := o.Value(db).UnlockHash
	db.address2sco[a] = append(db.address2sco[a], o)
}

func (db *Database) addSfo(o *SiafundOutput) {
	db.id2sfo[o.ID()] = o
	a := o.Value().UnlockHash
	db.address2sfo[a] = append(db.address2sfo[a], o)
}

func (db *Database) addSci(i *SiacoinInput) {
	db.id2sci[i.ID()] = i
}

func (db *Database) addSfi(i *SiafundInput) {
	db.id2sfi[i.ID()] = i
	db.addSco(&SiacoinOutput{
		tx:     i.tx,
		block:  db.tx2block[i.tx],
		nature: siaClaimOutput,
		index:  i.index,
	})
}

func (db *Database) addBlock(block *types.Block, storage *store.Storage) error {
	height := len(db.height2block)
	db.height2block = append(db.height2block, block)
	id := block.ID()
	log.Printf("processing block %d %s.", height, id)
	if storage != nil {
		return storage.Add(block)
	}
	db.block2height[block] = height
	db.block2id[block] = id
	db.id2block[id] = block
	sfpool := types.NewCurrency64(0)
	if height != 0 {
		sfpool = db.height2sfpool[len(db.height2sfpool)-1]
	}
	for i := range block.MinerPayouts {
		db.addSco(&SiacoinOutput{
			block:  block,
			nature: minerPayout,
			index:  i,
		})
	}
	for j := range block.Transactions {
		tx := &block.Transactions[j]
		db.id2tx[tx.ID()] = tx
		db.tx2block[tx] = block
		for i := range tx.SiacoinInputs {
			db.addSci(&SiacoinInput{
				tx:    tx,
				index: i,
			})
		}
		for i := range tx.SiafundInputs {
			db.addSfi(&SiafundInput{
				tx:    tx,
				index: i,
			})
		}
		for i := range tx.SiacoinOutputs {
			db.addSco(&SiacoinOutput{
				block:  block,
				tx:     tx,
				nature: siacoinOutput,
				index:  i,
			})
		}
		for i := range tx.SiafundOutputs {
			db.addSfo(&SiafundOutput{
				tx:    tx,
				index: i,
			})
		}
		for i0, contract := range tx.FileContracts {
			fcid := tx.FileContractID(uint64(i0))
			h, has := db.id2history[fcid]
			if !has {
				h = &ContractHistory{}
				db.id2history[fcid] = h
			}
			h.contract = Contract{
				tx:    tx,
				index: i0,
			}
			sum := types.NewCurrency64(0)
			for i, o := range contract.ValidProofOutputs {
				db.addSco(&SiacoinOutput{
					tx:     tx,
					block:  block,
					nature: validProofOutput,
					index:  i,
					index0: i0,
				})
				sum = sum.Add(o.Value)
			}
			tax := contract.Payout.Sub(sum)
			for i := range contract.MissedProofOutputs {
				db.addSco(&SiacoinOutput{
					tx:     tx,
					block:  block,
					nature: missedProofOutput,
					index:  i,
					index0: i0,
				})
			}
			sfpool = sfpool.Add(tax)
		}
		for i0, rev := range tx.FileContractRevisions {
			h := db.id2history[rev.ParentID]
			h.revs = append(h.revs, ContractRev{
				tx:    tx,
				index: i0,
			})
			for i := range rev.NewValidProofOutputs {
				db.addSco(&SiacoinOutput{
					tx:     tx,
					block:  block,
					nature: validProofOutputInRevision,
					index:  i,
					index0: i0,
				})
			}
			for i := range rev.NewMissedProofOutputs {
				db.addSco(&SiacoinOutput{
					tx:     tx,
					block:  block,
					nature: missedProofOutputInRevision,
					index:  i,
					index0: i0,
				})
			}
		}
		for i0, proof := range tx.StorageProofs {
			db.id2history[proof.ParentID].proof = &StorageProof{
				tx:    tx,
				index: i0,
			}
		}
	}
	db.height2sfpool = append(db.height2sfpool, sfpool)
	return nil
}

func processBlocks(ctx context.Context, bchan chan *types.Block, storage *store.Storage) (*Database, error) {
	log.Printf("processBlocks")
	db := NewDatabase()
	i := 0
	for block := range bchan {
		i++
		if *nblocks != 0 && i > *nblocks {
			log.Printf("processBlocks got %d blocks", *nblocks)
			break
		}
		if err := db.addBlock(block, storage); err != nil {
			return nil, err
		}
	}
	return db, nil
}

func (db *Database) fetchBlocks(ctx context.Context, sess *smux.Session, storage *store.Storage) error {
	stream, err := sess.OpenStream()
	if err != nil {
		return err
	}
	defer stream.Close()
	bchan := make(chan *types.Block, 20)
	prevBlock := db.height2block[len(db.height2block)-1]
	prevBlockID := db.block2id[prevBlock]
	if _, err := netlib.DownloadBlocks(ctx, bchan, stream, prevBlockID); err != nil {
		return err
	}
	close(bchan)
	db.mu.Lock()
	defer db.mu.Unlock()
	for block := range bchan {
		if err := db.addBlock(block, storage); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	flag.Parse()
	ctx := context.Background()
	var storage *store.Storage
	var err error
	if *files != "" {
		storage, err = store.New(*files)
		if err != nil {
			log.Fatalf("store.New: %v", err)
		}
	}
	sess, f, err := netlib.OpenOrConnect(ctx, *blockchain, *source)
	if err != nil {
		panic(err)
	}
	bchan := make(chan *types.Block, 1000)
	bchan <- &types.GenesisBlock
	var db *Database
	var wg sync.WaitGroup
	wg.Add(2)
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
	go func() {
		defer wg.Done()
		if db, err = processBlocks(ctx, bchan, storage); err != nil {
			panic(err)
		}
		cancel()
		for range bchan {
		}
	}()
	wg.Wait()

	fmt.Println("Initial block download completed.")

	if storage != nil {
		return
	}

	fmt.Printf("len(tx2block) = %d\n", len(db.tx2block))
	fmt.Printf("len(id2sco) = %d\n", len(db.id2sco))
	fmt.Printf("len(id2sfo) = %d\n", len(db.id2sfo))
	fmt.Printf("len(id2sci) = %d\n", len(db.id2sci))
	fmt.Printf("len(id2sfi) = %d\n", len(db.id2sfi))
	fmt.Printf("len(address2sco) = %d\n", len(db.address2sco))
	fmt.Printf("len(address2sfo) = %d\n", len(db.address2sfo))
	fmt.Printf("len(id2history) = %d\n", len(db.id2history))

	if sess != nil {
		go func() {
			for range time.NewTicker(5 * time.Second).C {
				ctx := context.Background()
				db.fetchBlocks(ctx, sess, storage)
			}
		}()
	}

	router := httprouter.New()
	db.addHandlers(router)
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		db.mu.RLock()
		defer db.mu.RUnlock()
		router.ServeHTTP(w, r)
	}
	log.Fatal(http.ListenAndServe(*addr, http.HandlerFunc(handler)))
}
