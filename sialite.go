package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/NebulousLabs/Sia/encoding"
	"github.com/NebulousLabs/Sia/modules"
	"github.com/NebulousLabs/Sia/modules/consensus"
	"github.com/NebulousLabs/Sia/types"
	"github.com/NebulousLabs/fastrand"
	"github.com/julienschmidt/httprouter"
	"github.com/xtaci/smux"
)

var (
	addr   = flag.String("addr", ":8080", "HTTP API address")
	source = flag.String("source", "", "Source of data (siad node)")
)

type sessionHeader struct {
	GenesisID  types.BlockID
	UniqueID   [8]byte
	NetAddress modules.NetAddress
}

func connect(node string) (net.Conn, error) {
	fmt.Println("Using node: ", node)
	conn, err := net.Dial("tcp", node)
	if err != nil {
		return nil, err
	}
	version := "1.3.0"
	if err := encoding.WriteObject(conn, version); err != nil {
		return nil, err
	}
	if err := encoding.ReadObject(conn, &version, uint64(100)); err != nil {
		return nil, err
	}
	fmt.Println(version)
	sh := sessionHeader{
		GenesisID:  types.GenesisID,
		UniqueID:   [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
		NetAddress: modules.NetAddress("62.210.92.11:1111"),
	}
	if err := encoding.WriteObject(conn, sh); err != nil {
		return nil, err
	}
	var response string
	if err := encoding.ReadObject(conn, &response, 100); err != nil {
		return nil, fmt.Errorf("failed to read header acceptance: %v", err)
	} else if response == modules.StopResponse {
		return nil, errors.New("peer did not want a connection")
	} else if response != modules.AcceptResponse {
		return nil, fmt.Errorf("peer rejected our header: %v", response)
	}
	if err := encoding.ReadObject(conn, &sh, uint64(100)); err != nil {
		return nil, err
	}
	if err := encoding.WriteObject(conn, modules.AcceptResponse); err != nil {
		return nil, err
	}
	return conn, nil
}

func downloadBlocks(bchan chan *types.Block, prevBlockID *types.BlockID, conn net.Conn) (bool, error) {
	var rpcName [8]byte
	copy(rpcName[:], "SendBlocks")
	if err := encoding.WriteObject(conn, rpcName); err != nil {
		return false, err
	}
	var history [32]types.BlockID
	history[31] = types.GenesisID
	moreAvailable := true
	// Send the block ids.
	history[0] = *prevBlockID
	if err := encoding.WriteObject(conn, history); err != nil {
		return false, err
	}
	var hadBlocks bool
	for moreAvailable {
		// Read a slice of blocks from the wire.
		var newBlocks []types.Block
		if err := encoding.ReadObject(conn, &newBlocks, uint64(consensus.MaxCatchUpBlocks)*types.BlockSizeLimit); err != nil {
			return hadBlocks, err
		}
		if err := encoding.ReadObject(conn, &moreAvailable, 1); err != nil {
			return hadBlocks, err
		}
		log.Printf("moreAvailable = %v.", moreAvailable)
		for i := range newBlocks {
			b := &newBlocks[i]
			if b.ParentID != *prevBlockID {
				return hadBlocks, fmt.Errorf("parent: %s, prev: %s", b.ParentID, *prevBlockID)
			}
			log.Printf("Downloaded block %s.", b.ID())
			hadBlocks = true
			bchan <- b
			*prevBlockID = b.ID()
		}
		// //test
		// return true, nil
	}
	return hadBlocks, nil
}

func downloadAllBlocks(bchan chan *types.Block, sess *smux.Session) error {
	prevBlockID := types.GenesisID
	for {
		stream, err := sess.OpenStream()
		if err != nil {
			return err
		}
		hadBlocks, err := downloadBlocks(bchan, &prevBlockID, stream)
		log.Printf("downloadBlocks returned %v, %v.", hadBlocks, err)
		if err == nil {
			log.Printf("No error, all blocks were downloaded. Stopping.")
			break
		}
		if (err != io.EOF && err != io.ErrUnexpectedEOF) || !hadBlocks {
			return err
		}
	}
	return nil
}

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

func (db *Database) addBlock(block *types.Block) {
	height := len(db.height2block)
	id := block.ID()
	log.Printf("processing block %d %s.", height, id)
	db.block2height[block] = height
	db.block2id[block] = id
	db.height2block = append(db.height2block, block)
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
}

func processBlocks(bchan chan *types.Block) (*Database, error) {
	log.Printf("processBlocks")
	db := NewDatabase()
	for block := range bchan {
		db.addBlock(block)
	}
	return db, nil
}

func (db *Database) fetchBlocks(sess *smux.Session) error {
	stream, err := sess.OpenStream()
	if err != nil {
		return err
	}
	defer stream.Close()
	bchan := make(chan *types.Block, 20)
	prevBlock := db.height2block[len(db.height2block)-1]
	prevBlockID := db.block2id[prevBlock]
	hadBlocks, err := downloadBlocks(bchan, &prevBlockID, stream)
	if err != nil {
		return err
	}
	close(bchan)
	if hadBlocks {
		db.mu.Lock()
		defer db.mu.Unlock()
		for block := range bchan {
			db.addBlock(block)
		}
	}
	return nil
}

func main() {
	flag.Parse()
	node := *source
	if node == "" {
		i := fastrand.Intn(len(modules.BootstrapPeers))
		node = string(modules.BootstrapPeers[i])
	}
	conn, err := connect(node)
	if err != nil {
		panic(err)
	}
	sess, err := smux.Client(conn, nil)
	if err != nil {
		panic(err)
	}
	bchan := make(chan *types.Block, 1000)
	bchan <- &types.GenesisBlock
	var db *Database
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := downloadAllBlocks(bchan, sess); err != nil {
			panic(err)
		}
		close(bchan)
	}()
	go func() {
		defer wg.Done()
		if db, err = processBlocks(bchan); err != nil {
			panic(err)
		}
	}()
	wg.Wait()

	fmt.Println("Initial block download completed.")

	fmt.Printf("len(tx2block) = %d\n", len(db.tx2block))
	fmt.Printf("len(id2sco) = %d\n", len(db.id2sco))
	fmt.Printf("len(id2sfo) = %d\n", len(db.id2sfo))
	fmt.Printf("len(id2sci) = %d\n", len(db.id2sci))
	fmt.Printf("len(id2sfi) = %d\n", len(db.id2sfi))
	fmt.Printf("len(address2sco) = %d\n", len(db.address2sco))
	fmt.Printf("len(address2sfo) = %d\n", len(db.address2sfo))
	fmt.Printf("len(id2history) = %d\n", len(db.id2history))

	go func() {
		for range time.NewTicker(5 * time.Second).C {
			db.fetchBlocks(sess)
		}
	}()

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