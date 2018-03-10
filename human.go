package main

import "github.com/NebulousLabs/Sia/types"

type HumanSource struct {
	Block  types.BlockID        `json:"block"`
	Blocki int                  `json:"blocki"`
	Tx     *types.TransactionID `json:"tx"`
	Index  int                  `json:"index"`

	// Only for SiacoinOutput.
	Nature string `json:"nature,omitempty"`
	Index0 *int   `json:"index0"`
}

type HumanSiacoinInput struct {
	*types.SiacoinInput
	Parent *types.SiacoinOutput `json:"parent"`
	Source *HumanSource         `json:"source"`
}

type HumanSiacoinOutput struct {
	*types.SiacoinOutput
	ID    types.SiacoinOutputID `json:"id"`
	Spent *HumanSource          `json:"spent"`
}

type HumanSiafundInput struct {
	*types.SiafundInput
	Parent *types.SiafundOutput `json:"parent"`
	Source *HumanSource         `json:"source"`
}

type HumanSiafundOutput struct {
	*types.SiafundOutput
	ID    types.SiafundOutputID `json:"id"`
	Spent *HumanSource          `json:"spent"`
}

type HumanContract struct {
	*types.FileContract
	ID     types.FileContractID `json:"id"`
	Source *HumanSource         `json:"source"`
}

type HumanRevision struct {
	*types.FileContractRevision
	Source *HumanSource `json:"source"`
}

type HumanProof struct {
	*types.StorageProof
	Source *HumanSource `json:"source"`
}

type HumanContractHistory struct {
	Contract  HumanContract   `json:"contract"`
	Revisions []HumanRevision `json:"revisions"`
	Proof     *HumanProof     `json:"proof"`
}

type HumanFileContract struct {
	*types.FileContract
	ID      types.FileContractID  `json:"id"`
	History *HumanContractHistory `json:"history"`
}

type HumanFileContractRevision struct {
	*types.FileContractRevision
	History *HumanContractHistory `json:"history"`
}

type HumanStorageProof struct {
	*types.StorageProof
	History *HumanContractHistory `json:"history"`
}

type HumanTransaction struct {
	ID                    types.TransactionID          `json:"id"`
	Block                 types.BlockID                `json:"block"`
	Blocki                int                          `json:"blocki"`
	Size                  int                          `json:"size"`
	SiacoinInputs         []*HumanSiacoinInput         `json:"siacoininputs"`
	SiacoinOutputs        []*HumanSiacoinOutput        `json:"siacoinoutputs"`
	FileContracts         []*HumanFileContract         `json:"filecontracts"`
	FileContractRevisions []*HumanFileContractRevision `json:"filecontractrevisions"`
	StorageProofs         []*HumanStorageProof         `json:"storageproofs"`
	SiafundInputs         []*HumanSiafundInput         `json:"siafundinputs"`
	SiafundOutputs        []*HumanSiafundOutput        `json:"siafundoutputs"`
	MinerFees             []types.Currency             `json:"minerfees"`
	ArbitraryData         [][]byte                     `json:"arbitrarydata"`
	TransactionSignatures []types.TransactionSignature `json:"transactionsignatures"`
}

func (db *Database) source0(block *types.Block, index int) *HumanSource {
	return &HumanSource{
		Block:  db.block2id[block],
		Blocki: db.block2height[block],
		Index:  index,
	}
}

func (db *Database) source(tx *types.Transaction, index int) *HumanSource {
	block := db.tx2block[tx]
	source := db.source0(block, index)
	txid := tx.ID()
	source.Tx = &txid
	return source
}

func (db *Database) contractHistory(history *ContractHistory) *HumanContractHistory {
	h := &HumanContractHistory{
		Contract: HumanContract{
			FileContract: &history.contract.tx.FileContracts[history.contract.index],
			ID:           history.contract.tx.FileContractID(uint64(history.contract.index)),
			Source:       db.source(history.contract.tx, history.contract.index),
		},
	}
	for _, rev := range history.revs {
		h.Revisions = append(h.Revisions, HumanRevision{
			FileContractRevision: &rev.tx.FileContractRevisions[rev.index],
			Source:               db.source(rev.tx, rev.index),
		})
	}
	if history.proof != nil {
		h.Proof = &HumanProof{
			StorageProof: &history.proof.tx.StorageProofs[history.proof.index],
			Source:       db.source(history.proof.tx, history.proof.index),
		}
	}
	return h
}

func (db *Database) scoSource(sco *SiacoinOutput) *HumanSource {
	source := db.source0(sco.block, sco.index)
	if sco.tx != nil {
		txid := sco.tx.ID()
		source.Tx = &txid
	}
	source.Nature = natureStr(sco.nature)
	if sco.nature == validProofOutput || sco.nature == missedProofOutput || sco.nature == validProofOutputInRevision || sco.nature == missedProofOutputInRevision {
		source.Index0 = &sco.index0
	}
	return source
}

func (db *Database) wrapTx(tx *types.Transaction) *HumanTransaction {
	block := db.tx2block[tx]
	ht := &HumanTransaction{
		ID:                    tx.ID(),
		Block:                 db.block2id[block],
		Blocki:                db.block2height[block],
		Size:                  tx.MarshalSiaSize(),
		MinerFees:             tx.MinerFees,
		ArbitraryData:         tx.ArbitraryData,
		TransactionSignatures: tx.TransactionSignatures,
	}
	for i := range tx.SiacoinInputs {
		sci := &tx.SiacoinInputs[i]
		sco := db.id2sco[sci.ParentID]
		hsci := &HumanSiacoinInput{
			SiacoinInput: sci,
			Parent:       sco.Value(db),
			Source:       db.scoSource(sco),
		}
		ht.SiacoinInputs = append(ht.SiacoinInputs, hsci)
	}
	for i := range tx.SiacoinOutputs {
		sco := &tx.SiacoinOutputs[i]
		outid := tx.SiacoinOutputID(uint64(i))
		hsco := &HumanSiacoinOutput{
			SiacoinOutput: sco,
			ID:            outid,
		}
		sci, has := db.id2sci[outid]
		if has {
			hsco.Spent = db.source(sci.tx, sci.index)
		}
		ht.SiacoinOutputs = append(ht.SiacoinOutputs, hsco)
	}
	for i := range tx.SiafundInputs {
		sfi := &tx.SiafundInputs[i]
		sfo := db.id2sfo[sfi.ParentID]
		source := db.source(sfo.tx, sfo.index)
		hsfi := &HumanSiafundInput{
			SiafundInput: sfi,
			Parent:       sfo.Value(),
			Source:       source,
		}
		ht.SiafundInputs = append(ht.SiafundInputs, hsfi)
		// Claim.
		claimid := sfi.ParentID.SiaClaimOutputID()
		sco := db.id2sco[claimid]
		hsco := &HumanSiacoinOutput{
			SiacoinOutput: sco.Value(db),
			ID:            claimid,
		}
		sci, has := db.id2sci[claimid]
		if has {
			hsco.Spent = db.source(sci.tx, sci.index)
		}
		ht.SiacoinOutputs = append(ht.SiacoinOutputs, hsco)
	}
	for i := range tx.SiafundOutputs {
		sfo := &tx.SiafundOutputs[i]
		outid := tx.SiafundOutputID(uint64(i))
		hsfo := &HumanSiafundOutput{
			SiafundOutput: sfo,
			ID:            outid,
		}
		sfi, has := db.id2sfi[outid]
		if has {
			hsfo.Spent = db.source(sfi.tx, sfi.index)
		}
		ht.SiafundOutputs = append(ht.SiafundOutputs, hsfo)
	}
	for i := range tx.FileContracts {
		contract := &tx.FileContracts[i]
		fcid := tx.FileContractID(uint64(i))
		history := db.id2history[fcid]
		ht.FileContracts = append(ht.FileContracts, &HumanFileContract{
			FileContract: contract,
			ID:           fcid,
			History:      db.contractHistory(history),
		})
	}
	for i := range tx.FileContractRevisions {
		rev := &tx.FileContractRevisions[i]
		history := db.id2history[rev.ParentID]
		ht.FileContractRevisions = append(ht.FileContractRevisions, &HumanFileContractRevision{
			FileContractRevision: rev,
			History:              db.contractHistory(history),
		})
	}
	for i := range tx.StorageProofs {
		proof := &tx.StorageProofs[i]
		history := db.id2history[proof.ParentID]
		ht.StorageProofs = append(ht.StorageProofs, &HumanStorageProof{
			StorageProof: proof,
			History:      db.contractHistory(history),
		})
	}
	return ht
}

type HumanBlock struct {
	Height       int                   `json:"height"`
	ID           types.BlockID         `json:"id"`
	ParentID     types.BlockID         `json:"parentid"`
	Nonce        types.BlockNonce      `json:"nonce"`
	Timestamp    types.Timestamp       `json:"timestamp"`
	MinerPayouts []types.SiacoinOutput `json:"minerpayouts"`
	Transactions []*HumanTransaction   `json:"transactions"`
}

func (db *Database) wrapBlock(block *types.Block) *HumanBlock {
	hb := &HumanBlock{
		Height:       db.block2height[block],
		ID:           db.block2id[block],
		ParentID:     block.ParentID,
		Nonce:        block.Nonce,
		Timestamp:    block.Timestamp,
		MinerPayouts: block.MinerPayouts,
	}
	for i := range block.Transactions {
		tx := &block.Transactions[i]
		hb.Transactions = append(hb.Transactions, db.wrapTx(tx))
	}
	return hb
}

type HumanSiacoinRecord struct {
	Income       *HumanSiacoinOutput `json:"income"`
	IncomeSource *HumanSource        `json:"income_source"`
	IncomeTx     *HumanTransaction   `json:"income_tx"`
	SpentTx      *HumanTransaction   `json:"spent_tx"`
}

type HumanSiafundRecord struct {
	Income       *HumanSiafundOutput `json:"income"`
	IncomeSource *HumanSource        `json:"income_source"`
	IncomeTx     *HumanTransaction   `json:"income_tx"`
	SpentTx      *HumanTransaction   `json:"spent_tx"`
}

type HumanAddressHistory struct {
	UnlockHash     types.UnlockHash      `json:"unlockhash"`
	SiacoinHistory []*HumanSiacoinRecord `json:"siacoin_history"`
	SiafundHistory []*HumanSiafundRecord `json:"siafund_history"`
}

func (db *Database) siacoinOutput(sco *SiacoinOutput) *HumanSiacoinRecord {
	r := &HumanSiacoinRecord{
		Income: &HumanSiacoinOutput{
			SiacoinOutput: sco.Value(db),
			ID:            sco.ID(),
		},
		IncomeSource: db.scoSource(sco),
	}
	if sco.tx != nil {
		r.IncomeTx = db.wrapTx(sco.tx)
	}
	outid := sco.ID()
	sci, has := db.id2sci[outid]
	if has {
		r.Income.Spent = db.source(sci.tx, sci.index)
		r.SpentTx = db.wrapTx(sci.tx)
	}
	return r
}

func (db *Database) siafundOutput(sfo *SiafundOutput) *HumanSiafundRecord {
	r := &HumanSiafundRecord{
		Income: &HumanSiafundOutput{
			SiafundOutput: sfo.Value(),
			ID:            sfo.ID(),
		},
	}
	if sfo.tx != nil {
		r.IncomeSource = db.source(sfo.tx, sfo.index)
		r.IncomeTx = db.wrapTx(sfo.tx)
	}
	outid := sfo.ID()
	sfi, has := db.id2sfi[outid]
	if has {
		r.Income.Spent = db.source(sfi.tx, sfi.index)
		r.SpentTx = db.wrapTx(sfi.tx)
	}
	return r
}

func (db *Database) addressHistory(address types.UnlockHash) *HumanAddressHistory {
	h := &HumanAddressHistory{
		UnlockHash: address,
	}
	for _, sco := range db.address2sco[address] {
		h.SiacoinHistory = append(h.SiacoinHistory, db.siacoinOutput(sco))
	}
	for _, sfo := range db.address2sfo[address] {
		h.SiafundHistory = append(h.SiafundHistory, db.siafundOutput(sfo))
	}
	return h
}
