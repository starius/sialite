package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/NebulousLabs/Sia/types"
	"github.com/starius/sialite/human"
)

func (db *Database) source0(block *types.Block, index int) *human.Source {
	return &human.Source{
		Block:  db.block2id[block],
		Blocki: db.block2height[block],
		Index:  index,
	}
}

func (db *Database) source(tx *types.Transaction, index int) *human.Source {
	block := db.tx2block[tx]
	source := db.source0(block, index)
	txid := tx.ID()
	source.Tx = &txid
	return source
}

func (db *Database) contractHistory(history *ContractHistory) *human.ContractHistory {
	contractTx := history.contract.tx
	contract := &contractTx.FileContracts[history.contract.index]
	fcid := contractTx.FileContractID(uint64(history.contract.index))
	h := &human.ContractHistory{
		Contract: human.Contract{
			ID:             fcid,
			Source:         db.source(contractTx, history.contract.index),
			FileSize:       contract.FileSize,
			FileMerkleRoot: contract.FileMerkleRoot,
			WindowStart:    contract.WindowStart,
			WindowEnd:      contract.WindowEnd,
			Payout:         contract.Payout,
			UnlockHash:     contract.UnlockHash,
			RevisionNumber: contract.RevisionNumber,
		},
	}
	for i := range contract.ValidProofOutputs {
		sco := &contract.ValidProofOutputs[i]
		outid := fcid.StorageProofOutputID(types.ProofValid, uint64(i))
		hsco := &human.SiacoinOutput{
			SiacoinOutput: sco,
			ID:            outid,
		}
		sci, has := db.id2sci[outid]
		if has {
			hsco.Spent = db.source(sci.tx, sci.index)
		}
		h.Contract.ValidProofOutputs = append(h.Contract.ValidProofOutputs, hsco)
	}
	for i := range contract.MissedProofOutputs {
		sco := &contract.MissedProofOutputs[i]
		outid := fcid.StorageProofOutputID(types.ProofMissed, uint64(i))
		hsco := &human.SiacoinOutput{
			SiacoinOutput: sco,
			ID:            outid,
		}
		sci, has := db.id2sci[outid]
		if has {
			hsco.Spent = db.source(sci.tx, sci.index)
		}
		h.Contract.MissedProofOutputs = append(h.Contract.MissedProofOutputs, hsco)
	}
	for _, rev := range history.revs {
		r := &rev.tx.FileContractRevisions[rev.index]
		hrev := human.Revision{
			Source:            db.source(rev.tx, rev.index),
			ParentID:          r.ParentID,
			UnlockConditions:  r.UnlockConditions,
			NewRevisionNumber: r.NewRevisionNumber,
			NewFileSize:       r.NewFileSize,
			NewFileMerkleRoot: r.NewFileMerkleRoot,
			NewWindowStart:    r.NewWindowStart,
			NewWindowEnd:      r.NewWindowEnd,
			NewUnlockHash:     r.NewUnlockHash,
		}
		for i := range r.NewValidProofOutputs {
			sco := &r.NewValidProofOutputs[i]
			outid := fcid.StorageProofOutputID(types.ProofValid, uint64(i))
			hsco := &human.SiacoinOutput{
				SiacoinOutput: sco,
				ID:            outid,
			}
			sci, has := db.id2sci[outid]
			if has {
				hsco.Spent = db.source(sci.tx, sci.index)
			}
			hrev.NewValidProofOutputs = append(hrev.NewValidProofOutputs, hsco)
		}
		for i := range r.NewMissedProofOutputs {
			sco := &r.NewMissedProofOutputs[i]
			outid := fcid.StorageProofOutputID(types.ProofMissed, uint64(i))
			hsco := &human.SiacoinOutput{
				SiacoinOutput: sco,
				ID:            outid,
			}
			sci, has := db.id2sci[outid]
			if has {
				hsco.Spent = db.source(sci.tx, sci.index)
			}
			hrev.NewMissedProofOutputs = append(hrev.NewMissedProofOutputs, hsco)
		}
		h.Revisions = append(h.Revisions, hrev)
	}
	if history.proof != nil {
		h.Proof = &human.Proof{
			StorageProof: &history.proof.tx.StorageProofs[history.proof.index],
			Source:       db.source(history.proof.tx, history.proof.index),
		}
	}
	return h
}

func (db *Database) scoSource(sco *SiacoinOutput) *human.Source {
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

func (db *Database) wrapTx(tx *types.Transaction) *human.Transaction {
	block := db.tx2block[tx]
	ht := &human.Transaction{
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
		hsci := &human.SiacoinInput{
			SiacoinInput: sci,
			Parent:       sco.Value(db),
			Source:       db.scoSource(sco),
		}
		ht.SiacoinInputs = append(ht.SiacoinInputs, hsci)
	}
	for i := range tx.SiacoinOutputs {
		sco := &tx.SiacoinOutputs[i]
		outid := tx.SiacoinOutputID(uint64(i))
		hsco := &human.SiacoinOutput{
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
		hsfi := &human.SiafundInput{
			SiafundInput: sfi,
			Parent:       sfo.Value(),
			Source:       source,
		}
		ht.SiafundInputs = append(ht.SiafundInputs, hsfi)
		// Claim.
		claimid := sfi.ParentID.SiaClaimOutputID()
		sco := db.id2sco[claimid]
		hsco := &human.SiacoinOutput{
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
		hsfo := &human.SiafundOutput{
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
		fcid := tx.FileContractID(uint64(i))
		history := db.id2history[fcid]
		ht.FileContracts = append(ht.FileContracts, &human.FileContract{
			ID:      fcid,
			History: db.contractHistory(history),
		})
	}
	for i := range tx.FileContractRevisions {
		rev := &tx.FileContractRevisions[i]
		history := db.id2history[rev.ParentID]
		ht.FileContractRevisions = append(ht.FileContractRevisions, &human.FileContractRevision{
			Index:   i,
			History: db.contractHistory(history),
		})
	}
	for i := range tx.StorageProofs {
		proof := &tx.StorageProofs[i]
		history := db.id2history[proof.ParentID]
		ht.StorageProofs = append(ht.StorageProofs, &human.StorageProof{
			History: db.contractHistory(history),
		})
	}
	return ht
}

func (db *Database) wrapBlock(block *types.Block) *human.Block {
	hb := &human.Block{
		Height:    db.block2height[block],
		ID:        db.block2id[block],
		ParentID:  block.ParentID,
		Nonce:     block.Nonce,
		Timestamp: block.Timestamp,
	}
	for i := range block.MinerPayouts {
		sco := &block.MinerPayouts[i]
		outid := block.MinerPayoutID(uint64(i))
		hsco := &human.SiacoinOutput{
			SiacoinOutput: sco,
			ID:            outid,
		}
		sci, has := db.id2sci[outid]
		if has {
			hsco.Spent = db.source(sci.tx, sci.index)
		}
		hb.MinerPayouts = append(hb.MinerPayouts, hsco)
	}
	for i := range block.Transactions {
		tx := &block.Transactions[i]
		hb.Transactions = append(hb.Transactions, db.wrapTx(tx))
	}
	return hb
}

func (db *Database) siacoinOutput(sco *SiacoinOutput) *human.SiacoinRecord {
	r := &human.SiacoinRecord{
		Income: &human.SiacoinOutput{
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

func (db *Database) siafundOutput(sfo *SiafundOutput) *human.SiafundRecord {
	r := &human.SiafundRecord{
		Income: &human.SiafundOutput{
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

type historyPosition struct {
	ScoStart, SfoStart uint32
}

func (db *Database) addressHistory(address types.UnlockHash, startWith string) (*human.AddressHistory, error) {
	scos := db.address2sco[address]
	sfos := db.address2sfo[address]
	var hi historyPosition
	if startWith != "" {
		startWithBytes, err := hex.DecodeString(startWith)
		if err != nil {
			return nil, fmt.Errorf("hex.DecodeString: %v", err)
		}
		if err := json.Unmarshal(startWithBytes, &hi); err != nil {
			return nil, fmt.Errorf("json.Unmarshal: %v", err)
		}
		if hi.ScoStart > uint32(len(scos)) {
			return nil, fmt.Errorf("ScoStart is too high")
		}
		if hi.SfoStart > uint32(len(sfos)) {
			return nil, fmt.Errorf("SfoStart is too high")
		}
	} else {
		hi.ScoStart = uint32(len(scos))
		hi.SfoStart = uint32(len(sfos))
	}
	const maxRecords = 20
	quota := maxRecords
	h := &human.AddressHistory{
		UnlockHash:        address,
		SiacoinHistoryLen: len(scos),
		SiafundHistoryLen: len(sfos),
	}
	for i := int(hi.SfoStart) - 1; i >= 0; i-- {
		if quota == 0 {
			break
		}
		h.SiafundHistory = append(h.SiafundHistory, db.siafundOutput(sfos[i]))
		quota--
		hi.SfoStart--
	}
	for i := int(hi.ScoStart) - 1; i >= 0; i-- {
		if quota == 0 {
			break
		}
		h.SiacoinHistory = append(h.SiacoinHistory, db.siacoinOutput(scos[i]))
		quota--
		hi.ScoStart--
	}
	if hi.ScoStart != 0 || hi.SfoStart != 0 {
		nextBytes, err := json.Marshal(hi)
		if err != nil {
			return nil, fmt.Errorf("json.Marshal: %v", err)
		}
		h.Next = hex.EncodeToString(nextBytes)
	}
	return h, nil
}
