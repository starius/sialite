package human

import "gitlab.com/NebulousLabs/Sia/types"
import "gitlab.com/NebulousLabs/Sia/crypto"

type BlockHeader struct {
	ID        types.BlockID    `json:"id"`
	Nonce     types.BlockNonce `json:"nonce"`
	Timestamp types.Timestamp  `json:"timestamp"`
}

type BlockHeaders struct {
	Headers []BlockHeader `json:"headers"`
	Next    string        `json:"next"`
}

type Source struct {
	Block  types.BlockID        `json:"block"`
	Blocki int                  `json:"blocki"`
	Tx     *types.TransactionID `json:"tx"`
	Index  int                  `json:"index"`

	// Only for SiacoinOutput.
	Nature string `json:"nature,omitempty"`
	Index0 *int   `json:"index0"`
}

type SiacoinInput struct {
	*types.SiacoinInput
	Parent *types.SiacoinOutput `json:"parent"`
	Source *Source              `json:"source"`
}

type SiacoinOutput struct {
	*types.SiacoinOutput
	ID    types.SiacoinOutputID `json:"id"`
	Spent *Source               `json:"spent"`
}

type SiafundInput struct {
	*types.SiafundInput
	Parent *types.SiafundOutput `json:"parent"`
	Source *Source              `json:"source"`
}

type SiafundOutput struct {
	*types.SiafundOutput
	ID    types.SiafundOutputID `json:"id"`
	Spent *Source               `json:"spent"`
}

type Contract struct {
	ID                 types.FileContractID `json:"id"`
	Source             *Source              `json:"source"`
	FileSize           uint64               `json:"filesize"`
	FileMerkleRoot     crypto.Hash          `json:"filemerkleroot"`
	WindowStart        types.BlockHeight    `json:"windowstart"`
	WindowEnd          types.BlockHeight    `json:"windowend"`
	Payout             types.Currency       `json:"payout"`
	ValidProofOutputs  []*SiacoinOutput     `json:"validproofoutputs"`
	MissedProofOutputs []*SiacoinOutput     `json:"missedproofoutputs"`
	UnlockHash         types.UnlockHash     `json:"unlockhash"`
	RevisionNumber     uint64               `json:"revisionnumber"`
}

type Revision struct {
	Source *Source `json:"source"`

	ParentID          types.FileContractID   `json:"parentid"`
	UnlockConditions  types.UnlockConditions `json:"unlockconditions"`
	NewRevisionNumber uint64                 `json:"newrevisionnumber"`

	NewFileSize           uint64            `json:"newfilesize"`
	NewFileMerkleRoot     crypto.Hash       `json:"newfilemerkleroot"`
	NewWindowStart        types.BlockHeight `json:"newwindowstart"`
	NewWindowEnd          types.BlockHeight `json:"newwindowend"`
	NewValidProofOutputs  []*SiacoinOutput  `json:"newvalidproofoutputs"`
	NewMissedProofOutputs []*SiacoinOutput  `json:"newmissedproofoutputs"`
	NewUnlockHash         types.UnlockHash  `json:"newunlockhash"`
}

type Proof struct {
	*types.StorageProof
	Source *Source `json:"source"`
}

type ContractHistory struct {
	Contract  Contract   `json:"contract"`
	Revisions []Revision `json:"revisions"`
	Proof     *Proof     `json:"proof"`
}

type FileContract struct {
	ID      types.FileContractID `json:"id"`
	History *ContractHistory     `json:"history"`
}

type FileContractRevision struct {
	Index   int              `json:"index"`
	History *ContractHistory `json:"history"`
}

type StorageProof struct {
	History *ContractHistory `json:"history"`
}

type Transaction struct {
	ID                    types.TransactionID          `json:"id"`
	Block                 types.BlockID                `json:"block"`
	Blocki                int                          `json:"blocki"`
	Size                  int                          `json:"size"`
	SiacoinInputs         []*SiacoinInput              `json:"siacoininputs"`
	SiacoinOutputs        []*SiacoinOutput             `json:"siacoinoutputs"`
	FileContracts         []*FileContract              `json:"filecontracts"`
	FileContractRevisions []*FileContractRevision      `json:"filecontractrevisions"`
	StorageProofs         []*StorageProof              `json:"storageproofs"`
	SiafundInputs         []*SiafundInput              `json:"siafundinputs"`
	SiafundOutputs        []*SiafundOutput             `json:"siafundoutputs"`
	MinerFees             []types.Currency             `json:"minerfees"`
	ArbitraryData         [][]byte                     `json:"arbitrarydata"`
	TransactionSignatures []types.TransactionSignature `json:"transactionsignatures"`
}

type Block struct {
	BlockHeader
	Height       int              `json:"height"`
	ParentID     types.BlockID    `json:"parentid"`
	MinerPayouts []*SiacoinOutput `json:"minerpayouts"`
	Transactions []*Transaction   `json:"transactions"`
}

type SiacoinRecord struct {
	Income       *SiacoinOutput `json:"income"`
	IncomeSource *Source        `json:"income_source"`
	IncomeTx     *Transaction   `json:"income_tx"`
	SpentTx      *Transaction   `json:"spent_tx"`
}

type SiafundRecord struct {
	Income       *SiafundOutput `json:"income"`
	IncomeSource *Source        `json:"income_source"`
	IncomeTx     *Transaction   `json:"income_tx"`
	SpentTx      *Transaction   `json:"spent_tx"`
}

type AddressHistory struct {
	UnlockHash        types.UnlockHash `json:"unlockhash"`
	SiacoinHistory    []*SiacoinRecord `json:"siacoin_history"`
	SiafundHistory    []*SiafundRecord `json:"siafund_history"`
	Next              string           `json:"next"`
	SiacoinHistoryLen int              `json:"siacoin_history_len"`
	SiafundHistoryLen int              `json:"siafund_history_len"`
}
