package human

import "github.com/NebulousLabs/Sia/types"

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
	*types.FileContract
	ID     types.FileContractID `json:"id"`
	Source *Source              `json:"source"`
}

type Revision struct {
	*types.FileContractRevision
	Source *Source `json:"source"`
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
	*types.FileContract
	ID      types.FileContractID `json:"id"`
	History *ContractHistory     `json:"history"`
}

type FileContractRevision struct {
	*types.FileContractRevision
	History *ContractHistory `json:"history"`
}

type StorageProof struct {
	*types.StorageProof
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
	Height       int                   `json:"height"`
	ID           types.BlockID         `json:"id"`
	ParentID     types.BlockID         `json:"parentid"`
	Nonce        types.BlockNonce      `json:"nonce"`
	Timestamp    types.Timestamp       `json:"timestamp"`
	MinerPayouts []types.SiacoinOutput `json:"minerpayouts"`
	Transactions []*Transaction        `json:"transactions"`
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
	UnlockHash     types.UnlockHash `json:"unlockhash"`
	SiacoinHistory []*SiacoinRecord `json:"siacoin_history"`
	SiafundHistory []*SiafundRecord `json:"siafund_history"`
	Next           string
}
