package siacoinoutput

const (
	// For SiacoinOutput.nature.
	SiacoinOutput               = iota
	MinerPayout                 = iota
	ValidProofOutput            = iota
	MissedProofOutput           = iota
	ValidProofOutputInRevision  = iota
	MissedProofOutputInRevision = iota
	SiaClaimOutput              = iota
)

func NatureStr(nature int) string {
	if nature == SiacoinOutput {
		return "siacoin_output"
	} else if nature == MinerPayout {
		return "miner_payout"
	} else if nature == ValidProofOutput {
		return "valid_proof_output"
	} else if nature == MissedProofOutput {
		return "missed_proof_output"
	} else if nature == ValidProofOutputInRevision {
		return "valid_proof_output_in_revision"
	} else if nature == MissedProofOutputInRevision {
		return "missed_proof_output_in_revision"
	} else if nature == SiaClaimOutput {
		return "sia_claim_output"
	} else {
		panic("unknown nature")
	}
}

type Location struct {
	Block  int
	Tx     int // Empty for MinerPayouts.
	Nature int
	Index  int // Index of SiacoinOutput in slice.
	Index0 int // Index of FileContract or FileContractRevision.
}
