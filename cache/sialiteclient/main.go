package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/NebulousLabs/Sia/crypto"
	"github.com/NebulousLabs/Sia/encoding"
	"github.com/NebulousLabs/Sia/modules"
	"github.com/NebulousLabs/Sia/types"
	"github.com/starius/sialite/cache"
)

var (
	server   = flag.String("server", "127.0.0.1:35813", "Target address")
	seedFile = flag.String("seed-file", "", "File with seed")
	maxGap   = flag.Int("max-gap", 100, "Maximum consecutive number of unused addresses")
)

type fullItem struct {
	source *cache.Item
	payout *types.SiacoinOutput
	tx     *types.Transaction
}

func getHistory(kind, id string, headersBytes []byte) ([]fullItem, error) {
	next := ""
	var rawItems []cache.Item
	for {
		url := fmt.Sprintf("http://%s/v1/%s-history?%s=%s&start=%s", *server, kind, kind, id, next)
		respHistory, err := http.Get(url)
		if err != nil {
			return nil, fmt.Errorf("http.Get(%q): %v", url, err)
		}
		if respHistory.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("http.Get(%q): %s", url, respHistory.Status)
		}
		var history []cache.Item
		if err := encoding.NewDecoder(respHistory.Body).DecodeAll(&next, &history); err != nil {
			return nil, fmt.Errorf("DecodeAll: %v", err)
		}
		respHistory.Body.Close()
		rawItems = append(rawItems, history...)
		if next == "" {
			break
		}
	}
	var fullItems []fullItem
	for i := 0; i < len(rawItems); i++ {
		item := &rawItems[i]
		data, err := item.SourceData(nil)
		if err != nil {
			return nil, fmt.Errorf("item.SourceData: %v", err)
		}
		if item.Block < 0 || item.Block >= len(headersBytes)/48 {
			return nil, fmt.Errorf("bad block index: %d", item.Block)
		}
		header := headersBytes[item.Block*48 : (item.Block+1)*48]
		merkleRoot := header[16:]
		if !cache.VerifyProof(merkleRoot, data, item.MerkleProof, item.Index, item.NumLeaves) {
			return nil, fmt.Errorf("cache.VerifyProof: bad proof")
		}
		full := fullItem{source: item}
		if item.Index < item.NumMinerPayouts {
			var payout types.SiacoinOutput
			if err := encoding.Unmarshal(data, &payout); err != nil {
				return nil, fmt.Errorf("encoding.Unmarshal payout: %v", err)
			}
			full.payout = &payout
		} else {
			var tx types.Transaction
			if err := encoding.Unmarshal(data, &tx); err != nil {
				return nil, fmt.Errorf("encoding.Unmarshal tx: %v", err)
			}
			full.tx = &tx
		}
		fullItems = append(fullItems, full)
	}
	return fullItems, nil
}

func addressHistory(address string, headersBytes []byte) ([]fullItem, error) {
	return getHistory("address", address, headersBytes)
}

// generateAddress generates a key and an address from seed.
// See function generateSpendableKey from Sia. https://git.io/fNfs6
func generateAddress(seed modules.Seed, index uint64) (types.UnlockConditions, crypto.SecretKey) {
	sk, pk := crypto.GenerateKeyPairDeterministic(crypto.HashAll(seed, index))
	uc := types.UnlockConditions{
		PublicKeys:         []types.SiaPublicKey{types.Ed25519PublicKey(pk)},
		SignaturesRequired: 1,
	}
	return uc, sk
}

func getHeadersSlice(headersBytes []byte) ([]types.BlockHeader, error) {
	headersN := len(headersBytes) / 48
	headersSlice := make([]types.BlockHeader, headersN)
	headersSlice[0] = types.BlockHeader{
		ParentID:   types.GenesisBlock.ParentID,
		Nonce:      types.GenesisBlock.Nonce,
		Timestamp:  types.GenesisBlock.Timestamp,
		MerkleRoot: types.GenesisBlock.MerkleRoot(),
	}
	for i := 1; i < headersN; i++ {
		header := headersBytes[i*48 : (i*48 + 48)]
		headersSlice[i] = types.BlockHeader{
			ParentID:  headersSlice[i-1].ID(),
			Timestamp: types.Timestamp(encoding.DecUint64(header[8:16])),
		}
		copy(headersSlice[i].Nonce[:], header[:8])
		copy(headersSlice[i].MerkleRoot[:], header[16:48])
	}
	if headersN > 1 && headersSlice[1].ParentID != types.GenesisID {
		return nil, fmt.Errorf("ParentID of 2nd header is not GenesisID")
	}
	return headersSlice, nil
}

// payoutID returns SiacoinOutputID for miner payout.
// See Block.MinerPayoutID from Sia.
func payoutID(blockID types.BlockID, index uint64) types.SiacoinOutputID {
	return types.SiacoinOutputID(crypto.HashAll(blockID, index))
}

type income struct {
	id    types.SiacoinOutputID
	value types.Currency
}

type sfincome struct {
	id    types.SiafundOutputID
	value types.Currency
}

type contractOutput struct {
	fcid   types.FileContractID
	rev    uint64
	income income
	valid  bool
}

func findMoney(address types.UnlockHash, item fullItem, blockID types.BlockID) (incomes []income, outcomes []types.SiacoinOutputID, sfincomes []sfincome, sfoutcomes []types.SiafundOutputID, contracts []contractOutput) {
	if item.payout != nil {
		if address == item.payout.UnlockHash {
			id := payoutID(blockID, uint64(item.source.Index))
			incomes = append(incomes, income{id: id, value: item.payout.Value})
		}
	} else if item.tx != nil {
		for _, si := range item.tx.SiacoinInputs {
			if si.UnlockConditions.UnlockHash() == address {
				outcomes = append(outcomes, si.ParentID)
			}
		}
		for _, si := range item.tx.SiafundInputs {
			if si.UnlockConditions.UnlockHash() == address {
				sfoutcomes = append(sfoutcomes, si.ParentID)
			}
		}
		for i, so := range item.tx.SiacoinOutputs {
			if so.UnlockHash == address {
				id := item.tx.SiacoinOutputID(uint64(i))
				incomes = append(incomes, income{id: id, value: so.Value})
			}
		}
		for i, so := range item.tx.SiafundOutputs {
			if so.UnlockHash == address {
				id := item.tx.SiafundOutputID(uint64(i))
				sfincomes = append(sfincomes, sfincome{id: id, value: so.Value})
			}
		}
		for i0, contract := range item.tx.FileContracts {
			fcid := item.tx.FileContractID(uint64(i0))
			for i, o := range contract.ValidProofOutputs {
				if o.UnlockHash == address {
					id := fcid.StorageProofOutputID(types.ProofValid, uint64(i))
					contracts = append(contracts, contractOutput{
						fcid:   fcid,
						rev:    contract.RevisionNumber,
						income: income{id: id, value: o.Value},
						valid:  true,
					})
				}
			}
			for i, o := range contract.MissedProofOutputs {
				if o.UnlockHash == address {
					id := fcid.StorageProofOutputID(types.ProofMissed, uint64(i))
					contracts = append(contracts, contractOutput{
						fcid:   fcid,
						rev:    contract.RevisionNumber,
						income: income{id: id, value: o.Value},
						valid:  false,
					})
				}
			}
		}
		for _, contractRev := range item.tx.FileContractRevisions {
			fcid := contractRev.ParentID
			for i, o := range contractRev.NewValidProofOutputs {
				if o.UnlockHash == address {
					id := fcid.StorageProofOutputID(types.ProofValid, uint64(i))
					contracts = append(contracts, contractOutput{
						fcid:   fcid,
						rev:    contractRev.NewRevisionNumber,
						income: income{id: id, value: o.Value},
						valid:  true,
					})
				}
			}
			for i, o := range contractRev.NewMissedProofOutputs {
				if o.UnlockHash == address {
					id := fcid.StorageProofOutputID(types.ProofMissed, uint64(i))
					contracts = append(contracts, contractOutput{
						fcid:   fcid,
						rev:    contractRev.NewRevisionNumber,
						income: income{id: id, value: o.Value},
						valid:  false,
					})
				}
			}
		}
		// TODO: add other sources of income and outcome.
	} else {
		panic("full item with neither payout nor tx")
	}
	return
}

type contractResult struct {
	lastRev uint64
	valid   bool
	closed  bool
}

func getContractResult(fcid types.FileContractID, headersBytes []byte) (contractResult, error) {
	items, err := getHistory("contract", fcid.String(), headersBytes)
	if err != nil {
		return contractResult{}, err
	}
	lastRev := uint64(0)
	lastWindowEnd := types.BlockHeight(0)
	valid := false
	closed := false
	for _, full := range items {
		if full.tx == nil {
			continue
		}
		for i0, contract := range full.tx.FileContracts {
			if full.tx.FileContractID(uint64(i0)) != fcid {
				continue
			}
			if contract.RevisionNumber > lastRev {
				lastRev = contract.RevisionNumber
				lastWindowEnd = contract.WindowEnd
			}
		}
		for _, contractRev := range full.tx.FileContractRevisions {
			if contractRev.ParentID != fcid {
				continue
			}
			if contractRev.NewRevisionNumber > lastRev {
				lastRev = contractRev.NewRevisionNumber
				lastWindowEnd = contractRev.NewWindowEnd
			}
		}
		for _, proof := range full.tx.StorageProofs {
			if proof.ParentID != fcid {
				continue
			}
			valid = true
			closed = true
		}
	}
	nblocks := len(headersBytes) / 48
	if types.BlockHeight(nblocks) > lastWindowEnd {
		closed = true
	}
	return contractResult{
		lastRev: lastRev,
		valid:   valid,
		closed:  closed,
	}, nil
}

func main() {
	flag.Parse()
	respHeaders, err := http.Get("http://" + *server + "/v1/headers")
	if err != nil {
		panic(err)
	}
	headersBytes, err := ioutil.ReadAll(respHeaders.Body)
	if err != nil {
		panic(err)
	}
	respHeaders.Body.Close()
	if err := cache.VerifyBlockHeaders(headersBytes); err != nil {
		panic(err)
	}
	headers, err := getHeadersSlice(headersBytes)
	if err != nil {
		panic(err)
	}
	seedBytes, err := ioutil.ReadFile(*seedFile)
	if err != nil {
		panic(err)
	}
	seed, err := modules.StringToSeed(string(seedBytes), "english")
	if err != nil {
		panic(err)
	}
	gap := 0
	incomesMap := make(map[types.SiacoinOutputID]types.Currency)
	outcomesMap := make(map[types.SiacoinOutputID]struct{})
	sfincomesMap := make(map[types.SiafundOutputID]types.Currency)
	sfoutcomesMap := make(map[types.SiafundOutputID]struct{})
	var allContracts []contractOutput
	for index := uint64(0); gap < *maxGap; index++ {
		uc, _ := generateAddress(seed, index)
		address := uc.UnlockHash()
		history, err := addressHistory(address.String(), headersBytes)
		if err != nil {
			panic(err)
		}
		if len(history) == 0 {
			gap++
		} else {
			gap = 0
		}
		for _, full := range history {
			incomes, outcomes, sfincomes, sfoutcomes, contracts := findMoney(address, full, headers[full.source.Block].ID())
			for _, income := range incomes {
				incomesMap[income.id] = income.value
			}
			for _, outcome := range outcomes {
				outcomesMap[outcome] = struct{}{}
			}
			for _, sfincome := range sfincomes {
				sfincomesMap[sfincome.id] = sfincome.value
			}
			for _, sfoutcome := range sfoutcomes {
				sfoutcomesMap[sfoutcome] = struct{}{}
			}
			allContracts = append(allContracts, contracts...)
		}
	}
	contractsSet := make(map[types.FileContractID]struct{})
	for _, co := range allContracts {
		contractsSet[co.fcid] = struct{}{}
	}
	contractsResults := make(map[types.FileContractID]contractResult)
	for fcid := range contractsSet {
		result, err := getContractResult(fcid, headersBytes)
		if err != nil {
			panic(err)
		}
		contractsResults[fcid] = result
	}
	for _, co := range allContracts {
		result := contractsResults[co.fcid]
		if !result.closed || co.rev != result.lastRev || co.valid != result.valid {
			continue
		}
		incomesMap[co.income.id] = co.income.value
	}
	unspent := make(map[types.SiacoinOutputID]types.Currency)
	for id, value := range incomesMap {
		if _, has := outcomesMap[id]; !has {
			unspent[id] = value
		}
	}
	for id := range outcomesMap {
		if _, has := incomesMap[id]; !has {
			// TODO: return err.
			log.Printf("Can't find income for outcome %s.", id)
		}
	}
	sfunspent := make(map[types.SiafundOutputID]types.Currency)
	for id, value := range sfincomesMap {
		if _, has := sfoutcomesMap[id]; !has {
			sfunspent[id] = value
		}
	}
	for id := range sfoutcomesMap {
		if _, has := sfincomesMap[id]; !has {
			// TODO: return err.
			log.Printf("Can't find SF income for outcome %s.", id)
		}
	}
	total := types.NewCurrency64(0)
	for _, value := range unspent {
		total = total.Add(value)
	}
	log.Printf("Available money: %s.", total.HumanString())
	sftotal := types.NewCurrency64(0)
	for _, value := range sfunspent {
		sftotal = sftotal.Add(value)
	}
	log.Printf("Available SF: %s.", sftotal)
}
