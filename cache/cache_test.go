package cache

import (
	"compress/gzip"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/NebulousLabs/Sia/encoding"
	"github.com/NebulousLabs/Sia/types"
)

var (
	cached1000Blocks []*types.Block
	cachedAddresses  []string
)

func read1000Blocks() ([]*types.Block, error) {
	if cached1000Blocks != nil {
		return cached1000Blocks, nil
	}
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return nil, fmt.Errorf("Unable to find current package file")
	}
	pkgDir := filepath.Dir(filename)
	blocksFile := filepath.Join(pkgDir, "testdata", "first_1000.blocks.gz")
	f, err := os.Open(blocksFile)
	if err != nil {
		return nil, fmt.Errorf("os.Open(%q): %v", blocksFile, err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("gzip.NewReader: %v", err)
	}
	var blocks []*types.Block
	for {
		var block types.Block
		err := encoding.ReadObject(gz, &block, types.BlockSizeLimit)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("encoding.ReadObject: %v", err)
		}
		blocks = append(blocks, &block)
	}
	cached1000Blocks = blocks
	return blocks, nil
}

func readAddresses() ([]string, error) {
	if cachedAddresses != nil {
		return cachedAddresses, nil
	}
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return nil, fmt.Errorf("Unable to find current package file")
	}
	pkgDir := filepath.Dir(filename)
	addressesFile := filepath.Join(pkgDir, "testdata", "addresses.txt")
	f, err := os.Open(addressesFile)
	if err != nil {
		return nil, fmt.Errorf("os.Open(%q): %v", addressesFile, err)
	}
	defer f.Close()
	var addresses []string
	for {
		var address string
		if _, err := fmt.Fscanf(f, "%s", &address); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("fmt.Fscanf: %v", err)
		}
		addresses = append(addresses, address)
	}
	cachedAddresses = addresses
	return addresses, nil
}

func TestOnRealBlocks(t *testing.T) {
	blocks, err := read1000Blocks()
	if err != nil {
		t.Fatalf("read1000Blocks: %v", err)
	}

	cases := []struct {
		memLimit                 int
		offsetLen                int
		offsetIndexLen           int
		addressPageLen           int
		addressPrefixLen         int
		addressFastmapPrefixLen  int
		addressOffsetLen         int
		contractPageLen          int
		contractPrefixLen        int
		contractFastmapPrefixLen int
		contractOffsetLen        int
	}{
		{
			memLimit:                 1,
			offsetLen:                8,
			offsetIndexLen:           4,
			addressPageLen:           4096,
			addressPrefixLen:         16,
			addressFastmapPrefixLen:  5,
			addressOffsetLen:         4,
			contractPageLen:          4096,
			contractPrefixLen:        16,
			contractFastmapPrefixLen: 5,
			contractOffsetLen:        4,
		},
		{
			memLimit:                 1,
			offsetLen:                7,
			offsetIndexLen:           2,
			addressPageLen:           1500,
			addressPrefixLen:         32,
			addressFastmapPrefixLen:  3,
			addressOffsetLen:         5,
			contractPageLen:          2000,
			contractPrefixLen:        31,
			contractFastmapPrefixLen: 4,
			contractOffsetLen:        4,
		},
	}

	addresses, err := readAddresses()
	if err != nil {
		t.Fatalf("readAddresses: %v", err)
	}

next:
	for _, tc := range cases {
		tmpDir, err := ioutil.TempDir("", "TestOnRealBlocks")
		if err != nil {
			t.Fatalf("ioutil.TempDir: %v", err)
		}
		b, err := NewBuilder(tmpDir, tc.memLimit, tc.offsetLen, tc.offsetIndexLen, tc.addressPageLen, tc.addressPrefixLen, tc.addressFastmapPrefixLen, tc.addressOffsetLen, tc.contractPageLen, tc.contractPrefixLen, tc.contractFastmapPrefixLen, tc.contractOffsetLen)
		if err != nil {
			t.Errorf("NewBuilder: %v", err)
			continue next
		}
		for _, block := range blocks {
			if err := b.Add(block); err != nil {
				t.Errorf("b.Add: %v", err)
				continue next
			}
		}
		if err := b.Close(); err != nil {
			t.Errorf("b.Close: %v", err)
			continue next
		}
		s, err := NewServer(tmpDir)
		if err != nil {
			t.Errorf("NewServer: %v", err)
			continue next
		}
	next2:
		for _, address := range addresses {
			addressBytes, err := hex.DecodeString(address)
			if err != nil {
				t.Errorf("hex.DecodeString(%s): %v", address, err)
				continue next2
			}
			history, _, err := s.AddressHistory(addressBytes[:32], "")
			if err != nil {
				t.Errorf("s.AddressHistory(%s): %v", address, err)
				continue next2
			}
			if len(history) == 0 {
				t.Errorf("s.AddressHistory(%s): returned nothing", address)
				continue next2
			}
			// Change the first byte and make sure nothing is found.
			addressBytes[0] = ^addressBytes[0]
			history, _, err = s.AddressHistory(addressBytes[:32], "")
			if err != nil {
				t.Errorf("s.AddressHistory(%s): %v", hex.EncodeToString(addressBytes), err)
				continue next2
			}
			if len(history) != 0 {
				t.Errorf("s.AddressHistory(%s): returned something", hex.EncodeToString(addressBytes))
				continue next2
			}
		}
		// TODO check contracts.
		if err := s.Close(); err != nil {
			t.Errorf("s.Close: %v", err)
			continue next
		}
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Errorf("os.RemoveAll(%q): %v", tmpDir, err)
			continue next
		}
	}
}
