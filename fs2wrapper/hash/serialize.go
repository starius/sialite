package hash

import (
	"github.com/NebulousLabs/Sia/crypto"
)

func Serialize(hash crypto.Hash) []byte {
	return hash[:]
}

func Deserialize(value []byte) crypto.Hash {
	if len(value) != crypto.HashSize {
		panic("value has wrong size")
	}
	var hash crypto.Hash
	copy(hash[:], value)
	return hash
}
