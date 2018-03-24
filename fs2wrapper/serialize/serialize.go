package serialize

import (
	"encoding/binary"

	"github.com/NebulousLabs/Sia/crypto"
)

func SerializeHash(hash crypto.Hash) []byte {
	return hash[:]
}

func DeserializeHash(value []byte) crypto.Hash {
	if len(value) != crypto.HashSize {
		panic("value has wrong size")
	}
	var hash crypto.Hash
	copy(hash[:], value)
	return hash
}

func SerializeSiacoinOutputLocation(so SiacoinOutputLocation) []byte {
	buf0 := make([]byte, binary.MaxVarintLen64*5)
	buf := buf0
	for _, i := range []int{so.Block, so.Tx, so.Nature, so.Index, so.Index0} {
		n := binary.PutUvarint(buf, uint64(i))
		buf = buf[n:]
	}
	return buf0[:len(buf)]
}

func DeserializeSiacoinOutputLocation(value []byte) (so SiacoinOutputLocation) {
	for _, p := range []*int{&so.Block, &so.Tx, &so.Nature, &so.Index, &so.Index0} {
		i, n := binary.Uvarint(value)
		if n <= 0 {
			panic("Value is too short")
		}
		*p = int(i)
		value = value[n:]
	}
	if len(value) != 0 {
		panic("Value is too long")
	}
	return
}
