package transaction

import (
	"encoding/binary"
)

func Serialize(loc Location) []byte {
	buf0 := make([]byte, binary.MaxVarintLen64*2)
	buf := buf0
	for _, i := range []int{loc.Block, loc.Tx} {
		n := binary.PutUvarint(buf, uint64(i))
		buf = buf[n:]
	}
	return buf0[:len(buf0)-len(buf)]
}

func Deserialize(value []byte) (loc Location) {
	for _, p := range []*int{&loc.Block, &loc.Tx} {
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
