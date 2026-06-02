package codec

import "encoding/binary"

const (
	xxhPrime1      uint32 = 2654435761
	xxhPrime2      uint32 = 2246822519
	xxhPrime3      uint32 = 3266489917
	xxhPrime4      uint32 = 668265263
	xxhPrime5      uint32 = 374761393
	xxhPrime1Plus2 uint32 = 606290984
	xxhPrime1Minus uint32 = 1640531535
)

func xxh32Zero(input []byte) uint32 {
	n := len(input)
	h := uint32(n)
	p := 0
	if n >= 16 {
		v1 := xxhPrime1Plus2
		v2 := xxhPrime2
		v3 := uint32(0)
		v4 := xxhPrime1Minus
		for limit := n - 16; p <= limit; p += 16 {
			block := input[p:][:16]
			v1 = bitsRotateLeft32(v1+binary.LittleEndian.Uint32(block[0:])*xxhPrime2, 13) * xxhPrime1
			v2 = bitsRotateLeft32(v2+binary.LittleEndian.Uint32(block[4:])*xxhPrime2, 13) * xxhPrime1
			v3 = bitsRotateLeft32(v3+binary.LittleEndian.Uint32(block[8:])*xxhPrime2, 13) * xxhPrime1
			v4 = bitsRotateLeft32(v4+binary.LittleEndian.Uint32(block[12:])*xxhPrime2, 13) * xxhPrime1
		}
		h += bitsRotateLeft32(v1, 1) + bitsRotateLeft32(v2, 7) + bitsRotateLeft32(v3, 12) + bitsRotateLeft32(v4, 18)
	} else {
		h += xxhPrime5
	}
	for limit := n - 4; p <= limit; p += 4 {
		h += binary.LittleEndian.Uint32(input[p:p+4]) * xxhPrime3
		h = bitsRotateLeft32(h, 17) * xxhPrime4
	}
	for p < n {
		h += uint32(input[p]) * xxhPrime5
		h = bitsRotateLeft32(h, 11) * xxhPrime1
		p++
	}
	h ^= h >> 15
	h *= xxhPrime2
	h ^= h >> 13
	h *= xxhPrime3
	h ^= h >> 16
	return h
}

func bitsRotateLeft32(x uint32, k uint) uint32 {
	return x<<k | x>>(32-k)
}
