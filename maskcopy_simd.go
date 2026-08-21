//go:build goexperiment.simd

package websocket

import (
	"encoding/binary"
	"simd"
)

// maskCopy masks src into dst and returns the next key position. dst must
// hold len(src) bytes; dst and src may be the same slice but must not
// otherwise overlap.
func maskCopy(dst, src []byte, key [4]byte, pos int) int {
	if len(src) >= 64 {
		return maskCopySIMD(dst, src, key, pos)
	}
	// Keep this small-buffer path in sync with maskcopy_nosimd.go. Duplicating
	// it avoids an extra function call for small frames in SIMD builds.
	pos &= 3
	n := len(src)
	if n == 0 {
		return pos
	}
	if pos != 0 {
		key = [4]byte{key[pos], key[(pos+1)&3], key[(pos+2)&3], key[(pos+3)&3]}
	}

	if len(src) >= 8 {
		k := uint64(key[0]) | uint64(key[1])<<8 | uint64(key[2])<<16 | uint64(key[3])<<24
		k |= k << 32
		for len(src) >= 8 {
			binary.LittleEndian.PutUint64(dst, binary.LittleEndian.Uint64(src)^k)
			src = src[8:]
			dst = dst[8:]
		}
	}
	for i := range src {
		dst[i] = src[i] ^ key[i&3]
	}
	return (pos + n) & 3
}

func maskCopySIMD(dst, src []byte, key [4]byte, pos int) int {
	pos &= 3
	n := len(src)
	if n == 0 {
		return pos
	}
	if pos != 0 {
		key = [4]byte{key[pos], key[(pos+1)&3], key[(pos+2)&3], key[(pos+3)&3]}
	}

	// Reshape is defined in little-endian lane order, producing the repeating
	// key bytes without constructing a vector-sized temporary slice.
	k := uint32(key[0]) | uint32(key[1])<<8 | uint32(key[2])<<16 | uint32(key[3])<<24
	mask := simd.BroadcastUint32s(k).ReshapeToUint8s()
	width := mask.Len()
	for len(src) >= width {
		simd.LoadUint8s(src).Xor(mask).Store(dst)
		src = src[width:]
		dst = dst[width:]
	}
	if len(src) >= 8 {
		k := uint64(key[0]) | uint64(key[1])<<8 | uint64(key[2])<<16 | uint64(key[3])<<24
		k |= k << 32
		for len(src) >= 8 {
			binary.LittleEndian.PutUint64(dst, binary.LittleEndian.Uint64(src)^k)
			src = src[8:]
			dst = dst[8:]
		}
	}
	for i := range src {
		dst[i] = src[i] ^ key[i&3]
	}
	return (pos + n) & 3
}
