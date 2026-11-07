package websocket

import "encoding/binary"

// maskCopy masks src into dst and returns the next key position. dst must
// hold len(src) bytes; dst and src may be the same slice but must not
// otherwise overlap.
func maskCopy(dst, src []byte, key [4]byte, pos int) int {
	pos &= 3
	n := len(src)
	if n == 0 {
		return pos
	}
	if pos != 0 {
		key = [4]byte{key[pos], key[(pos+1)&3], key[(pos+2)&3], key[(pos+3)&3]}
	}
	dst = dst[:n]
	if n >= 8 {
		k := uint64(key[0]) | uint64(key[1])<<8 | uint64(key[2])<<16 | uint64(key[3])<<24
		k |= k << 32
		// Four words per iteration. Testing both lengths in the loop
		// condition lets the compiler drop the bounds checks.
		for len(src) >= 32 && len(dst) >= 32 {
			binary.LittleEndian.PutUint64(dst[0:], binary.LittleEndian.Uint64(src[0:])^k)
			binary.LittleEndian.PutUint64(dst[8:], binary.LittleEndian.Uint64(src[8:])^k)
			binary.LittleEndian.PutUint64(dst[16:], binary.LittleEndian.Uint64(src[16:])^k)
			binary.LittleEndian.PutUint64(dst[24:], binary.LittleEndian.Uint64(src[24:])^k)
			src = src[32:]
			dst = dst[32:]
		}
		// Fewer than 32 bytes remain.
		if len(src) >= 16 && len(dst) >= 16 {
			binary.LittleEndian.PutUint64(dst[0:], binary.LittleEndian.Uint64(src[0:])^k)
			binary.LittleEndian.PutUint64(dst[8:], binary.LittleEndian.Uint64(src[8:])^k)
			src = src[16:]
			dst = dst[16:]
		}
		if len(src) >= 8 && len(dst) >= 8 {
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
