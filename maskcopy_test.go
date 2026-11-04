package websocket

import (
	"bytes"
	"fmt"
	"testing"
)

// BenchmarkMaskCopy measures maskCopy. To compare the SIMD and portable
// implementations, run it with and without GOEXPERIMENT=simd and compare the
// results with benchstat.
func BenchmarkMaskCopy(b *testing.B) {
	key := [4]byte{0x37, 0xfa, 0x21, 0x3d}
	for _, n := range []int{16, 32, 64, 128, 256, 1024, 4096, 16384, 65536} {
		src := bytes.Repeat([]byte("0123456789abcdef"), n/16)
		for _, inPlace := range []bool{false, true} {
			name := fmt.Sprintf("%d/Copy", n)
			if inPlace {
				name = fmt.Sprintf("%d/InPlace", n)
			}
			b.Run(name, func(b *testing.B) {
				dst := make([]byte, n)
				in := src
				if inPlace {
					copy(dst, src)
					in = dst
				}
				b.SetBytes(int64(n))
				for b.Loop() {
					maskCopy(dst, in, key, 2)
				}
			})
		}
	}
}
