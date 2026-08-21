//go:build goexperiment.simd

package websocket

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

// maskCopyWordBenchmark is the pre-SIMD implementation retained as the
// benchmark baseline.
func maskCopyWordBenchmark(dst, src []byte, key [4]byte, pos int) int {
	pos &= 3
	n := len(src)
	if n == 0 {
		return pos
	}
	if pos != 0 {
		key = [4]byte{key[pos], key[(pos+1)&3], key[(pos+2)&3], key[(pos+3)&3]}
	}
	if n >= 8 {
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

func TestMaskCopySIMD(t *testing.T) {
	key := [4]byte{0x37, 0xfa, 0x21, 0x3d}
	for _, n := range []int{0, 1, 7, 8, 15, 16, 17, 31, 32, 63, 64, 65, 4096} {
		src := bytes.Repeat([]byte("0123456789abcdef"), n/16+1)[:n]
		for pos := range 4 {
			want := bytes.Clone(src)
			wantPos := refMask(want, key, pos)

			dst := make([]byte, n)
			if gotPos := maskCopy(dst, src, key, pos); gotPos != wantPos {
				t.Errorf("n=%d pos=%d: position = %d, want %d", n, pos, gotPos, wantPos)
			}
			if !bytes.Equal(dst, want) {
				t.Errorf("n=%d pos=%d: out-of-place result mismatch", n, pos)
			}

			inPlace := bytes.Clone(src)
			maskCopy(inPlace, inPlace, key, pos)
			if !bytes.Equal(inPlace, want) {
				t.Errorf("n=%d pos=%d: in-place result mismatch", n, pos)
			}
		}
	}
}

// Run with:
//
//	GOEXPERIMENT=simd go test -run '^$' -bench '^BenchmarkMaskCopySIMD$' -benchmem
func BenchmarkMaskCopySIMD(b *testing.B) {
	key := [4]byte{0x37, 0xfa, 0x21, 0x3d}
	implementations := []struct {
		name string
		fn   func([]byte, []byte, [4]byte, int) int
	}{
		{"Current", maskCopyWordBenchmark},
		{"SIMD", maskCopy},
	}
	for _, impl := range implementations {
		b.Run(impl.name, func(b *testing.B) {
			for _, n := range []int{16, 32, 64, 128, 256, 1024, 4096, 16384, 65536} {
				b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
					src := bytes.Repeat([]byte("0123456789abcdef"), n/16)
					for _, inPlace := range []bool{false, true} {
						name := "Copy"
						if inPlace {
							name = "InPlace"
						}
						b.Run(name, func(b *testing.B) {
							dst := make([]byte, n)
							if inPlace {
								copy(dst, src)
								src = dst
							}
							b.SetBytes(int64(n))
							b.ReportAllocs()
							for b.Loop() {
								impl.fn(dst, src, key, 2)
							}
						})
					}
				})
			}
		})
	}
}
