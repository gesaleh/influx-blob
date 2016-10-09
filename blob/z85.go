package blob

import "encoding/binary"

var btoa = [85]byte{
	/*00 - 09:*/ '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
	/*10 - 19:*/ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
	/*20 - 29:*/ 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
	/*30 - 39:*/ 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D',
	/*40 - 49:*/ 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N',
	/*50 - 59:*/ 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
	/*60 - 69:*/ 'Y', 'Z', '.', '-', ':', '+', '=', '^', '!', '/',
	/*70 - 79:*/ '*', '?', '&', '<', '>', '(', ')', '[', ']', '{',
	/*80 - 84:*/ '}', '@', '%', '$', '#',
}

var atob = [256]byte{}

func init() {
	for i := 0; i < len(atob); i++ {
		atob[i] = 0xFF
	}

	for val, a := range btoa {
		atob[a] = uint8(val)
	}
}

// Z85EncodedLen returns the length of the result of encoding n bytes into Z85.
func Z85EncodedLen(n int) int {
	return n * 5 / 4
}

// Z85Append encodes one frame at a time from src, into dst.
// Each 4-byte binary frame in src is encoded into a 5-byte ASCII frame in dst.
// Because dst grows more quickly than src, dst must not overlap src.
func Z85EncodeAppend(dst []byte, src []byte) []byte {
	nFrames := len(src) / 4

	var buf [5]byte
	for i := 0; i < nFrames; i++ {
		// Take slice of 4 bytes as big endian uint32...
		v := binary.BigEndian.Uint32(src[i*4 : (i+1)*4])
		// v := binary.BigEndian.Uint32(src[i*4 : (i+1)*4])
		// Convert to Z85, most significant byte first
		for z := 0; z < 5; z++ {
			buf[4-z] = btoa[v%85]
			v /= 85
		}
		dst = append(dst, buf[:]...)
	}

	return dst
}

// Z85DecodeAppend decodes one frame at a time from src, into dst.
// Each 5-byte ASCII frame in src is encoded into a 4-byte binary frame in dst.
// Because src is depleted more quickly than dst grows, it is acceptable for dst to overlap src.
func Z85DecodeAppend(dst []byte, src []byte) []byte {
	nFrames := len(src) / 5

	for i := 0; i < nFrames; i++ {
		aFrame := src[i*5 : (i+1)*5] // Ascii Frame

		// Convert Ascii Frame to uint32.
		var v uint32 = uint32(atob[aFrame[0]])
		for j := 1; j < 5; j++ {
			v *= 85
			v += uint32(atob[aFrame[j]])
		}

		// Encode uint32 into buf.
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], v)

		// Append buf to dst.
		dst = append(dst, buf[:]...)
	}

	return dst
}
