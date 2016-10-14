package blob

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
)

// FileMeta is the metadata for a File.
// Volumes report this meta-information at the file level.
type FileMeta struct {
	// Path to the file.
	Path string
	// Checksum of the entire file.
	SHA256 [sha256.Size]byte
	// Size of each block.
	BlockSize int
	// Total size of the file (does not need to be a multiple of the block size).
	Size int
	// Timestamp in seconds since Unix epoch.
	Time int64
}

// NewFileMeta returns a new FileMeta with Size and SHA256 set as calculated from r.
// It is the responsibility of the caller to set any other fields.
func NewFileMeta(r io.Reader) (*FileMeta, error) {
	fm := new(FileMeta)
	// Determine file SHA
	h := sha256.New()
	n, err := io.Copy(h, r)
	if err != nil {
		return nil, err
	}
	fm.Size = int(n)

	if n := copy(fm.SHA256[:], h.Sum(nil)); n != sha256.Size {
		return nil, fmt.Errorf("SHA256 checksum did not fit! Got 0x%X (len %d)", fm.SHA256, n)
	}

	return fm, nil
}

// NewBlockMeta returns a new BlockMeta with the Index field set.
// The SHA256 field must be set separately.
func (fm *FileMeta) NewBlockMeta(blockIndex int) *BlockMeta {
	bm := &BlockMeta{
		FileMeta: fm,
		Index:    blockIndex,

		offset:  blockIndex * fm.BlockSize,
		expSize: fm.BlockSize,
	}

	if bm.offset+bm.expSize > fm.Size {
		bm.expSize = fm.Size - bm.offset
	}

	return bm
}

// NumBlocks returns the number of blocks in the file.
func (fm *FileMeta) NumBlocks() int {
	n := fm.Size / fm.BlockSize
	if n*fm.BlockSize < fm.Size {
		n++
	}
	return n
}

// SetSHA256String sets the SHA256 from the hex-encoded string.
func (fm *FileMeta) SetSHA256String(hexSha string) error {
	return setSHA256String(&fm.SHA256, hexSha)
}

// CompareSHA256Against calculates the checksum of r and returns an error if the hashes
// do not match, or any IO error that occurs while reading r.
func (fm *FileMeta) CompareSHA256Against(r io.Reader) error {
	return compareSHA256Against(r, fm.SHA256, int64(fm.Size))
}

// BlockMeta is the meta-information about a block.
type BlockMeta struct {
	*FileMeta

	Index  int
	SHA256 [sha256.Size]byte

	offset  int
	expSize int
}

func (bm *BlockMeta) FileOffset() int64 {
	return int64(bm.offset)
}

func (bm *BlockMeta) ExpSize() int {
	return bm.expSize
}

// SetSHA256 copies r to the internal hashing mechanism.
func (bm *BlockMeta) SetSHA256(r io.Reader) error {
	h := sha256.New()

	n, err := io.Copy(h, r)
	if err != nil {
		return err
	}
	if n != int64(bm.expSize) {
		return fmt.Errorf("Expected to read %d bytes, got %d", bm.expSize, n)
	}

	if n := copy(bm.SHA256[:], h.Sum(nil)); n != sha256.Size {
		return fmt.Errorf("SHA256 checksum did not fit! Got 0x%X (len %d)", bm.SHA256, n)
	}

	return nil
}

// CompareSHA256Against calculates the checksum of r and returns an error if the hashes
// do not match, or any IO error that occurs while reading r.
func (bm *BlockMeta) CompareSHA256Against(r io.Reader) error {
	return compareSHA256Against(r, bm.SHA256, int64(bm.expSize))
}

func compareSHA256Against(r io.Reader, sha [sha256.Size]byte, expSize int64) error {
	h := sha256.New()

	if n, err := io.Copy(h, r); err != nil {
		return err
	} else if n != expSize {
		return fmt.Errorf("Expected to read %d bytes, got %d", expSize, n)
	}

	if !bytes.Equal(h.Sum(nil), sha[:]) {
		return fmt.Errorf("GetBlock: checksum did not match! exp %x, got %x", sha, h.Sum(nil))
	}

	return nil
}

// SetSHA256String sets the SHA256 from the hex-encoded string.
func (bm *BlockMeta) SetSHA256String(hexSha string) error {
	return setSHA256String(&bm.SHA256, hexSha)
}

func setSHA256String(dst *[sha256.Size]byte, hexSha string) error {
	if dl := hex.DecodedLen(len(hexSha)); dl != sha256.Size {
		return fmt.Errorf("exp sha length %d, got %s (len %d)", sha256.Size, hexSha, dl)
	}
	sha, err := hex.DecodeString(hexSha)
	if err != nil {
		return err
	}
	if n := copy(dst[:], sha); n != sha256.Size {
		return fmt.Errorf("SHA256 checksum did not fit! Got 0x%X (len %d)", dst, n)
	}
	return nil
}
