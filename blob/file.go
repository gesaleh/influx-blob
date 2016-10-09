package blob

import (
	"crypto/sha256"
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

	if bm.offset > fm.Size {
		bm.expSize = fm.Size % fm.BlockSize
	}

	return bm
}

// NumBlocks returns the number of blocks in the file.
func (fm *FileMeta) NumBlocks() int {
	return fm.Size / fm.BlockSize
}

// BlockMeta is the meta-information about a block.
type BlockMeta struct {
	*FileMeta

	Index  int
	SHA256 [sha256.Size]byte

	offset  int
	expSize int
}

// SetSHA256 copies r to the internal hashing mechanism.
// You can set bm.SHA256 manually, but SetSHA256 allows you to make a single iteration through r.
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
