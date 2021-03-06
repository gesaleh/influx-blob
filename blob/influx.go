package blob

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/mark-rushakoff/influx-blob/internal/influxclient"
)

type InfluxVolume struct {
	client *influxclient.Client

	database        string
	retentionPolicy string
}

func NewInfluxVolume(httpURL, database, retentionPolicy string) *InfluxVolume {
	return &InfluxVolume{
		client:          influxclient.NewClient(httpURL),
		database:        database,
		retentionPolicy: retentionPolicy,
	}
}

// UploadBlock writes the block to InfluxDB.
// This method is safe to call concurrently.
//
// The block is stored with this schema:
//
// Measurement:
//   The path of the file.
//
// Tags, unique per block:
//   bi (Block Index): The index of the block in this file, base 10. 0 <= bi <= sz/bs.
//   bsha256: The SHA256 checksum of the block, plain ASCII hex representation.
//
// Tags, identical per block within a file, unique per file:
//   bs (Block Size): The size of the raw data of each block in the file, base 10.
//                    (The last block in the file may be smaller than the block size.)
//   sha256: The sha256 of the entire raw file, plain ASCII hex representation.
//   sz: The size of the entire file, base 10. May not be a multiple of bs.
//
// Fields:
//   b: Reserved. Currently always set to integer zero.
//      Used to avoid downloading a whole block when selecting a field is necessary.
//   z: Z85-encoded binary data representing the raw content of the block.
//      For all but the last block, len(z) == bs * 5 / 4.
//      For the last block, len(z) == sz % bs, rounding up to nearest 4 for padding.
func (v *InfluxVolume) UploadBlock(data []byte, bm *BlockMeta) error {
	fm := bm.FileMeta

	prefix := fmt.Sprintf("%s,bi=%d,bs=%d,bsha256=%x,sha256=%x,sz=%d b=0i,z=\"",
		fm.Path, bm.Index, fm.BlockSize, bm.SHA256[:], fm.SHA256[:], fm.Size,
	)
	suffix := fmt.Sprintf("\" %d\n", fm.Time)

	buf := make([]byte, 0, len(prefix)+Z85EncodedLen(len(data))+len(suffix))
	buf = append(buf, prefix...)
	buf = Z85EncodeAppend(buf, data)
	buf = append(buf, suffix...)

	return v.client.SendWrite(buf, influxclient.SendOpts{
		Database:        v.database,
		RetentionPolicy: v.retentionPolicy,
		Consistency:     "all", // seeing too many errors on consistency one.
	})
}

func (v *InfluxVolume) DownloadBlock(bm *BlockMeta) ([]byte, error) {
	encoded, err := v.client.GetSingleBlock(v.database, v.retentionPolicy, bm.Path, bm.Index)
	if err != nil {
		return nil, err
	}

	// It's safe to Z85DecodeAppend into the source slice.
	raw := Z85DecodeAppend(encoded[:0], encoded)
	raw = raw[:bm.expSize] // If decoding a short frame, don't read into padding.

	if err := bm.CompareSHA256Against(bytes.NewReader(raw)); err != nil {
		return nil, err
	}

	return raw, nil
}

// ListBlocks returns a slice of block meta information belonging to path exactly.
// There may be multiple timestamps that match.
//
// The path must be an exact match.
func (v *InfluxVolume) ListBlocks(path string) ([]*BlockMeta, error) {
	ss, err := v.client.ShowSeriesForPath(path, influxclient.QueryOpts{
		Database:        v.database,
		RetentionPolicy: v.retentionPolicy,
	})
	if err != nil {
		return nil, err
	}

	mb := newMetaBuilder(len(ss))
	for _, s := range ss {
		// Assuming no malformed data arrived, tag values will never be escaped
		// and the series key can be parsed by splitting on commas and equals.
		if err := mb.Add(s); err != nil {
			return nil, err
		}
	}

	return mb.blocks, nil
}

type ListMatch int

const (
	ByPrefix ListMatch = iota
)

type ListOptions struct {
	Database  string
	ListMatch ListMatch
}

// ListFiles returns a list of filenames matching pattern, according to opts.ListMatch
func (v *InfluxVolume) ListFiles(pattern string, opts ListOptions) ([]string, error) {
	// For now, assuming ByPrefix is the only choice.
	return v.client.ShowMeasurementsByPrefix(pattern, v.database)
}

// Internal struct to quickly look up a timestamp-less FileMeta from a series key.
// You don't need to bother splitting the tags on = if you don't want to.
type fileKey struct {
	Path      string
	SHA256    string
	Size      string
	BlockSize string
}

type metaBuilder struct {
	blocks []*BlockMeta
	files  map[fileKey]*FileMeta
}

// newMetaBuilder returns a new metaBuilder with capacity for initialSize files.
func newMetaBuilder(initialSize int) *metaBuilder {
	return &metaBuilder{
		blocks: make([]*BlockMeta, 0, 4*initialSize),
		files:  make(map[fileKey]*FileMeta, initialSize),
	}
}

// Add parses a series key string into a new BlockMeta.
// Prevents duplicate FileMeta.
func (m *metaBuilder) Add(sk string) error {
	parts := strings.Split(sk, ",")
	if len(parts) != 6 {
		panic(fmt.Sprintf("exp 6 part, got: %#v", parts))
	}

	var (
		// Raw path
		path = parts[0]

		// Everything else is k=v, and we'll need to split them up

		blockIndex  = parts[1]
		blockSize   = parts[2]
		blockSHA256 = parts[3]
		fileSHA256  = parts[4]
		size        = parts[5]
	)

	// Ensure we have the one copy of this fileMeta.
	fk := fileKey{Path: path, SHA256: fileSHA256, Size: size, BlockSize: blockSize}
	var fm *FileMeta
	if fm = m.files[fk]; fm == nil {
		var err error
		fm, err = fileMetaFromFileKey(fk)
		if err != nil {
			return err
		}
		m.files[fk] = fm
	}

	// Always make a new BlockMeta.
	i, err := getV("bi", blockIndex)
	if err != nil {
		return err
	}
	idx, err := strconv.Atoi(i)
	if err != nil {
		return err
	}
	bm := fm.NewBlockMeta(idx)

	// Copy in the hash.
	hexSha, err := getV("bsha256", blockSHA256)
	if err != nil {
		return err
	}
	if err := bm.SetSHA256String(hexSha); err != nil {
		return err
	}

	m.blocks = append(m.blocks, bm)
	return nil
}

// getV returns the value in a key-value pair separated by =.
func getV(k, kv string) (string, error) {
	parts := strings.Split(kv, "=")
	if len(parts) != 2 {
		return "", fmt.Errorf("exp 2 parts, got %#v", parts)
	}
	if parts[0] != k {
		return "", fmt.Errorf("exp key %s, got %s", k, parts[0])
	}
	return parts[1], nil
}

// fileMetaFromFileKey returns a new FileMeta based on the given fileKey.
func fileMetaFromFileKey(fk fileKey) (*FileMeta, error) {
	blockSize, err := getV("bs", fk.BlockSize)
	if err != nil {
		return nil, err
	}
	bs, err := strconv.Atoi(blockSize)
	if err != nil {
		return nil, err
	}

	size, err := getV("sz", fk.Size)
	if err != nil {
		return nil, err
	}
	sz, err := strconv.Atoi(size)
	if err != nil {
		return nil, err
	}

	fm := &FileMeta{
		Path:      fk.Path,
		BlockSize: bs,
		Size:      sz,
	}
	hexSha, err := getV("sha256", fk.SHA256)
	if err != nil {
		return nil, err
	}
	if err := fm.SetSHA256String(hexSha); err != nil {
		return nil, err
	}

	return fm, nil
}
