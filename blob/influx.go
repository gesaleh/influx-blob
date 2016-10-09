package blob

import (
	"fmt"

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

// PutBlock writes the block to InfluxDB.
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
//   z: Z85-encoded binary data representing the raw content of the block.
//      For all but the last block, len(z) == bs * 5 / 4.
//      For the last block, len(z) == sz % bs, rounding up to nearest 4 for padding.
func (v *InfluxVolume) PutBlock(data []byte, bm *BlockMeta) error {
	fm := bm.FileMeta

	prefix := fmt.Sprintf("%s,bi=%d,bs=%d,bsha256=%x,sha256=%x,sz=%d z=\"",
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
	})
}

func (v *InfluxVolume) List(prefix, measurement string) ([]*FileMeta, error) {
	// SHOW SERIES FROM <m> WHERE path =~ /^.../ AND bi='1'
	//
	return nil, nil
}

func (v *InfluxVolume) ListBlocks(path, measurement string, fm FileMeta) ([]*BlockMeta, error) {
	// SHOW SERIES FROM <m> WHERE path = path
	return nil, nil
}
