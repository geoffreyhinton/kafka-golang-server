package commitlog

import (
	"encoding/binary"

	"github.com/pkg/errors"
)

var (
	ErrSegmentNotFound = errors.New("segment not found")
	Encoding           = binary.BigEndian
)

type CleanupPolicy string
type Options struct {
	Path string
	// MaxSegmentBytes is the max number of bytes a segment can contain, once the limit is hit a
	// new segment will be split off.
	MaxSegmentBytes int64
	MaxLogBytes     int64
	CleanupPolicy   CleanupPolicy
}
