package commitlog

import (
	"io"
	"os"
	"sync"
)

const (
	fileFormat    = "%020d%s"
	logSuffix     = ".log"
	cleanedSuffix = ".cleaned"
	indexSuffix   = ".index"
)

type segment struct {
	writer     io.Writer
	reader     io.Reader
	log        *os.File
	Index      *Index
	BaseOffset int64
	NextOffset int64
	Position   int64
	maxBytes   int64
	path       string
	suffix     string

	sync.Mutex
}
