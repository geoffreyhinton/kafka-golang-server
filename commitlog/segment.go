package commitlog

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	logNameFormat   = "%020d.log"
	indexNameFormat = "%020d.index"
)

type segment struct {
	writer      io.Writer
	reader      io.Reader
	file        *os.File
	startOffset int64
}

func NewSegment(path string, startOffset int64) (*segment, error) {
	filePath := filepath.Join(path, fmt.Sprintf(logNameFormat, startOffset))
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &segment{
		writer:      file,
		reader:      file,
		file:        file,
		startOffset: startOffset,
	}, nil
}

func (s *segment) Write(p []byte) (n int, err error) {
	return s.writer.Write(p)
}

func (s *segment) Read(p []byte) (n int, err error) {
	return s.reader.Read(p)
}

func (s *segment) ReadAt(p []byte, off int64) (n int, err error) {
	return s.file.ReadAt(p, off)
}
