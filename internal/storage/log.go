package storage

import (
	"errors"
	"fmt"
	"os"
)

var partitionLogOpenFile = os.OpenFile

type partitionLog struct {
	file      *os.File
	positions []int64
}

// openPartitionLog opens or creates a log and recovers its byte index.
func openPartitionLog(path string) (*partitionLog, error) {
	file, err := partitionLogOpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open partition log: %w", err)
	}

	positions, err := rebuildIndex(file)
	if err != nil {
		closeErr := file.Close()
		return nil, errors.Join(fmt.Errorf("recover partition log index: %w", err), closeErr)
	}
	return &partitionLog{file: file, positions: positions}, nil
}
