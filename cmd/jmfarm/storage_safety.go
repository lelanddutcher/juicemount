package main

import (
	"errors"
	"fmt"
	"strings"
	"syscall"
)

var errFarmStoragePressure = errors.New("farm storage safety interlock")

const defaultWorkerStorageReserve = uint64(64 << 30)

type workerStorageHeadroom struct {
	Total     uint64
	Available uint64
	Required  uint64
}

func checkWorkerStorageHeadroom(path string, configured uint64) (workerStorageHeadroom, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return workerStorageHeadroom{}, err
	}
	if st.Bsize <= 0 {
		return workerStorageHeadroom{}, fmt.Errorf("invalid filesystem block size %d", st.Bsize)
	}
	bsize := uint64(st.Bsize)
	headroom := workerStorageHeadroom{
		Total: uint64(st.Blocks) * bsize, Available: uint64(st.Bavail) * bsize,
	}
	headroom.Required = configured
	if headroom.Required == 0 {
		headroom.Required = headroom.Total / 100
		if headroom.Required < defaultWorkerStorageReserve {
			headroom.Required = defaultWorkerStorageReserve
		}
	}
	return headroom, nil
}

// isFarmStoragePressure recognizes both native write errors and subprocess
// text. ffmpeg reports a failed trailer/close through CombinedOutput, which
// loses the original errno; Redis reports the same pool exhaustion as MISCONF.
// Treat EIO as a safety event too: on a network filesystem it means the output
// path cannot prove durable bytes, never evidence that a GPU codec is broken.
func isFarmStoragePressure(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT) ||
		errors.Is(err, syscall.EIO) || errors.Is(err, syscall.EROFS) ||
		errors.Is(err, syscall.ESTALE) || errors.Is(err, syscall.ENOTCONN) {
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, marker := range []string{
		"no space left on device",
		"disk quota exceeded",
		"input/output error",
		"read-only file system",
		"errors writing to the aof file",
		"error writing trailer",
		"error closing file",
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}
