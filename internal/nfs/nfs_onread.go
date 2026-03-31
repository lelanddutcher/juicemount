package nfs

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/willscott/go-nfs-client/nfs/xdr"
)

type nfsReadArgs struct {
	Handle []byte
	Offset uint64
	Count  uint32
}

type nfsReadResponse struct {
	Count uint32
	EOF   uint32
	Data  []byte
}

// MaxRead is the advertised largest buffer the server is willing to read
const MaxRead = 1 << 24

// CheckRead is a size where - if a request to read is larger than this,
// the server will stat the file to learn it's actual size before allocating
// a buffer to read into.
const CheckRead = 1 << 15

// [JM5] Pool for READ data buffers to avoid per-RPC allocation.
// The default capacity covers the common case (1MB rsize).
var readDataPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 1<<20) // 1MB
		return &buf
	},
}

func onRead(ctx context.Context, w *response, userHandle Handler) error {
	w.errorFmt = opAttrErrorFormatter
	var obj nfsReadArgs
	err := xdr.Read(w.req.Body, &obj)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	fs, path, err := userHandle.FromHandle(obj.Handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}

	fh, err := fs.Open(fs.Join(path...))
	if err != nil {
		if os.IsNotExist(err) {
			return &NFSStatusError{NFSStatusNoEnt, err}
		}
		return &NFSStatusError{NFSStatusAccess, err}
	}
	defer fh.Close()

	resp := nfsReadResponse{}

	if obj.Count > CheckRead {
		info, err := fs.Stat(fs.Join(path...))
		if err != nil {
			return &NFSStatusError{NFSStatusAccess, err}
		}
		if info.Size()-int64(obj.Offset) < int64(obj.Count) {
			obj.Count = uint32(uint64(info.Size()) - obj.Offset)
		}
	}
	if obj.Count > MaxRead {
		obj.Count = MaxRead
	}

	// [JM5] Use pooled buffer for read data when it fits the common 1MB case.
	var pooled bool
	if obj.Count <= 1<<20 {
		bufPtr := readDataPool.Get().(*[]byte)
		resp.Data = (*bufPtr)[:obj.Count]
		pooled = true
		defer func() {
			if pooled {
				readDataPool.Put(bufPtr)
			}
		}()
	} else {
		resp.Data = make([]byte, obj.Count)
	}

	cnt, err := fh.ReadAt(resp.Data, int64(obj.Offset))
	if err != nil && !errors.Is(err, io.EOF) {
		return &NFSStatusError{NFSStatusIO, err}
	}
	resp.Count = uint32(cnt)
	resp.Data = resp.Data[:resp.Count]
	if errors.Is(err, io.EOF) {
		resp.EOF = 1
	}

	// [JM5] Use pooled response buffer instead of allocating.
	writer := getResponseBuffer()
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		putResponseBuffer(writer)
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := WritePostOpAttrs(writer, tryStat(fs, path)); err != nil {
		putResponseBuffer(writer)
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := xdr.Write(writer, resp); err != nil {
		putResponseBuffer(writer)
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	bodyBytes := writer.Bytes()
	writeErr := w.Write(bodyBytes)
	putResponseBuffer(writer)
	if writeErr != nil {
		return &NFSStatusError{NFSStatusServerFault, writeErr}
	}
	return nil
}
