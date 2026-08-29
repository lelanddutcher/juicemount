package farm

// Durable proxy commit receipts close the only cross-worker idempotency gap in
// the proxy pipeline. The large blob and the tiny manifest cannot be committed
// in one filesystem transaction. A worker can therefore publish proxy.mp4 and
// die (or lose storage/Redis) before its private SQLite row and shared
// manifest.json are durable. Without another proof, the next worker re-encodes
// and atomically replaces the same proxy; JuiceFS then retains the overwritten
// slices in trash, multiplying physical storage for no new logical output.
//
// The receipt is written and fsynced BEFORE the staged blob is renamed. It is
// bound to the exact blob bytes by size + the same sampled xxh3 recipe used for
// sources. A crash before rename leaves a receipt that cannot match the prior
// final blob; a crash after rename leaves enough shared evidence for any worker
// to reconstruct the row/manifest without encoding again.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"golang.org/x/sys/unix"
)

const (
	proxyReceiptName     = ".proxy-commit.json"
	proxyReceiptVersion  = 1
	maxProxyReceiptBytes = 16 << 10
)

type proxyCommitReceipt struct {
	Version     int    `json:"version"`
	Inode       uint64 `json:"inode"`
	SourceHash  string `json:"source_hash"`
	SourceSize  int64  `json:"source_size"`
	BlobHash    string `json:"blob_hash"`
	BlobSize    int64  `json:"blob_size"`
	Codec       string `json:"codec"`
	CodecString string `json:"codec_string"`
	WrittenAt   int64  `json:"written_at"`
}

func proxyReceiptFromStaged(dir *os.File, staged string, inode uint64, sourceHash string,
	sourceSize int64, codec, codecString string) (proxyCommitReceipt, error) {
	f, size, err := openRegularAt(dir, staged)
	if err != nil {
		return proxyCommitReceipt{}, err
	}
	defer f.Close()
	if size <= 0 {
		return proxyCommitReceipt{}, fmt.Errorf("proxy receipt: staged blob is empty")
	}
	blobHash, err := SampleHashFile(f, size)
	if err != nil {
		return proxyCommitReceipt{}, fmt.Errorf("proxy receipt: hash staged blob: %w", err)
	}
	return proxyCommitReceipt{
		Version: proxyReceiptVersion, Inode: inode,
		SourceHash: sourceHash, SourceSize: sourceSize,
		BlobHash: blobHash, BlobSize: size,
		Codec: codec, CodecString: codecString,
		WrittenAt: time.Now().Unix(),
	}, nil
}

func writeProxyCommitReceipt(dir *os.File, receipt proxyCommitReceipt) error {
	b, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	return derivatives.WriteFileAt(dir, proxyReceiptName, b, 0o644)
}

func readProxyCommitReceipt(dir *os.File) (proxyCommitReceipt, error) {
	f, size, err := openRegularAt(dir, proxyReceiptName)
	if err != nil {
		return proxyCommitReceipt{}, err
	}
	defer f.Close()
	if size <= 0 || size > maxProxyReceiptBytes {
		return proxyCommitReceipt{}, fmt.Errorf("proxy receipt: invalid size %d", size)
	}
	raw, err := io.ReadAll(io.LimitReader(f, maxProxyReceiptBytes+1))
	if err != nil {
		return proxyCommitReceipt{}, err
	}
	if len(raw) > maxProxyReceiptBytes {
		return proxyCommitReceipt{}, fmt.Errorf("proxy receipt: exceeds %d bytes", maxProxyReceiptBytes)
	}
	var receipt proxyCommitReceipt
	if err := json.Unmarshal(raw, &receipt); err != nil {
		return proxyCommitReceipt{}, err
	}
	return receipt, nil
}

func openRegularAt(dir *os.File, name string) (*os.File, int64, error) {
	if dir == nil {
		return nil, 0, fmt.Errorf("proxy receipt: nil directory")
	}
	fd, err := unix.Openat(int(dir.Fd()), name,
		unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, 0, err
	}
	f := os.NewFile(uintptr(fd), name)
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		f.Close()
		return nil, 0, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG {
		f.Close()
		return nil, 0, fmt.Errorf("proxy receipt: %q is not a regular file", name)
	}
	return f, st.Size, nil
}

// recoverProxyCommitReceipt repairs private/shared indexes from a proven final
// blob. found=true means the receipt matched the current source and final blob;
// callers must never encode again in that case, even if repairing an index
// returns an error. The next retry repeats only this cheap repair path.
func recoverProxyCommitReceipt(store *derivatives.Store, fi os.FileInfo, inode uint64,
	sourceHash string, opt Options) (found bool, err error) {
	if store == nil || fi == nil || opt.Mount == "" || opt.RegenerateFresh {
		return false, nil
	}
	dir, err := derivatives.OpenDirUnder(opt.Mount, derivatives.DerivDirRel(inode))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	defer dir.Close()
	receipt, err := readProxyCommitReceipt(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		// A malformed consumer-writable receipt is not proof of committed work.
		// Ignore it and regenerate through the normal fail-closed path.
		return false, nil
	}
	desiredCodec, _ := proxyCodecStrings(opt.ProxyVCodec, nil)
	codecAllowed := receipt.Codec == desiredCodec ||
		(opt.PreserveHEVCOnFallback && desiredCodec == "h264" && receipt.Codec == "hevc")
	if receipt.Version != proxyReceiptVersion || receipt.Inode != inode ||
		receipt.SourceHash != sourceHash || receipt.SourceSize != fi.Size() ||
		receipt.BlobSize <= 0 || !isHashHex(receipt.BlobHash) || !codecAllowed ||
		len(receipt.CodecString) == 0 || len(receipt.CodecString) > 160 {
		return false, nil
	}
	blob, blobSize, err := openRegularAt(dir, "proxy.mp4")
	if err != nil {
		return false, nil
	}
	defer blob.Close()
	if blobSize != receipt.BlobSize {
		return false, nil
	}
	blobHash, err := SampleHashFile(blob, blobSize)
	if err != nil || blobHash != receipt.BlobHash {
		return false, nil
	}
	actualCodec, err := probeProxyFileCodecContext(optionContext(opt), opt.FFprobeBin, blob)
	if err != nil || actualCodec != receipt.Codec {
		return false, nil
	}
	// Keep the token honest enough for consumers even though the volume receipt
	// is untrusted: its video half must correspond to the codec ffprobe proved.
	prefix := map[string]string{"h264": "avc1.", "hevc": "hvc1.", "av1": "av01."}[actualCodec]
	if prefix == "" || !strings.HasPrefix(receipt.CodecString, prefix) {
		return false, nil
	}
	rel, mediaType := "proxy.mp4", "video/mp4"
	codec, codecString, size := actualCodec, receipt.CodecString, blobSize
	row := derivatives.DerivRow{
		Kind: "proxy", Status: "ready", Producer: opt.Producer, Version: opt.Version,
		Hash: &sourceHash, BlobRelPath: &rel, MediaType: &mediaType,
		Codec: &codec, CodecString: &codecString, BlobSize: &size,
	}
	stampSource(&row, fi)
	if err := store.PutSource(inode, &sourceHash); err != nil {
		return true, fmt.Errorf("recover proxy receipt source row: %w", err)
	}
	if err := store.PutDeriv(inode, row); err != nil {
		return true, fmt.Errorf("recover proxy receipt derivative row: %w", err)
	}
	if err := WriteManifestSidecar(store, opt.Mount, inode); err != nil {
		return true, fmt.Errorf("recover proxy receipt manifest: %w", err)
	}
	return true, nil
}

func probeProxyFileCodecContext(ctx context.Context, ffprobeBin string, f *os.File) (string, error) {
	if ffprobeBin == "" {
		ffprobeBin = "ffprobe"
	}
	cmd := commandContext(ctx, ffprobeBin,
		"-v", "error", "-select_streams", "v:0", "-show_entries", "stream=codec_name",
		"-of", "default=nk=1:nw=1", "/dev/fd/3")
	cmd.ExtraFiles = []*os.File{f}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("ffprobe proxy codec: %w: %s", err, out)
	}
	codec := strings.ToLower(strings.TrimSpace(string(out)))
	if line, _, ok := strings.Cut(codec, "\n"); ok {
		codec = strings.TrimSpace(line)
	}
	if codec == "h265" {
		codec = "hevc"
	}
	return codec, nil
}
