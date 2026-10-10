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
	"math"
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
	sourceSize int64, codec, codecString string) (proxyCommitReceipt, *os.File, error) {
	f, size, err := openRegularAt(dir, staged)
	if err != nil {
		return proxyCommitReceipt{}, nil, err
	}
	if size <= 0 {
		f.Close()
		return proxyCommitReceipt{}, nil, fmt.Errorf("proxy receipt: staged blob is empty")
	}
	blobHash, err := SampleHashFile(f, size)
	if err != nil {
		f.Close()
		return proxyCommitReceipt{}, nil, fmt.Errorf("proxy receipt: hash staged blob: %w", err)
	}
	return proxyCommitReceipt{
		Version: proxyReceiptVersion, Inode: inode,
		SourceHash: sourceHash, SourceSize: sourceSize,
		BlobHash: blobHash, BlobSize: size,
		Codec: codec, CodecString: codecString,
		WrittenAt: time.Now().Unix(),
	}, f, nil
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

// verifyRegularAt pins the named blob to the same regular inode, size and
// modification time observed through the held descriptor before probing.
func verifyRegularAt(dir *os.File, name string, held *os.File, before os.FileInfo) error {
	if held == nil || before == nil || !before.Mode().IsRegular() || before.Size() <= 0 {
		return fmt.Errorf("proxy blob has no held regular-file witness")
	}
	current, err := held.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(before, current) || current.Size() != before.Size() || !current.ModTime().Equal(before.ModTime()) {
		return fmt.Errorf("held proxy blob changed after probe")
	}
	named, size, err := openRegularAt(dir, name)
	if err != nil {
		return err
	}
	defer named.Close()
	namedInfo, err := named.Stat()
	if err != nil {
		return err
	}
	if size != before.Size() || !os.SameFile(before, namedInfo) || !namedInfo.ModTime().Equal(before.ModTime()) {
		return fmt.Errorf("named proxy blob changed after probe")
	}
	return nil
}

func verifyRegularNamedFile(held *os.File, path string, before os.FileInfo) error {
	if held == nil || before == nil || !before.Mode().IsRegular() || before.Size() <= 0 {
		return fmt.Errorf("local proxy has no held regular-file witness")
	}
	current, err := held.Stat()
	if err != nil {
		return err
	}
	named, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !named.Mode().IsRegular() || !os.SameFile(before, current) || !os.SameFile(before, named) ||
		current.Size() != before.Size() || named.Size() != before.Size() ||
		!current.ModTime().Equal(before.ModTime()) || !named.ModTime().Equal(before.ModTime()) {
		return fmt.Errorf("local proxy name or bytes changed after probe")
	}
	return nil
}

// Only the proxy byte-derived fields may override a newer sidecar row. Run
// after ordinary merge while holding its existing per-inode manifest lock.
func writeVerifiedProxyManifest(store *derivatives.Store, mount string, inode uint64,
	sourceHash string, sourceSize int64, blob *os.File, before os.FileInfo, metadata proxyOutputMetadata) error {
	return writeManifestSidecarWithPostMerge(store, mount, inode, func(sc *ManifestSidecar, dir *os.File) error {
		if sc.SourceHash == nil || *sc.SourceHash != sourceHash {
			return fmt.Errorf("proxy manifest source binding changed")
		}
		if err := verifyRegularAt(dir, "proxy.mp4", blob, before); err != nil {
			return err
		}
		for i := range sc.Derivatives {
			row := &sc.Derivatives[i]
			if row.Kind != "proxy" {
				continue
			}
			if row.Status != "ready" || row.Hash == nil || *row.Hash != sourceHash ||
				row.SourceSize == nil || *row.SourceSize != sourceSize ||
				row.BlobRelPath == nil || *row.BlobRelPath != "proxy.mp4" ||
				row.BlobSize == nil || *row.BlobSize != before.Size() {
				return fmt.Errorf("proxy manifest row lost its source/blob binding")
			}
			row.Codec, row.CodecString = &metadata.Codec, &metadata.CodecString
			return nil
		}
		return fmt.Errorf("proxy manifest has no ready row")
	})
}

func repairFreshProxyMetadata(store *derivatives.Store, fi os.FileInfo, inode uint64,
	sourceHash string, opt Options) (bool, error) {
	dir, err := derivatives.OpenDirUnder(opt.Mount, derivatives.DerivDirRel(inode))
	if err != nil {
		return true, err
	}
	defer dir.Close()
	blob, size, err := openRegularAt(dir, "proxy.mp4")
	if err != nil {
		return true, err
	}
	defer blob.Close()
	before, err := blob.Stat()
	if err != nil {
		return true, fmt.Errorf("stat fresh proxy: %w", err)
	}
	if size <= 0 {
		return true, fmt.Errorf("fresh proxy is empty")
	}
	metadata, err := probeProxyOutputMetadata(optionContext(opt), opt.FFprobeBin, blob)
	if err != nil {
		return true, err
	}
	if err := verifyRegularAt(dir, "proxy.mp4", blob, before); err != nil {
		return true, err
	}
	allowed, _ := proxyCodecSatisfied(&metadata.Codec, proxyDesiredCodec(opt.ProxyVCodec), opt)
	if !allowed {
		return false, nil
	}
	rows, err := store.Manifest(inode)
	if err != nil {
		return true, err
	}
	matched := false
	for _, row := range rows {
		if row.Kind != "proxy" || row.Status != "ready" || row.Hash == nil || *row.Hash != sourceHash ||
			row.SourceSize == nil || *row.SourceSize != fi.Size() || row.BlobRelPath == nil || *row.BlobRelPath != "proxy.mp4" ||
			(row.BlobSize != nil && *row.BlobSize != size) {
			continue
		}
		matched = true
		if row.Codec == nil || *row.Codec != metadata.Codec || row.CodecString == nil || *row.CodecString != metadata.CodecString || row.BlobSize == nil {
			row.Codec, row.CodecString, row.BlobSize = &metadata.Codec, &metadata.CodecString, &size
			// Imported sidecars may have future timestamps. Keep this change
			// visible to a consumer already holding the previous cursor.
			if row.UpdatedAt == math.MaxInt64 {
				return true, fmt.Errorf("proxy metadata change timestamp exhausted")
			}
			row.UpdatedAt = max(time.Now().Unix(), row.UpdatedAt+1)
			if err := store.PutDeriv(inode, row); err != nil {
				return true, err
			}
		}
		break
	}
	if !matched {
		return true, fmt.Errorf("fresh proxy row disappeared before metadata repair")
	}
	if err := writeVerifiedProxyManifest(store, opt.Mount, inode, sourceHash, fi.Size(), blob, before, metadata); err != nil {
		return true, err
	}
	// Existing receipts are immutable historical source/blob evidence. Rewriting
	// one here could clobber a concurrent producer's pre-rename receipt. Its
	// codec token is informational; recovery derives current metadata again.

	return true, nil
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
	if receipt.Version != proxyReceiptVersion || receipt.Inode != inode ||
		receipt.SourceHash != sourceHash || receipt.SourceSize != fi.Size() ||
		receipt.BlobSize <= 0 || !isHashHex(receipt.BlobHash) {
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
	before, err := blob.Stat()
	if err != nil {
		return true, err
	}
	blobHash, err := SampleHashFile(blob, blobSize)
	if err != nil {
		return true, err
	}
	if blobHash != receipt.BlobHash {
		return false, nil
	}
	if err := verifyRegularAt(dir, "proxy.mp4", blob, before); err != nil {
		return true, err
	}
	metadata, err := probeProxyOutputMetadata(optionContext(opt), opt.FFprobeBin, blob)
	if err != nil {
		return true, err
	}
	if err := verifyRegularAt(dir, "proxy.mp4", blob, before); err != nil {
		return true, err
	}
	if metadata.Codec != receipt.Codec {
		return false, nil
	}
	codecAllowed, _ := proxyCodecSatisfied(&metadata.Codec, proxyDesiredCodec(opt.ProxyVCodec), opt)
	if !codecAllowed {
		return false, nil
	}
	rel, mediaType := "proxy.mp4", "video/mp4"
	codec, codecString, size := metadata.Codec, metadata.CodecString, blobSize
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
	if err := writeVerifiedProxyManifest(store, opt.Mount, inode, sourceHash, fi.Size(), blob, before, metadata); err != nil {
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
