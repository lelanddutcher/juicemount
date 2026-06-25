package farm

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

// Options configures a producer run.
type Options struct {
	Producer    string // stamped on every row: "macos-node" | "linux-farm" | "on-device"
	Version     int    // producer schema/algo version
	Mount       string // mount point, for resolving the Tier-A blob dir (only used when Blobs)
	Blobs       bool   // also generate poster thumbnails into Tier-A
	ThumbMaxDim int    // poster fit box (px); 0 → 640
	FFprobeBin  string // override; "" → "ffprobe" on PATH
	FFmpegBin   string // override; "" → "ffmpeg" on PATH
}

// Result is the per-file outcome (for CLI reporting / JM-15 accounting).
type Result struct {
	Path       string
	Inode      uint64
	Hash       string
	DurationMS int64
	HasVideo   bool
	ThumbWrote bool
	Err        error
}

// DerivBlobDir is the Tier-A on-volume location for an asset's blobs:
// <mount>/.juicemount/derivatives/<inode>/. Inode-addressed so it survives
// renames/remounts and is shareable across machines; web-servable as-is.
func DerivBlobDir(mount string, inode uint64) string {
	return filepath.Join(mount, ".juicemount", "derivatives", strconv.FormatUint(inode, 10))
}

// Process derives all artifacts for one file and writes them through the store:
// source_assets (inode+hash), metadata(kind=tech), a tech manifest row, and —
// when Options.Blobs — a poster thumbnail blob + its manifest row. Idempotent
// (every write is an upsert), so re-running re-derives in place.
func Process(store *derivatives.Store, path string, opt Options) Result {
	res := Result{Path: path}

	fi, err := os.Stat(path)
	if err != nil {
		res.Err = err
		return res
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		res.Err = fmt.Errorf("stat: no syscall.Stat_t for %q", path)
		return res
	}
	inode := uint64(st.Ino)
	size := fi.Size()
	res.Inode = inode

	hash, err := SampleHash(path, size)
	if err != nil {
		res.Err = fmt.Errorf("hash: %w", err)
		return res
	}
	res.Hash = hash

	tech, err := Probe(opt.FFprobeBin, path, size)
	if err != nil {
		res.Err = err
		return res
	}
	res.DurationMS = tech.DurationMS
	res.HasVideo = tech.Video != nil

	payload, err := json.Marshal(tech)
	if err != nil {
		res.Err = fmt.Errorf("marshal tech: %w", err)
		return res
	}

	// Tier-B writes. source first (drives /derivatives exists + source_hash),
	// then the structured tech, then the manifest row pointing at it.
	if err := store.PutSource(inode, &hash); err != nil {
		res.Err = fmt.Errorf("put source: %w", err)
		return res
	}
	if err := store.PutMetadata(inode, "tech", opt.Producer, opt.Version, &hash, payload); err != nil {
		res.Err = fmt.Errorf("put metadata: %w", err)
		return res
	}
	if err := store.PutDeriv(inode, derivatives.DerivRow{
		Kind: "tech", Status: "ready", Producer: opt.Producer, Version: opt.Version, Hash: &hash,
	}); err != nil {
		res.Err = fmt.Errorf("put tech deriv: %w", err)
		return res
	}

	// Tier-A poster blob (optional, video only).
	if opt.Blobs && tech.Video != nil {
		rel := "poster.jpg"
		out := filepath.Join(DerivBlobDir(opt.Mount, inode), rel)
		if err := Thumbnail(opt.FFmpegBin, path, out, opt.ThumbMaxDim); err != nil {
			// Non-fatal: the tech row is already written and useful on its own.
			// Record a failed thumbnail row so the manifest is honest.
			mt := "image/jpeg"
			_ = store.PutDeriv(inode, derivatives.DerivRow{
				Kind: "thumbnail", Status: "failed", Producer: opt.Producer, Version: opt.Version,
				Hash: &hash, BlobRelPath: &rel, MediaType: &mt,
			})
			res.Err = fmt.Errorf("thumbnail: %w", err)
			return res
		}
		mt := "image/jpeg"
		if err := store.PutDeriv(inode, derivatives.DerivRow{
			Kind: "thumbnail", Status: "ready", Producer: opt.Producer, Version: opt.Version,
			Hash: &hash, BlobRelPath: &rel, MediaType: &mt,
		}); err != nil {
			res.Err = fmt.Errorf("put thumbnail deriv: %w", err)
			return res
		}
		res.ThumbWrote = true
	}

	return res
}
