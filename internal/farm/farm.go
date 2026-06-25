package farm

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

// Options configures a producer run.
type Options struct {
	Producer      string // stamped on every row: "macos-node" | "linux-farm" | "on-device"
	Version       int    // producer schema/algo version
	Mount         string // mount point, for resolving the Tier-A blob dir (only used when Blobs/Filmstrip)
	Blobs         bool   // also generate poster thumbnails into Tier-A
	ThumbMaxDim   int    // poster fit box (px); 0 → 640
	Filmstrip     bool   // also generate a filmstrip sprite-sheet (JM-16) into Tier-A
	FilmstripCell int    // filmstrip cell width (px); 0 → 160
	Waveform      bool   // also generate an audio waveform overview (JM-18) into Tier-A
	WaveformSPP   int    // waveform samples-per-pixel; 0 → 1024
	FFprobeBin    string // override; "" → "ffprobe" on PATH
	FFmpegBin     string // override; "" → "ffmpeg" on PATH
	WhisperBin    string // whisper.cpp CLI; "" → "whisper-cli" on PATH (transcripts)
	WhisperModel  string // path to a ggml whisper model (required for transcripts)
}

// Result is the per-file outcome (for CLI reporting / JM-15 accounting). Err is a
// HARD failure (the asset was not published). BlobErr is non-fatal: the tech row
// is published, but a requested poster/filmstrip couldn't be rendered — the
// manifest carries a status:"failed" blob row and the consumer regenerates it.
type Result struct {
	Path       string
	Inode      uint64
	Hash       string
	DurationMS int64
	HasVideo   bool
	ThumbWrote bool
	FilmWrote  bool
	WaveWrote  bool
	Err        error
	BlobErr    error
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
	// st.Ino is uint64 on darwin/linux; this is the JuiceFS inode the NFS mount
	// exposes and /lookup returns, so the consumer queries by the same value.
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

	// Assemble the manifest rows. The tech row is always present; the thumbnail
	// blob is generated FIRST (slow, external) so its row reflects reality, then
	// everything is committed in a single transaction (IngestTech) — the asset
	// never appears half-written to the serving side.
	rows := []derivatives.DerivRow{
		{Kind: "tech", Status: "ready", Producer: opt.Producer, Version: opt.Version, Hash: &hash},
	}
	var blobErrs []error
	if opt.Blobs && tech.Video != nil {
		rel := "poster.jpg"
		mt := "image/jpeg"
		out := filepath.Join(DerivBlobDir(opt.Mount, inode), rel)
		if err := Thumbnail(opt.FFmpegBin, path, out, opt.ThumbMaxDim); err != nil {
			blobErrs = append(blobErrs, fmt.Errorf("thumbnail: %w", err))
			rows = append(rows, derivatives.DerivRow{
				Kind: "thumbnail", Status: "failed", Producer: opt.Producer, Version: opt.Version,
				Hash: &hash, BlobRelPath: &rel, MediaType: &mt,
			})
		} else {
			rows = append(rows, derivatives.DerivRow{
				Kind: "thumbnail", Status: "ready", Producer: opt.Producer, Version: opt.Version,
				Hash: &hash, BlobRelPath: &rel, MediaType: &mt,
			})
			res.ThumbWrote = true
		}
	}

	if opt.Filmstrip && tech.Video != nil {
		rel := "strip.jpg"
		mt := "image/jpeg"
		out := filepath.Join(DerivBlobDir(opt.Mount, inode), rel)
		geo, err := Filmstrip(opt.FFmpegBin, path, out, tech.DurationMS, tech.Video.Width, tech.Video.Height, opt.FilmstripCell)
		if err != nil {
			blobErrs = append(blobErrs, fmt.Errorf("filmstrip: %w", err))
			rows = append(rows, derivatives.DerivRow{
				Kind: "filmstrip", Status: "failed", Producer: opt.Producer, Version: opt.Version,
				Hash: &hash, BlobRelPath: &rel, MediaType: &mt,
			})
		} else {
			rows = append(rows, derivatives.DerivRow{
				Kind: "filmstrip", Status: "ready", Producer: opt.Producer, Version: opt.Version,
				Hash: &hash, BlobRelPath: &rel, MediaType: &mt, Filmstrip: geo,
			})
			res.FilmWrote = true
		}
	}

	if opt.Waveform && len(tech.Audio) > 0 {
		rel := "waveform.json"
		mt := "application/json"
		out := filepath.Join(DerivBlobDir(opt.Mount, inode), rel)
		if _, err := Waveform(opt.FFmpegBin, path, out, opt.WaveformSPP); err != nil {
			blobErrs = append(blobErrs, fmt.Errorf("waveform: %w", err))
			rows = append(rows, derivatives.DerivRow{
				Kind: "waveform", Status: "failed", Producer: opt.Producer, Version: opt.Version,
				Hash: &hash, BlobRelPath: &rel, MediaType: &mt,
			})
		} else {
			rows = append(rows, derivatives.DerivRow{
				Kind: "waveform", Status: "ready", Producer: opt.Producer, Version: opt.Version,
				Hash: &hash, BlobRelPath: &rel, MediaType: &mt,
			})
			res.WaveWrote = true
		}
	}

	if err := store.IngestTech(inode, &hash, opt.Producer, opt.Version, payload, rows); err != nil {
		res.Err = fmt.Errorf("ingest: %w", err)
		res.ThumbWrote, res.FilmWrote, res.WaveWrote = false, false, false
		return res
	}
	// Blob failures are non-fatal — the tech row is published and the manifest
	// carries failed blob rows; the consumer regenerates those locally.
	res.BlobErr = errors.Join(blobErrs...)
	return res
}
