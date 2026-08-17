package farm

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

// Options configures a producer run.
type Options struct {
	Producer      string // stamped on every row: "macos-node" | "linux-farm" | "on-device"
	Version       int    // producer schema/algo version
	Mount         string // mount point, for resolving the Tier-A blob dir (only used when Blobs/Filmstrip)
	Blobs         bool   // also generate poster thumbnails into Tier-A
	ThumbMaxDim   int    // poster fit box (px); 0 → 720 (D2 density ask, 2026-07-21)
	Filmstrip     bool   // also generate a filmstrip sprite-sheet (JM-16) into Tier-A
	FilmstripCell int    // filmstrip cell width (px); 0 → 320 (D5 density ask, 2026-07-21)
	Waveform      bool   // also generate an audio waveform overview (JM-18) into Tier-A
	WaveformSPP   int    // waveform samples-per-pixel; 0 → 1024
	FFprobeBin    string // override; "" → "ffprobe" on PATH
	FFmpegBin     string // override; "" → "ffmpeg" on PATH
	WhisperBin    string // whisper.cpp CLI; "" → "whisper-cli" on PATH (transcripts)
	WhisperModel  string // path to a ggml whisper model (required for transcripts)
	ProxyVCodec   string // proxy H.264 encoder; "" → "libx264" (GPU: h264_nvenc/qsv/vaapi)
	ProxyCRF      int    // proxy quality; 0 → 21 (lower = sharper/bigger)
	ProxyPreset   string // proxy x264 preset; "" → "slow" (faster preset = quicker, larger)

	// MinBlobSizeBytes gates the EXPENSIVE blob generators (poster, filmstrip,
	// waveform) — a file below it still gets its cheap `tech` probe row (so it
	// stays discoverable in OpenLoupe), but skips the decode-heavy derivatives
	// a sub-threshold clip doesn't warrant in a mass sweep (2026-07-14). 0 =
	// generate blobs for everything (old behavior). Gating blobs (not the whole
	// asset) keeps metadata coverage while cutting the mass-sweep cost.
	MinBlobSizeBytes int64

	// RegenerateFresh forces work on an asset whose derivatives are already
	// present and whose source is byte-identical. Default false = SKIP it.
	//
	// WHY THE DEFAULT IS SKIP. A job targets a DIRECTORY, and the watcher
	// enqueues a directory whenever media settles in it — including when a
	// folder is merely MOVED or reorganised. Without this gate Process
	// re-encodes the poster, filmstrip and waveform for every already-derived
	// file in that folder, every time, for content that has not changed a byte.
	// On a shoot folder of camera masters that is minutes of CPU to reproduce
	// artifacts that already exist.
	//
	// A MOVE within the volume preserves the JuiceFS inode, the size and the
	// content, so it lands here as "known, same hash, same size" and costs one
	// stat plus one sampled hash. A real EDIT changes the content, so the hash
	// moves and the asset regenerates. That is the whole discriminator, and the
	// data it needs was already being stamped on every row by stampSource.
	RegenerateFresh bool
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

	// SkippedFresh reports that the asset already had derivatives for
	// byte-identical source, so nothing was re-encoded. Surfaced so a move can
	// be SEEN as a move rather than inferred from an absence of log lines.
	SkippedFresh bool
	// SidecarRepaired reports that the skip still wrote a missing manifest.json.
	// A skip must not leave an asset unindexed just because its blobs predate
	// the sidecar emit.
	SidecarRepaired bool
}

// derivDirFor opens (creating if needed) an asset's derivative directory and
// returns its DESCRIPTOR, symlink-checked at every component.
//
// This replaces DerivBlobDir, which returned a joined absolute path. That was
// the write-side twin of the unanchored open helper deleted in c01a6eb: a path
// can only be guarded at the moment it is produced, and every caller then
// re-resolved it by name at write time. Handing out a descriptor instead means
// the callers physically cannot re-resolve, so the guard cannot be bypassed by
// a caller that simply forgets — which is what happened at five call sites
// across five review rounds.
//
// Callers must Close the returned directory.
// stageUnder creates the output name exclusively inside the held directory and
// returns the path to hand ffmpeg. A nil dir (mount unset, or the directory
// refused because a component was a symlink) yields an error, which the caller
// records as a blob error — never as a silent skip.
func stageUnder(dir *os.File, name string) (staged, absPath string, err error) {
	if dir == nil {
		return "", "", fmt.Errorf("no safe derivative directory")
	}
	return derivatives.StageNameAt(dir, name)
}

// commitStaged moves a generated blob onto its final name THROUGH the directory
// descriptor, so the commit cannot be redirected even though the subprocess
// wrote by path.
func commitStaged(dir *os.File, staged, final string) error {
	if dir == nil {
		return fmt.Errorf("no safe derivative directory")
	}
	return derivatives.CommitStagedAt(dir, staged, final)
}

func derivDirFor(mount string, inode uint64) (*os.File, error) {
	rel := derivatives.DerivDirRel(inode)
	if err := derivatives.EnsureDirUnder(mount, rel); err != nil {
		return nil, err
	}
	return derivatives.OpenDirUnder(mount, rel)
}

// stampSource records the source file's size + mtime on a derivative row so a
// consumer can stat-verify the derivative directly off the manifest (the
// read-gate: live /lookup size == row.source_size) without a separate tech row.
func stampSource(row *derivatives.DerivRow, fi os.FileInfo) {
	sz := fi.Size()
	mt := fi.ModTime().Unix()
	row.SourceSize = &sz
	row.SourceMtime = &mt
}

// skipIfFresh reports whether an asset already has usable derivatives for
// byte-identical source, and repairs a missing manifest while it is there.
//
// It requires ALL of:
//   - the store knows this inode and has a recorded source hash
//   - that hash equals the one just sampled  (content unchanged)
//   - at least one ready derivative row, stamped with this exact source size
//
// Anything less regenerates. Being wrong in the SKIP direction leaves a stale
// derivative serving fresh content, so every uncertainty resolves to "do the
// work": an unknown inode, a missing hash, no ready rows, or a size that does
// not match all fall through.
//
// The sidecar repair matters as much as the skip. An asset generated before the
// JM-15 emit worked has blobs and rows but no manifest.json, so the consumer
// refuses it as "not indexed". Skipping without writing that manifest would
// leave it invisible forever, since the skip means the generate path — the only
// other place the manifest is written — never runs again.
func skipIfFresh(store *derivatives.Store, path string, inode uint64, hash string, size int64, opt Options) (fresh, repaired bool) {
	known, srcHash := store.Known(inode)
	if !known || srcHash == nil || *srcHash != hash {
		return false, false
	}
	rows, err := store.Manifest(inode)
	if err != nil || len(rows) == 0 {
		return false, false
	}
	ready := 0
	for _, r := range rows {
		if r.Status != "ready" {
			continue
		}
		// A row with no stamped size predates the read-gate and cannot vouch
		// for the source it was made from.
		if r.SourceSize == nil || *r.SourceSize != size {
			return false, false
		}
		ready++
	}
	if ready == 0 {
		return false, false
	}
	if opt.Mount != "" {
		if err := WriteManifestSidecar(store, opt.Mount, inode); err == nil {
			repaired = true
		}
	}
	return true, repaired
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

	// FRESHNESS GATE — the move-vs-edit discriminator.
	//
	// Placed AFTER the sampled hash (cheap: a stat plus a few reads) and BEFORE
	// ffprobe and every decode-heavy generator, because those are the cost. A
	// folder that was merely MOVED re-enqueues its whole directory, and without
	// this every already-derived file in it is re-postered, re-filmstripped and
	// re-waveformed for content that has not changed a byte.
	//
	// Identical content is the test, not an identical path: a move preserves the
	// JuiceFS inode, the size and the bytes, so it matches here; an edit changes
	// the bytes, so the sampled hash moves and the asset regenerates. Size is
	// checked alongside the hash because a sampled hash reads a few windows
	// rather than the whole file — two files can share sampled windows and
	// differ in length, and the pair is far stronger than either alone.
	if !opt.RegenerateFresh {
		if fresh, repaired := skipIfFresh(store, path, inode, hash, size, opt); fresh {
			res.SkippedFresh = true
			res.SidecarRepaired = repaired
			return res
		}
	}

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
	// Blob size gate: a sub-threshold clip keeps its tech row above but skips
	// the decode-heavy poster/filmstrip/waveform. 0 = generate for everything.
	blobBigEnough := opt.MinBlobSizeBytes <= 0 || size >= opt.MinBlobSizeBytes
	// Hold a DESCRIPTOR for the derivative directory for the whole pass. Every
	// write below goes through it, so nothing re-resolves a path that an
	// attacker can swap mid-encode. os.MkdirAll — which every generator calls — FOLLOWS a
	// symlinked component, so a planted .juicemount/derivatives/<inode> link
	// redirects ffmpeg output, the transcript blob and the manifest itself out of
	// the volume, on a host where the farm runs as root. Creating it safely here
	// means each generator's own MkdirAll finds it already present.
	//
	// UNCONDITIONAL on purpose. Gating this on opt.Blobs left real holes: the
	// waveform is gated on opt.Waveform, the transcript on its own flag, and the
	// manifest is written for every asset regardless.
	//
	// NON-FATAL on purpose too. Making it fatal was a regression: Mount=="" is a
	// supported configuration elsewhere in this same function, and a transient
	// EROFS/ENOSPC or a mid-sweep unmount would have dropped the whole asset —
	// including the cheap `tech` row that a -blobs=false run exists to publish.
	// A directory we cannot safely open means no BLOBS, not no result.
	var derivDir *os.File
	if opt.Mount != "" {
		d, derr := derivDirFor(opt.Mount, inode)
		if derr != nil {
			blobErrs = append(blobErrs, fmt.Errorf("derivative dir: %w", derr))
		} else {
			derivDir = d
			defer derivDir.Close()
		}
	}
	if opt.Blobs && tech.Video != nil && blobBigEnough {
		rel := "poster.jpg"
		mt := "image/jpeg"
		staged, out, stErr := stageUnder(derivDir, rel)
		err := stErr
		if err == nil {
			if err = Thumbnail(opt.FFmpegBin, path, out, opt.ThumbMaxDim, tech.DurationMS); err == nil {
				err = commitStaged(derivDir, staged, rel)
			}
		}
		if err != nil {
			derivatives.DiscardStagedAt(derivDir, staged)
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

	if opt.Filmstrip && tech.Video != nil && blobBigEnough {
		rel := "strip.jpg"
		mt := "image/jpeg"
		staged, out, stErr := stageUnder(derivDir, rel)
		var geo *derivatives.FilmstripGeo
		err := stErr
		if err == nil {
			if geo, err = Filmstrip(opt.FFmpegBin, path, out, tech.DurationMS, tech.Video.Width, tech.Video.Height, opt.FilmstripCell, tech.Video.FPS); err == nil {
				err = commitStaged(derivDir, staged, rel)
			}
		}
		if err != nil {
			derivatives.DiscardStagedAt(derivDir, staged)
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

	if opt.Waveform && len(tech.Audio) > 0 && blobBigEnough {
		rel := "waveform.json"
		mt := "application/json"
		// No staging for the waveform: it is our own write, so it goes straight
		// through the descriptor and commits atomically inside WriteFileAt.
		err := error(nil)
		if derivDir == nil {
			err = fmt.Errorf("no safe derivative directory")
		}
		wrote := false
		if err == nil {
			// Waveform returns (0, nil) when the source has no audio — a success
			// that writes NOTHING. The enclosing gate uses tech.Audio from an
			// EARLIER, separate ffprobe, so the two can disagree whenever the
			// source changes underneath us, which is the normal condition on a
			// volume a second app writes. Treat "no samples" as no blob.
			var n int
			if n, err = Waveform(opt.FFmpegBin, path, derivDir, rel, opt.WaveformSPP); err == nil && n > 0 {
				wrote = true
			}
		}
		if err == nil && !wrote {
			// No audio after all: publish NO row, exactly as a pre-staging run
			// would have done. WriteFileAt was never called, so there is nothing
			// on disk to clean up either.
		} else if err != nil {
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

	// Stamp the source size+mtime on every row (consumer read-gate).
	for i := range rows {
		stampSource(&rows[i], fi)
	}

	if err := store.IngestTech(inode, &hash, opt.Producer, opt.Version, payload, rows); err != nil {
		res.Err = fmt.Errorf("ingest: %w", err)
		res.ThumbWrote, res.FilmWrote, res.WaveWrote = false, false, false
		return res
	}
	// Blob failures are non-fatal — the tech row is published and the manifest
	// carries failed blob rows; the consumer regenerates those locally.
	res.BlobErr = errors.Join(blobErrs...)
	// JM-15: mirror the committed manifest to a volume sidecar so the Mac can
	// reconcile server-generated rows. Best-effort (recoverable next pass).
	if opt.Mount != "" {
		if err := WriteManifestSidecar(store, opt.Mount, inode); err != nil {
			res.BlobErr = errors.Join(res.BlobErr, fmt.Errorf("sidecar: %w", err))
		}
	}
	return res
}
