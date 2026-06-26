package farm

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

// --- LoupeJSON: the OpenLoupe sidecar shape, byte-for-byte
// (OpenLoupeSidecar/LoupeJSON.swift). The farm writes `ai.loupe.json` in EXACTLY
// this shape (snake_case keys, ISO8601 `indexed_at`) so OpenLoupe's D6 trust gate
// (`applyLoupeJSON`) decodes + ingests it. The farm is the canonical xxh3 hasher,
// so `media.hash_xxh3 == source_hash` holds and the hash gate passes directly
// (unlike OL-1's on-device push, which uses the size/mtime gate). ---

type LoupeJSON struct {
	LoupeVersion  int        `json:"loupe_version"`
	SchemaVersion string     `json:"schema_version"`
	IndexedAt     string     `json:"indexed_at"` // ISO8601 / RFC3339
	Media         LoupeMedia `json:"media"`
	AI            *LoupeAI   `json:"ai,omitempty"`
}

// LoupeMedia is the FULL tech block. Per AI_DELIVERY_SPEC Rule B, OpenLoupe's
// applyLoupeJSON refreshes the asset's tech columns from this and NULL-binds any
// missing field — so every field the farm knows is emitted (no omitempty);
// genuinely-absent color_space/log_format are explicit null.
type LoupeMedia struct {
	Filename   string  `json:"filename"`
	Type       string  `json:"type"`
	Codec      string  `json:"codec"`
	DurationMs int     `json:"duration_ms"`
	Width      int     `json:"width"`
	Height     int     `json:"height"`
	FPS        float64 `json:"fps"`
	ColorSpace *string `json:"color_space"`
	LogFormat  *string `json:"log_format"`
	HashXXH3   string  `json:"hash_xxh3"`
}

// LoupeAI — every sub-kind is emitted explicitly (null when absent) so each
// delivery is a complete, self-consistent Path-A snapshot matching the spec
// fixture; all fields are optional Swift-side, so null decodes to nil safely.
type LoupeAI struct {
	ImageEmbeddingGlobal *string           `json:"image_embedding_global"`
	SceneEmbeddings      []LoupeScene      `json:"scene_embeddings"`
	Faces                []LoupeFace       `json:"faces"`
	Transcript           *LoupeTranscript  `json:"transcript"`
	AIProviderSummary    map[string]string `json:"ai_provider_summary"`
}

type LoupeScene struct {
	TMs       int      `json:"t_ms"`
	Embedding string   `json:"embedding"`
	AutoTags  []string `json:"auto_tags,omitempty"`
}

type LoupeFace struct {
	ClusterID  string    `json:"cluster_id"`
	TMs        int       `json:"t_ms"`
	Bbox       []float64 `json:"bbox"`
	Confidence float64   `json:"confidence"`
}

type LoupeTranscript struct {
	Language string               `json:"language"`
	Model    string               `json:"model"`
	Segments []LoupeTranscriptSeg `json:"segments"`
}

type LoupeTranscriptSeg struct {
	StartMs int    `json:"start_ms"`
	EndMs   int    `json:"end_ms"`
	Text    string `json:"text"`
}

// whisperJSON is whisper.cpp's `-oj` output (offsets in ms).
type whisperJSON struct {
	Result struct {
		Language string `json:"language"`
	} `json:"result"`
	Transcription []struct {
		Offsets struct {
			From int `json:"from"`
			To   int `json:"to"`
		} `json:"offsets"`
		Text string `json:"text"`
	} `json:"transcription"`
}

// Transcribe extracts mono-16kHz PCM and runs whisper.cpp to produce a transcript
// in the LoupeJSON shape. PORTABLE: the identical whisper.cpp binary + ggml model
// run on the macOS node now and the Linux farm later, so transcripts are stable
// across lanes (no Apple framework). Returns (nil, nil) when the clip has no
// audio or no speech.
func Transcribe(ffmpegBin, whisperBin, modelPath, srcPath string) (*LoupeTranscript, error) {
	if ffmpegBin == "" {
		ffmpegBin = "ffmpeg"
	}
	if whisperBin == "" {
		whisperBin = "whisper-cli"
	}
	if modelPath == "" {
		return nil, fmt.Errorf("transcribe: whisper model path required")
	}
	tmp, err := os.MkdirTemp("", "jmfarm-asr-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmp)
	wav := filepath.Join(tmp, "a.wav")

	// Fold ALL audio streams + channels down to 16kHz mono s16le (whisper.cpp's
	// required input). Folding every stream — not just 0:a:0 — is the silent-data-
	// loss fix (audioFoldArgs): on multi-mono camera clips the first stream is
	// often the dead mic, so a:0-only transcribed pure silence.
	foldArgs, hasAudio, ferr := audioFoldArgs(ffmpegBin, srcPath, 16000)
	if ferr != nil {
		return nil, fmt.Errorf("transcribe: probe audio %q: %w", srcPath, ferr)
	}
	if !hasAudio {
		return nil, nil // no audio ⇒ no transcript, not an error
	}
	extArgs := append([]string{"-v", "error", "-y", "-i", srcPath}, foldArgs...)
	extArgs = append(extArgs, "-c:a", "pcm_s16le", wav)
	ext := exec.Command(ffmpegBin, extArgs...)
	if out, err := ext.CombinedOutput(); err != nil {
		// Belt-and-suspenders: a stream-map miss ⇒ no audio, not an error.
		if strings.Contains(string(out), "Stream map") || strings.Contains(string(out), "matches no streams") {
			return nil, nil
		}
		return nil, fmt.Errorf("transcribe: extract audio %q: %w: %s", srcPath, err, out)
	}

	prefix := filepath.Join(tmp, "out")
	asr := exec.Command(whisperBin, "-m", modelPath, "-f", wav, "-oj", "-of", prefix, "-l", "auto", "-np")
	if out, err := asr.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("transcribe: whisper %q: %w: %s", srcPath, err, out)
	}
	raw, err := os.ReadFile(prefix + ".json")
	if err != nil {
		return nil, fmt.Errorf("transcribe: read whisper json: %w", err)
	}
	var wj whisperJSON
	if err := json.Unmarshal(raw, &wj); err != nil {
		return nil, fmt.Errorf("transcribe: parse whisper json: %w", err)
	}

	tr := &LoupeTranscript{Language: wj.Result.Language, Model: whisperModelID(modelPath)}
	for _, s := range wj.Transcription {
		text := strings.TrimSpace(s.Text)
		if text == "" || isNonSpeechMarker(text) {
			continue
		}
		tr.Segments = append(tr.Segments, LoupeTranscriptSeg{StartMs: s.Offsets.From, EndMs: s.Offsets.To, Text: text})
	}
	if len(tr.Segments) == 0 {
		return nil, nil // silence / non-speech only
	}
	return tr, nil
}

// whisperModelID turns a ggml model path into the contract's provider/model id
// (AI_DELIVERY_SPEC §2.1), e.g. "/models/ggml-base.en.bin" → "whisper.cpp/base.en".
// The id is the invalidation key OpenLoupe stamps + shows for provenance.
func whisperModelID(modelPath string) string {
	b := filepath.Base(modelPath)
	b = strings.TrimSuffix(b, ".bin")
	b = strings.TrimPrefix(b, "ggml-")
	if b == "" {
		b = "unknown"
	}
	return "whisper.cpp/" + b
}

func isNonSpeechMarker(t string) bool {
	t = strings.ToLower(strings.Trim(t, "[]() "))
	switch t {
	case "blank_audio", "silence", "music", "no speech", "inaudible", "applause":
		return true
	}
	return false
}

// AIResult is the per-file outcome of an AI-generation pass.
type AIResult struct {
	Path      string
	Inode     uint64
	Hash      string
	HasSpeech bool
	Segments  int
	Err       error
}

// GenerateTranscript transcribes one file and merges the result into its
// `ai.loupe.json` (read-merge-write so a later embedding/face pass augments the
// same blob, never clobbers it), then registers an `ai` manifest row. The blob is
// written through opt.Mount; the manifest row is hash-gated (hash == source_hash).
func GenerateTranscript(store *derivatives.Store, path string, opt Options) AIResult {
	res := AIResult{Path: path}
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
	res.Inode = inode

	hash, err := SampleHash(path, fi.Size())
	if err != nil {
		res.Err = fmt.Errorf("hash: %w", err)
		return res
	}
	res.Hash = hash

	tech, err := Probe(opt.FFprobeBin, path, fi.Size())
	if err != nil {
		res.Err = err
		return res
	}

	tr, err := Transcribe(opt.FFmpegBin, opt.WhisperBin, opt.WhisperModel, path)
	if err != nil {
		res.Err = fmt.Errorf("transcribe: %w", err)
		return res
	}
	if tr == nil {
		return res // no speech ⇒ nothing to publish (HasSpeech stays false)
	}
	res.HasSpeech = true
	res.Segments = len(tr.Segments)

	// Path A (AI_DELIVERY_SPEC Rule A): the consumer's apply is a destructive
	// replace, so we always re-write the COMPLETE bundle — preserve any prior
	// sub-kinds (embeddings/faces) and set the transcript into the same bundle.
	rel := "ai.loupe.json"
	blobPath := filepath.Join(DerivBlobDir(opt.Mount, inode), rel)
	ai := loadExistingAI(blobPath)
	ai.Transcript = tr
	if ai.AIProviderSummary == nil {
		ai.AIProviderSummary = map[string]string{}
	}
	ai.AIProviderSummary["transcript"] = tr.Model
	doc := &LoupeJSON{
		LoupeVersion:  1,
		SchemaVersion: "1.0",
		IndexedAt:     time.Now().UTC().Format(time.RFC3339),
		Media:         buildMedia(path, tech, hash), // full block every time (Rule B)
		AI:            ai,
	}

	if err := writeLoupe(blobPath, doc); err != nil {
		res.Err = fmt.Errorf("write ai.loupe.json: %w", err)
		return res
	}

	model := tr.Model
	mt := "application/json"
	row := derivatives.DerivRow{
		Kind: "ai", Status: "ready", Producer: opt.Producer, Version: 1,
		Hash: &hash, BlobRelPath: &rel, MediaType: &mt, Model: &model,
	}
	stampSource(&row, fi)
	if err := store.PutSource(inode, &hash); err != nil {
		res.Err = fmt.Errorf("put source: %w", err)
		return res
	}
	if err := store.PutDeriv(inode, row); err != nil {
		res.Err = fmt.Errorf("put ai deriv: %w", err)
		return res
	}
	if opt.Mount != "" {
		_ = WriteManifestSidecar(store, opt.Mount, inode) // JM-15: best-effort
	}
	return res
}

// loadExistingAI returns the asset's current AI sub-bundle (embeddings/faces from
// a prior pass) so the new sub-kind merges INTO it — Path A: we always re-write
// the COMPLETE bundle so the consumer's destructive-replace never drops a kind.
func loadExistingAI(blobPath string) *LoupeAI {
	if raw, err := os.ReadFile(blobPath); err == nil {
		var d LoupeJSON
		if json.Unmarshal(raw, &d) == nil && d.AI != nil {
			return d.AI
		}
	}
	return &LoupeAI{}
}

// buildMedia assembles the FULL media block from tech (Rule B). color_space ←
// ffprobe color_space (the matrix coefficients); log_format/color_space are
// emitted as explicit null when absent.
func buildMedia(srcPath string, tech *Tech, hash string) LoupeMedia {
	m := LoupeMedia{
		Filename:   filepath.Base(srcPath),
		Type:       "video",
		DurationMs: int(tech.DurationMS),
		HashXXH3:   hash,
	}
	if tech.Video != nil {
		m.Codec = tech.Video.Codec
		m.Width = tech.Video.Width
		m.Height = tech.Video.Height
		m.FPS = tech.Video.FPS
		m.ColorSpace = tech.Video.Matrix
		m.LogFormat = tech.Video.LogFormat
	} else if len(tech.Audio) > 0 {
		m.Type = "audio"
		m.Codec = tech.Audio[0].Codec
	}
	return m
}

func writeLoupe(blobPath string, doc *LoupeJSON) error {
	if err := os.MkdirAll(filepath.Dir(blobPath), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(blobPath, b, 0o644)
}
