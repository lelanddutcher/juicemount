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

type LoupeMedia struct {
	Filename   string  `json:"filename"`
	Type       string  `json:"type"`
	Codec      string  `json:"codec,omitempty"`
	DurationMs int     `json:"duration_ms,omitempty"`
	Width      int     `json:"width,omitempty"`
	Height     int     `json:"height,omitempty"`
	FPS        float64 `json:"fps,omitempty"`
	ColorSpace string  `json:"color_space,omitempty"`
	LogFormat  string  `json:"log_format,omitempty"`
	HashXXH3   string  `json:"hash_xxh3,omitempty"`
}

type LoupeAI struct {
	ImageEmbeddingGlobal *string           `json:"image_embedding_global,omitempty"`
	SceneEmbeddings      []LoupeScene      `json:"scene_embeddings,omitempty"`
	Faces                []LoupeFace       `json:"faces,omitempty"`
	Transcript           *LoupeTranscript  `json:"transcript,omitempty"`
	AIProviderSummary    map[string]string `json:"ai_provider_summary,omitempty"`
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

	// 16kHz mono s16le — whisper.cpp's required input.
	ext := exec.Command(ffmpegBin, "-v", "error", "-y", "-i", srcPath,
		"-map", "0:a:0", "-ar", "16000", "-ac", "1", "-c:a", "pcm_s16le", wav)
	if out, err := ext.CombinedOutput(); err != nil {
		// No audio track ⇒ no transcript, not an error.
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

	tr := &LoupeTranscript{Language: wj.Result.Language, Model: filepath.Base(modelPath)}
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

	rel := "ai.loupe.json"
	blobPath := filepath.Join(DerivBlobDir(opt.Mount, inode), rel)
	doc := loadOrNewLoupe(blobPath, path, tech, hash)
	if doc.AI == nil {
		doc.AI = &LoupeAI{}
	}
	doc.AI.Transcript = tr
	if doc.AI.AIProviderSummary == nil {
		doc.AI.AIProviderSummary = map[string]string{}
	}
	doc.AI.AIProviderSummary["transcript"] = opt.Producer + ":whisper.cpp/" + filepath.Base(opt.WhisperModel)
	doc.IndexedAt = time.Now().UTC().Format(time.RFC3339)
	doc.Media.HashXXH3 = hash // keep the trust anchor pinned to the canonical xxh3

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
	if err := store.PutSource(inode, &hash); err != nil {
		res.Err = fmt.Errorf("put source: %w", err)
		return res
	}
	if err := store.PutDeriv(inode, row); err != nil {
		res.Err = fmt.Errorf("put ai deriv: %w", err)
		return res
	}
	return res
}

// loadOrNewLoupe reads an existing ai.loupe.json (to merge into) or builds a fresh
// doc with the media block derived from tech.
func loadOrNewLoupe(blobPath, srcPath string, tech *Tech, hash string) *LoupeJSON {
	if raw, err := os.ReadFile(blobPath); err == nil {
		var d LoupeJSON
		if json.Unmarshal(raw, &d) == nil && d.Media.Filename != "" {
			return &d
		}
	}
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
		if tech.Video.ColorPrimaries != nil {
			m.ColorSpace = *tech.Video.ColorPrimaries
		}
		if tech.Video.LogFormat != nil {
			m.LogFormat = *tech.Video.LogFormat
		}
	} else {
		m.Type = "audio"
	}
	return &LoupeJSON{LoupeVersion: 1, SchemaVersion: "1.0", Media: m}
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
