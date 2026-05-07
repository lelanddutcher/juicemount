// Package jmlog provides structured JSON logging for JuiceMount built on
// the standard library's log/slog. It exposes package-level convenience
// functions and a small Init() for one-time configuration from main().
//
// All output is JSON, written to stderr by default. An optional log file
// can be configured via Init() — the same record is fanned out to both
// destinations.
package jmlog

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

// Rotation policy. Kept conservative so even a chatty debug build can't
// fill a laptop disk: 16 MB per file × 5 generations = 80 MB max.
const (
	rotateMaxBytes  = 16 << 20 // 16 MiB
	rotateMaxBackup = 5
)

// Level mirrors slog.Level for callers that don't want to import slog.
type Level = slog.Level

const (
	LevelDebug = slog.LevelDebug
	LevelInfo  = slog.LevelInfo
	LevelWarn  = slog.LevelWarn
	LevelError = slog.LevelError
)

var (
	mu      sync.RWMutex
	logger  *slog.Logger
	logFile *os.File
	leveler *slog.LevelVar
)

// rotatingFile wraps an *os.File and rotates by size on Write().
// The wrapper is safe under the per-handler mu in slog (slog serializes
// writes to a single io.Writer), but we still gate with rotating.CAS
// because rotation itself calls Write paths internally.
//
// rotating is per-instance so that re-Init() (which constructs a new
// rotatingFile) doesn't share state with any dangling old instance.
type rotatingFile struct {
	mu       sync.Mutex
	path     string
	f        *os.File
	written  int64
	rotating atomic.Bool
}

func newRotatingFile(path string) (*rotatingFile, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	st, _ := f.Stat()
	rf := &rotatingFile{path: path, f: f}
	if st != nil {
		rf.written = st.Size()
	}
	return rf, nil
}

func (rf *rotatingFile) Write(p []byte) (int, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.written+int64(len(p)) > rotateMaxBytes && !rf.rotating.Load() {
		_ = rf.rotateLocked()
	}
	n, err := rf.f.Write(p)
	rf.written += int64(n)
	return n, err
}

// rotateLocked rolls juicemount.log → .1, .1 → .2, ... up to rotateMaxBackup.
// Caller must hold rf.mu.
func (rf *rotatingFile) rotateLocked() error {
	if !rf.rotating.CompareAndSwap(false, true) {
		return nil
	}
	defer rf.rotating.Store(false)

	if rf.f != nil {
		_ = rf.f.Close()
		rf.f = nil
	}
	for i := rotateMaxBackup; i >= 1; i-- {
		from := fmt.Sprintf("%s.%d", rf.path, i-1)
		if i-1 == 0 {
			from = rf.path
		}
		to := fmt.Sprintf("%s.%d", rf.path, i)
		if _, err := os.Stat(from); err == nil {
			_ = os.Rename(from, to) // best effort
		}
	}
	f, err := os.OpenFile(rf.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	rf.f = f
	rf.written = 0
	return nil
}

func (rf *rotatingFile) Close() error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.f != nil {
		err := rf.f.Close()
		rf.f = nil
		return err
	}
	return nil
}

// rotatedSink is what we keep in the package-level state when LogFile is set.
// We hold the typed value so Close() can release it cleanly on re-Init.
var rotatedSink *rotatingFile

func init() {
	// Default logger: JSON to stderr at Info.
	leveler = &slog.LevelVar{}
	leveler.Set(slog.LevelInfo)
	logger = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: leveler}))
}

// Config controls how Init() builds the global logger.
type Config struct {
	// LogFile is an optional path. When set, log records are written to
	// the file in addition to stderr. Pass "" to disable file output.
	LogFile string

	// Level controls the minimum severity. Defaults to Info if zero.
	Level Level

	// Stderr lets callers redirect the stderr sink (mainly for tests).
	// When nil, os.Stderr is used.
	Stderr io.Writer
}

// Init configures the global logger. Safe to call multiple times; the
// previous file handle (if any) is closed before a new one is opened.
//
// When cfg.LogFile is set, output is fanned to stderr AND a size-rotated
// file (16 MB × 5 generations). The directory is auto-created.
func Init(cfg Config) error {
	mu.Lock()
	defer mu.Unlock()

	// Tear down any previous file sink so re-Init is safe.
	if rotatedSink != nil {
		_ = rotatedSink.Close()
		rotatedSink = nil
	}
	logFile = nil // legacy field — kept nil; the rotated sink owns the fd

	if leveler == nil {
		leveler = &slog.LevelVar{}
	}
	leveler.Set(cfg.Level)

	stderr := cfg.Stderr
	if stderr == nil {
		stderr = os.Stderr
	}

	var sink io.Writer = stderr
	if cfg.LogFile != "" {
		rf, err := newRotatingFile(cfg.LogFile)
		if err != nil {
			return err
		}
		rotatedSink = rf
		sink = io.MultiWriter(stderr, rf)
	}

	logger = slog.New(slog.NewJSONHandler(sink, &slog.HandlerOptions{Level: leveler}))
	return nil
}

// SetLevel updates the active level on the live logger.
func SetLevel(l Level) {
	mu.RLock()
	defer mu.RUnlock()
	if leveler != nil {
		leveler.Set(l)
	}
}

// ParseLevel maps a case-insensitive string ("debug", "info", "warn",
// "error") to a Level. Unknown inputs yield LevelInfo.
func ParseLevel(s string) Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return LevelDebug
	case "warn", "warning":
		return LevelWarn
	case "error", "err":
		return LevelError
	default:
		return LevelInfo
	}
}

// Logger returns the underlying *slog.Logger for advanced use (e.g.
// .With() chains for component-scoped loggers).
func Logger() *slog.Logger {
	mu.RLock()
	defer mu.RUnlock()
	return logger
}

// With returns a derived logger that always includes the given attrs.
func With(attrs ...any) *slog.Logger {
	return Logger().With(attrs...)
}

// Close releases the log file (if any). Call from main() during shutdown.
func Close() {
	mu.Lock()
	defer mu.Unlock()
	if rotatedSink != nil {
		_ = rotatedSink.Close()
		rotatedSink = nil
	}
	logFile = nil
}

// Debug logs at DEBUG level.
func Debug(msg string, attrs ...any) {
	Logger().LogAttrs(context.Background(), slog.LevelDebug, msg, toAttrs(attrs)...)
}

// Info logs at INFO level.
func Info(msg string, attrs ...any) {
	Logger().LogAttrs(context.Background(), slog.LevelInfo, msg, toAttrs(attrs)...)
}

// Warn logs at WARN level.
func Warn(msg string, attrs ...any) {
	Logger().LogAttrs(context.Background(), slog.LevelWarn, msg, toAttrs(attrs)...)
}

// Error logs at ERROR level.
func Error(msg string, attrs ...any) {
	Logger().LogAttrs(context.Background(), slog.LevelError, msg, toAttrs(attrs)...)
}

// toAttrs converts a key/value variadic list (and bare slog.Attr values)
// into []slog.Attr. Unpaired keys are tolerated by attaching a "MISSING"
// sentinel rather than panicking — operational logs should never crash
// the process.
func toAttrs(args []any) []slog.Attr {
	if len(args) == 0 {
		return nil
	}
	out := make([]slog.Attr, 0, len(args)/2+1)
	for i := 0; i < len(args); {
		switch v := args[i].(type) {
		case slog.Attr:
			out = append(out, v)
			i++
		case string:
			if i+1 < len(args) {
				out = append(out, slog.Any(v, args[i+1]))
				i += 2
			} else {
				out = append(out, slog.String(v, "MISSING"))
				i++
			}
		default:
			out = append(out, slog.Any("!BADKEY", v))
			i++
		}
	}
	return out
}
