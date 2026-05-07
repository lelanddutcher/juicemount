// verify-smoke is a CLI smoke test for the verify package.
//
// Usage:
//   verify-smoke <target-dir-1> <target-dir-2> [<target-dir-3> ...]
//
// Walks each directory, hashes every file, and prints aggregate status +
// a per-file breakdown. Useful for proving the system end-to-end against
// real directories before the menu bar UI is wired up.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/lelanddutcher/juicemount/internal/verify"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: verify-smoke <target-dir-1> <target-dir-2> [<target-dir-3> ...]")
		os.Exit(2)
	}

	// Manifest in a temp file (each smoke run is independent)
	tmpManifest := filepath.Join(os.TempDir(), fmt.Sprintf("verify-smoke-%d.json", os.Getpid()))
	defer os.Remove(tmpManifest)
	m, err := verify.NewManifest(tmpManifest)
	if err != nil {
		fmt.Fprintf(os.Stderr, "manifest: %v\n", err)
		os.Exit(1)
	}

	// Build targets from CLI args
	var targets []verify.Target
	for _, dir := range os.Args[1:] {
		t, err := verify.NewLocalTarget(dir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "target %s: %v\n", dir, err)
			os.Exit(1)
		}
		t.SkipFunc(verify.DefaultMacSkipFunc)
		targets = append(targets, t)
	}

	mgr := verify.NewManager(m, targets...)

	fmt.Printf("=== Verify-smoke ===\n")
	fmt.Printf("Targets: %d\n", len(targets))
	for _, t := range targets {
		fmt.Printf("  %s (available=%v)\n", t.Identifier(), t.Available(context.Background()))
	}
	fmt.Println()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	start := time.Now()
	results, err := mgr.VerifyAll(ctx)
	elapsed := time.Since(start)

	if err != nil {
		fmt.Fprintf(os.Stderr, "verify error: %v\n", err)
	}

	fmt.Printf("=== Per-target results (%v elapsed) ===\n", elapsed.Round(time.Millisecond))
	for _, t := range targets {
		s := results[t.Identifier()]
		fmt.Printf("  %s\n", t.Identifier())
		fmt.Printf("    seen=%d hashed=%d failed=%d available=%v duration=%v\n",
			s.FilesSeen, s.FilesHashed, s.FilesFailed, s.Available, s.Duration.Round(time.Millisecond))
	}
	fmt.Println()

	stats := mgr.Stats()
	fmt.Printf("=== Aggregate manifest stats ===\n")
	fmt.Printf("  Total files: %d\n", stats.TotalFiles)
	fmt.Printf("  🟢 Green (≥%d copies): %d\n", verify.MinGreenCopies, stats.GreenCount)
	fmt.Printf("  🟡 Yellow (1-%d copies): %d\n", verify.MinGreenCopies-1, stats.YellowCount)
	fmt.Printf("  🔴 Red (corruption or no copies): %d\n", stats.RedCount)
	if stats.UnknownCount > 0 {
		fmt.Printf("  ⚪ Unknown: %d\n", stats.UnknownCount)
	}
	fmt.Println()

	// Show the first few records and their status
	paths := mgr.AllPaths()
	preview := 10
	if len(paths) < preview {
		preview = len(paths)
	}
	fmt.Printf("=== Per-file detail (first %d) ===\n", preview)
	for _, p := range paths[:preview] {
		st := mgr.Status(p)
		fmt.Printf("  %s  %s\n", emojiOf(st), p)
	}

	// Demo SafeToDelete on the first file from the first target
	if len(paths) > 0 && len(targets) > 0 {
		p := paths[0]
		v := mgr.SafeToDelete(p, targets[0].Identifier())
		fmt.Printf("\n=== SafeToDelete check ===\n")
		fmt.Printf("  File:        %s\n", p)
		fmt.Printf("  Deleting from: %s\n", targets[0].Identifier())
		fmt.Printf("  Verdict:     safe=%v copies=%d\n", v.Safe, v.VerifiedCopies)
		fmt.Printf("  Explanation: %s\n", v.Explanation)
		if len(v.OtherTargets) > 0 {
			fmt.Printf("  Other targets: %v\n", v.OtherTargets)
		}
	}
}

func emojiOf(s verify.Status) string {
	switch s {
	case verify.StatusGreen:
		return "🟢"
	case verify.StatusYellow:
		return "🟡"
	case verify.StatusRed:
		return "🔴"
	default:
		return "⚪"
	}
}
