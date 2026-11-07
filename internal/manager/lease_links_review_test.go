//go:build linux

package manager

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Regression tests from the security review of the project fence (patch
// 0017). They run the real juicefs (JM_TEST_JUICEFS_BIN); the standalone
// ones also need JM_TEST_JFS_META.

// standaloneVolume formats a throwaway jfs:// volume, seeds it from the
// files seed writes under its argument (with --links), and returns the
// layout, the RunSync spec and a function that copies the volume's /data
// back to a host folder.
func standaloneVolume(t *testing.T, vol string, seed func(root string)) (syncLayout, RunSyncSpec, func() string) {
	t.Helper()
	bin := realJuicefsBin(t)
	meta := os.Getenv("JM_TEST_JFS_META")
	if meta == "" {
		t.Skip("JM_TEST_JFS_META not set; point it at a throwaway Redis database to run this test")
	}
	l := newSyncLayout(t)
	env := append(os.Environ(), vol+"="+meta)
	jfs := func(args ...string) {
		t.Helper()
		cmd := exec.Command(bin, args...)
		cmd.Env = env
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("juicefs %s: %v\n%s", args[0], err, out)
		}
	}
	flushRedisDB(t, meta)
	jfs("format", "--storage", "file", "--bucket", filepath.Join(l.base, "bucket")+"/", meta, vol)
	root := filepath.Join(l.base, "seed")
	seed(root)
	jfs("sync", "--links", "file://"+root+"/", "jfs://"+vol+"/")
	spec := RunSyncSpec{Mode: ModeStandalone, MetaURL: meta, VolName: vol, SourceRoots: []string{l.host}}
	n := 0
	readBack := func() string {
		t.Helper()
		n++
		back := filepath.Join(l.base, "back"+string(rune('0'+n)))
		jfs("sync", "--links", "jfs://"+vol+"/data/", "file://"+back+"/")
		return back
	}
	return l, spec, readBack
}

// Standalone: juicefs resolves a link's target lexically (the link's folder
// by name, joined and cleaned), so ".." in a target, or after an earlier
// link, must be judged the same way. Only projA is leased; nothing may land
// in projB.
func TestLeaseStandaloneLexicalDotDot(t *testing.T) {
	bin := realJuicefsBin(t)
	l, spec, readBack := standaloneVolume(t, "jmlexical", func(seed string) {
		mustWrite(t, filepath.Join(seed, "data/projA/keep.txt"), "a")
		mustWrite(t, filepath.Join(seed, "data/projB/file.txt"), "b")
		mustWrite(t, filepath.Join(seed, "data/projA/s/t/k.txt"), "k")
		// A missing first component of the target.
		mustLink(t, "nope/../../projB", filepath.Join(seed, "data/projA/x"))
		// ".." after a link to a deeper folder.
		mustLink(t, "s/t", filepath.Join(seed, "data/projA/y"))
		mustLink(t, "y/../../projB", filepath.Join(seed, "data/projA/z"))
		// A link reached through another link: juicefs joins its target to
		// the folder by name (projA/y), not to where y leads (projA/s/t).
		mustLink(t, "../../projB", filepath.Join(seed, "data/projA/s/t/w"))
		// Same-project ".." that stays inside: still followed.
		mustLink(t, "s/../s/t", filepath.Join(seed, "data/projA/q"))
	})
	cases := []struct{ name, rel, dst string }{
		{"missing component, above project", "projA/x/p1.txt", "/jfs/data"},
		{"missing component, into project", "x/p2.txt", "/jfs/data/projA"},
		{"link chain, above project", "projA/z/p3.txt", "/jfs/data"},
		{"link chain, into project", "z/p4.txt", "/jfs/data/projA"},
		{"link by name through a link", "y/w/p5.txt", "/jfs/data/projA"},
	}
	for i, c := range cases {
		src := filepath.Join(l.host, "in", string(rune('a'+i)))
		mustWrite(t, filepath.Join(src, c.rel), "payload")
		fl, _, err := l.runLeased(t, bin, spec, src, c.dst)
		onlyLeased(t, fl)
		if err == nil {
			t.Errorf("%s: the sync reported success", c.name)
		}
	}
	src := filepath.Join(l.host, "in", "ok")
	mustWrite(t, filepath.Join(src, "q/same.txt"), "same")
	if _, _, err := l.runLeased(t, bin, spec, src, "/jfs/data/projA"); err != nil {
		t.Errorf("write through a same-project link with ..: %v", err)
	}
	back := readBack()
	projBUntouched(t, "standalone lexical", filepath.Join(back, "projB"))
	if b, err := os.ReadFile(filepath.Join(back, "projA/s/t/same.txt")); err != nil || string(b) != "same" {
		t.Errorf("same-project write: %q %v", b, err)
	}
}

// Embedded: the same links resolved by the kernel beneath the project
// folder. Nothing lands in projB.
func TestLeaseEmbeddedLexicalDotDot(t *testing.T) {
	bin := realJuicefsBin(t)
	l, a, b := leaseLayout(t)
	mustWrite(t, filepath.Join(a, "s/t/k.txt"), "k")
	mustMkdir(t, filepath.Join(a, "nope"))
	mustLink(t, "nope/../../projB", filepath.Join(a, "x"))
	mustLink(t, "s/t", filepath.Join(a, "y"))
	mustLink(t, "y/../../projB", filepath.Join(a, "z"))
	for i, c := range []struct{ rel, dst string }{
		{"projA/x/e1.txt", "/jfs/data"}, {"x/e2.txt", "/jfs/data/projA"},
		{"projA/z/e3.txt", "/jfs/data"}, {"z/e4.txt", "/jfs/data/projA"},
	} {
		src := filepath.Join(l.host, "in", string(rune('a'+i)))
		mustWrite(t, filepath.Join(src, c.rel), "payload")
		if _, _, err := l.runLeased(t, bin, l.spec(), src, c.dst); err == nil {
			t.Errorf("%s -> %s: the sync reported success", c.rel, c.dst)
		}
	}
	projBUntouched(t, "embedded lexical", b)
}

// Copied links: y -> . and x -> y/../projB each look inside projA by name,
// but together resolve into projB. x is skipped; y (harmless) is stored.
func TestLeaseCopiedLinkChainIsSkipped(t *testing.T) {
	bin := realJuicefsBin(t)
	l, a, b := leaseLayout(t)
	src := filepath.Join(l.host, "in")
	mustMkdir(t, src)
	mustLink(t, ".", filepath.Join(src, "y"))
	mustLink(t, "y/../projB", filepath.Join(src, "x"))
	_, report, err := l.runLeased(t, bin, l.spec(), src, "/jfs/data/projA")
	if err != nil {
		t.Fatalf("RunSync: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(a, "x")); err == nil {
		t.Errorf("link projA/x (y/../projB) was stored; projA: %v", tree(t, a))
	}
	if got, err := os.Readlink(filepath.Join(a, "y")); err != nil || got != "." {
		t.Errorf("same-project link y: %q %v", got, err)
	}
	if report.SkippedLinks != 1 {
		t.Errorf("skipped %d links %v, want 1", report.SkippedLinks, report.SkippedLinkSample)
	}
	projBUntouched(t, "copied chain", b)
}

// Standalone: a destination named through a same-project link
// (projA/inner -> sub) is used by its real path, so copied links work.
func TestLeaseStandaloneLinkIntoAliasedDestination(t *testing.T) {
	bin := realJuicefsBin(t)
	l, spec, readBack := standaloneVolume(t, "jmalias", func(seed string) {
		mustWrite(t, filepath.Join(seed, "data/projA/sub/s.txt"), "s")
		mustLink(t, "sub", filepath.Join(seed, "data/projA/inner"))
	})
	src := filepath.Join(l.host, "in")
	mustWrite(t, filepath.Join(src, "f.txt"), "f")
	mustLink(t, "f.txt", filepath.Join(src, "rel"))
	_, report, err := l.runLeased(t, bin, spec, src, "/jfs/data/projA/inner")
	if err != nil {
		t.Fatalf("RunSync into a same-project link: %v", err)
	}
	if report.SkippedLinks != 0 {
		t.Errorf("skipped %d links %v, want none", report.SkippedLinks, report.SkippedLinkSample)
	}
	back := readBack()
	if got, err := os.Readlink(filepath.Join(back, "projA/sub/rel")); err != nil || got != "f.txt" {
		t.Errorf("copied link: %q %v", got, err)
	}
	if b, err := os.ReadFile(filepath.Join(back, "projA/sub/f.txt")); err != nil || string(b) != "f" {
		t.Errorf("copied file: %q %v", b, err)
	}
}

// A juicefs without the project fence (-jm2, -jm1 or stock) never syncs
// into the volume; it is not even started for the copy.
func TestRunSyncIntoVolumeNeedsFencedJuicefs(t *testing.T) {
	l := newSyncLayout(t)
	src := filepath.Join(l.host, "in")
	mustWrite(t, filepath.Join(src, "a.txt"), "a")
	for _, v := range []string{"juicefs version 1.3.1+2025-12-02.e0032b2ae5e9-jm2", jm1Version, "juicefs version 1.3.1+2025-12-02.e0032b2ae5e9"} {
		bin, argsFile, _ := versionedJuicefs(t, v)
		err := l.run(t, bin, src, "/jfs/data/projA", DefaultSyncOptions())
		if err == nil || !strings.Contains(err.Error(), "sync into the volume refused") {
			t.Errorf("%s: RunSync into the volume: %v, want a refusal", v, err)
		}
		if _, serr := os.Stat(argsFile); serr == nil {
			t.Errorf("%s: juicefs sync was started", v)
		}
	}
}
