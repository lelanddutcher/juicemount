package perf

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestDistributionBuildNumberPreflight(t *testing.T) {
	bin := t.TempDir()
	git := filepath.Join(bin, "git")
	if err := os.WriteFile(git, []byte("#!/bin/sh\nexit 1\n"), 0755); err != nil {
		t.Fatal(err)
	}
	writeFakeCurl(t, bin)

	published13 := writeAppcast(t, bin, "published13.xml", `<item><sparkle:version>13</sparkle:version></item>`)
	published14 := writeAppcast(t, bin, "published14.xml",
		`<item><sparkle:version>9</sparkle:version></item><item><sparkle:version>14</sparkle:version></item>`)
	enclosure20 := writeAppcast(t, bin, "enclosure20.xml",
		`<item><enclosure url="https://example.invalid/a.zip" sparkle:version="20"/></item>`)
	nonInteger := writeAppcast(t, bin, "noninteger.xml", `<item><sparkle:version>14.1</sparkle:version></item>`)
	empty := writeAppcast(t, bin, "empty.xml", ``)

	const pastPreflight = "JM_COMMIT was supplied, but this source tree has no verifiable git HEAD"
	// live is the file the stubbed curl serves for SUFeedURL; "" = unreachable.
	for _, tc := range []struct {
		name, buildNumber, previous, live, want string
	}{
		{"missing", "", published13, "", "requires explicit JM_VERSION, JM_COMMIT, and JM_BUILD_NUMBER"},
		{"old", "13", published13, "", "JM_BUILD_NUMBER must be a positive decimal integer greater than 13"},
		{"nonnumeric", "1x", published13, "", "JM_BUILD_NUMBER must be a positive decimal integer greater than 13"},
		{"leading zero", "014", published13, "", "JM_BUILD_NUMBER must be a positive decimal integer greater than 13"},
		{"no local copy and live feed unreachable", "15", "", "", "could not fetch the published appcast from https://github.com/lelanddutcher/juicemount/releases/latest/download/appcast.xml"},
		{"no local copy reuses live build", "14", "", published14, "JM_BUILD_NUMBER (14) must exceed the highest published build (14)"},
		{"no local copy unreadable live feed", "15", "", nonInteger, "could not be read as a Sparkle appcast"},
		{"no local copy above live build", "15", "", published14, pastPreflight},
		{"local copy path missing", "15", filepath.Join(bin, "absent.xml"), "", "JM_PREVIOUS_APPCAST does not name a file"},
		{"stale local copy", "15", published13, enclosure20, "JM_PREVIOUS_APPCAST is stale: its highest build is 13 but"},
		{"local copy agrees with live feed", "15", published14, published14, pastPreflight},
		{"reuses published build", "14", published14, "", "JM_BUILD_NUMBER (14) must exceed the highest published build (14)"},
		{"below enclosure-attribute build", "19", enclosure20, "", "JM_BUILD_NUMBER (19) must exceed the highest published build (20)"},
		{"non-integer published build", "15", nonInteger, "", "could not be read as a Sparkle appcast"},
		{"empty published appcast", "15", empty, "", "could not be read as a Sparkle appcast"},
		{"first eligible, live unreachable", "14", published13, "", pastPreflight},
		{"above published", "15", published14, "", pastPreflight},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command("bash", "../../scripts/build-app.sh", "--distribution")
			cmd.Env = append(os.Environ(),
				"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
				"JM_VERSION=0.5.0",
				"JM_COMMIT="+strings.Repeat("a", 40),
				"JM_BUILD_NUMBER="+tc.buildNumber,
				"JM_PREVIOUS_APPCAST="+tc.previous,
				"FAKE_FEED="+tc.live,
			)
			out, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(out), tc.want) {
				t.Fatalf("expected preflight failure %q, got err=%v output=%s", tc.want, err, out)
			}
		})
	}
}

func TestDistributionBuildRejectsUntrackedSource(t *testing.T) {
	repo := t.TempDir()
	scriptDir := filepath.Join(repo, "scripts")
	if err := os.MkdirAll(scriptDir, 0755); err != nil {
		t.Fatal(err)
	}
	script, err := os.ReadFile("../../scripts/build-app.sh")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(scriptDir, "build-app.sh"), script, 0755); err != nil {
		t.Fatal(err)
	}
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	plistDir := filepath.Join(repo, "app", "JuiceMount", "Resources")
	if err := os.MkdirAll(plistDir, 0755); err != nil {
		t.Fatal(err)
	}
	plist, err := os.ReadFile("../../app/JuiceMount/Resources/Info.plist")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(plistDir, "Info.plist"), plist, 0644); err != nil {
		t.Fatal(err)
	}
	stubs := t.TempDir()
	writeFakeCurl(t, stubs)
	git("init", "-q")
	git("add", "scripts/build-app.sh", "app/JuiceMount/Resources/Info.plist")
	git("-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "base")
	if err := os.MkdirAll(filepath.Join(repo, "app", "JuiceMount", "Sources"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "app", "JuiceMount", "Sources", "Untracked.swift"), []byte("struct Untracked {}\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", filepath.Join(scriptDir, "build-app.sh"), "--distribution")
	cmd.Dir = repo
	cmd.Env = append(os.Environ(),
		"JM_VERSION=0.5.0",
		"JM_COMMIT="+git("rev-parse", "HEAD"),
		"JM_BUILD_NUMBER=14",
		"PATH="+stubs+string(os.PathListSeparator)+os.Getenv("PATH"),
		"FAKE_FEED=",
		"JM_PREVIOUS_APPCAST="+writeAppcast(t, t.TempDir(), "published.xml", `<item><sparkle:version>13</sparkle:version></item>`),
	)
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "release builds reject untracked files: app/JuiceMount/Sources/Untracked.swift") {
		t.Fatalf("expected untracked source rejection, got err=%v output=%s", err, out)
	}
}

// writeFakeCurl installs a curl stub that never touches the network: it copies
// $FAKE_FEED to the -o target, or fails like an unreachable host when unset.
func writeFakeCurl(t *testing.T, dir string) {
	t.Helper()
	stub := "#!/bin/sh\nout=\"\"\nwhile [ $# -gt 0 ]; do\n  case \"$1\" in -o) out=\"$2\"; shift ;; esac\n  shift\ndone\n" +
		"[ -n \"$FAKE_FEED\" ] || exit 6\ncp \"$FAKE_FEED\" \"$out\"\n"
	if err := os.WriteFile(filepath.Join(dir, "curl"), []byte(stub), 0755); err != nil {
		t.Fatal(err)
	}
}

func writeAppcast(t *testing.T, dir, name, items string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	body := `<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0" xmlns:sparkle="http://www.andymatuschak.org/xml-namespaces/sparkle"><channel>` + items + `</channel></rss>
`
	if err := os.WriteFile(path, []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

// H-6. A signed update feed is a shipping artifact. A non-distribution build
// must refuse to produce one before it builds anything. The script runs from
// an empty copy with failing go/swift stubs, so a neutered guard fails fast
// instead of building the real tree.
func TestDevBuildRefusesAppcast(t *testing.T) {
	repo := t.TempDir()
	scriptDir := filepath.Join(repo, "scripts")
	stubs := filepath.Join(repo, "stubs")
	for _, dir := range []string{scriptDir, stubs} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
	}
	script, err := os.ReadFile("../../scripts/build-app.sh")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(scriptDir, "build-app.sh"), script, 0755); err != nil {
		t.Fatal(err)
	}
	for _, tool := range []string{"go", "swift", "git"} {
		if err := os.WriteFile(filepath.Join(stubs, tool), []byte("#!/bin/sh\necho STUB-$0-RAN >&2\nexit 1\n"), 0755); err != nil {
			t.Fatal(err)
		}
	}
	for _, mode := range []string{"--release", "--debug"} {
		t.Run(mode, func(t *testing.T) {
			cmd := exec.Command("bash", filepath.Join(scriptDir, "build-app.sh"), mode)
			cmd.Dir = repo
			cmd.Env = append(os.Environ(),
				"PATH="+stubs+string(os.PathListSeparator)+os.Getenv("PATH"),
				"JM_APPCAST=1",
				"JM_BUILD_NUMBER=99",
			)
			out, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(out), "JM_APPCAST is only allowed with --distribution") {
				t.Fatalf("expected dev appcast refusal, got err=%v output=%s", err, out)
			}
			if strings.Contains(string(out), "STUB-") {
				t.Fatalf("refusal must precede any build step; output=%s", out)
			}
		})
	}
}

// H-6. verify-build.sh --release is the shipping gate. A development bundle can
// be Developer-ID signed, notarized, and numbered above 13, so the gate must
// also require the JMBuildChannel stamp that only --distribution writes, on
// both the app and its QuickLook extension.
func TestReleaseVerifierRequiresDistributionChannel(t *testing.T) {
	const plistBuddy = "/usr/libexec/PlistBuddy"
	if _, err := os.Stat(plistBuddy); err != nil {
		t.Skip("PlistBuddy unavailable")
	}
	commit := strings.Repeat("a", 40)
	makeBundle := func(t *testing.T, appChannel, appexChannel string) string {
		t.Helper()
		app := filepath.Join(t.TempDir(), "JuiceMount.app")
		macos := filepath.Join(app, "Contents", "MacOS")
		appex := filepath.Join(app, "Contents", "PlugIns", "JuiceMountThumbnails.appex", "Contents")
		for _, dir := range []string{macos, appex} {
			if err := os.MkdirAll(dir, 0755); err != nil {
				t.Fatal(err)
			}
		}
		for _, exe := range []string{"JuiceMount", "juicemount-service", "juicemount-mount-probe"} {
			if err := os.WriteFile(filepath.Join(macos, exe), []byte("#!/bin/sh\nexit 0\n"), 0755); err != nil {
				t.Fatal(err)
			}
		}
		stamp := func(plist, channel string) {
			cmds := []string{
				"Add :CFBundleShortVersionString string 0.5.0",
				"Add :CFBundleVersion string 14",
				"Add :JMBuildCommit string " + commit,
			}
			if channel != "" {
				cmds = append(cmds, "Add :JMBuildChannel string "+channel)
			}
			for _, c := range cmds {
				if out, err := exec.Command(plistBuddy, "-c", c, plist).CombinedOutput(); err != nil {
					t.Fatalf("PlistBuddy %q: %v: %s", c, err, out)
				}
			}
		}
		stamp(filepath.Join(app, "Contents", "Info.plist"), appChannel)
		stamp(filepath.Join(appex, "Info.plist"), appexChannel)
		return app
	}
	run := func(t *testing.T, app string, extra ...string) string {
		t.Helper()
		args := append([]string{"../../scripts/verify-build.sh", "--app", app, "--release",
			"--version", "0.5.0", "--build-number", "14", "--commit", commit}, extra...)
		out, _ := exec.Command("bash", args...).CombinedOutput()
		return string(out)
	}
	zipBundle := func(t *testing.T, app string) string {
		t.Helper()
		zip := filepath.Join(t.TempDir(), "JuiceMount.zip")
		if out, err := exec.Command("ditto", "-c", "-k", "--keepParent", app, zip).CombinedOutput(); err != nil {
			t.Fatalf("ditto: %v: %s", err, out)
		}
		return zip
	}

	for _, tc := range []struct {
		name, app, appex, want string
	}{
		{"development app", "development", "distribution", "bundle build channel: got 'development', expected 'distribution'"},
		{"unstamped app", "", "distribution", "bundle build channel: got '<missing>', expected 'distribution'"},
		{"development extension", "distribution", "development", "QuickLook extension build channel: got 'development', expected 'distribution'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if out := run(t, makeBundle(t, tc.app, tc.appex)); !strings.Contains(out, tc.want) {
				t.Fatalf("expected %q in verifier output:\n%s", tc.want, out)
			}
		})
	}
	// The update archive is what Sparkle installs, so a distribution-stamped
	// outer bundle must not vouch for a development archive.
	for _, tc := range []struct {
		name, app, appex, want string
	}{
		{"development archive app", "development", "distribution", "update archive build channel: got 'development', expected 'distribution'"},
		{"unstamped archive app", "", "distribution", "update archive build channel: got '<missing>', expected 'distribution'"},
		{"development archive extension", "distribution", "development", "update archive extension build channel: got 'development', expected 'distribution'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			archive := zipBundle(t, makeBundle(t, tc.app, tc.appex))
			if out := run(t, makeBundle(t, "distribution", "distribution"), "--archive", archive); !strings.Contains(out, tc.want) {
				t.Fatalf("expected %q in verifier output:\n%s", tc.want, out)
			}
		})
	}
	t.Run("distribution archive", func(t *testing.T) {
		archive := zipBundle(t, makeBundle(t, "distribution", "distribution"))
		out := run(t, makeBundle(t, "distribution", "distribution"), "--archive", archive)
		for _, want := range []string{"update archive build channel: distribution", "update archive extension build channel: distribution"} {
			if !strings.Contains(out, want) {
				t.Fatalf("expected %q in verifier output:\n%s", want, out)
			}
		}
	})
	t.Run("distribution", func(t *testing.T) {
		out := run(t, makeBundle(t, "distribution", "distribution"))
		for _, want := range []string{"bundle build channel: distribution", "QuickLook extension build channel: distribution"} {
			if !strings.Contains(out, want) {
				t.Fatalf("expected %q in verifier output:\n%s", want, out)
			}
		}
	})
}
