package unmountd

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// forbiddenUnmountShapes are every path-taking way to unmount, plus process
// execution (which is how `umount` / `diskutil` would be reached). The helper
// may only unmount through vfs.generic.ctlbyfsid. Delete the unsafe shape;
// do not guard call sites.
var forbiddenUnmountShapes = []*regexp.Regexp{
	regexp.MustCompile(`\bunix\.Unmount\b`),
	regexp.MustCompile(`\bsyscall\.Unmount\b`),
	regexp.MustCompile(`\bunix\.Funmount\b`),
	regexp.MustCompile(`\bunmount\s*\(`),  // unmount(2) by path, any language
	regexp.MustCompile(`\bfunmount\s*\(`), // descriptor tier belongs to the custodians
	regexp.MustCompile(`\bunmountat\s*\(`),
	regexp.MustCompile(`\bumount\b`), // umount(8), "sudo umount -f", /sbin/umount
	regexp.MustCompile(`\bdiskutil\b`),
	regexp.MustCompile(`\bhdiutil\b`),
	regexp.MustCompile(`"os/exec"`),
	regexp.MustCompile(`\bexec\.Command`),
	regexp.MustCompile(`\b(syscall|unix)\.(Exec|ForkExec|StartProcess)\b`),
	regexp.MustCompile(`\bos\.StartProcess\b`),
	regexp.MustCompile(`\b(posix_spawn|execve|execv|execvp|system|popen)\s*\(`),
	regexp.MustCompile(`\bNSWorkspace\b|\bunmountAndEjectDevice\b|\bDADiskUnmount\b`),
}

func TestNoPathTakingUnmountAPIs(t *testing.T) {
	_, self, _, _ := runtime.Caller(0)
	pkgDir := filepath.Dir(self)
	repo := filepath.Clean(filepath.Join(pkgDir, "..", ".."))
	roots := []string{pkgDir, filepath.Join(repo, "cmd", "juicemount-unmountd")}
	scanned := 0
	for _, root := range roots {
		if _, err := os.Stat(root); err != nil {
			t.Fatalf("gate root missing: %s: %v", root, err)
		}
		err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			if path == self {
				return nil // this file names the patterns
			}
			switch filepath.Ext(path) {
			case ".go", ".c", ".h", ".m", ".s", ".sh", ".plist", ".swift":
			default:
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			scanned++
			for i, line := range strings.Split(string(data), "\n") {
				for _, re := range forbiddenUnmountShapes {
					if re.MatchString(line) {
						t.Errorf("%s:%d: forbidden path-taking unmount shape %q: %s", path, i+1, re, strings.TrimSpace(line))
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	// Refuse to report a pass that scanned nothing.
	if scanned < 5 {
		t.Fatalf("grep gate scanned only %d files; the gate is not looking at the helper", scanned)
	}
}
