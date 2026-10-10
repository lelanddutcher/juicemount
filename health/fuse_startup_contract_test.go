package health

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
	"time"
)

func TestSpotlightMarkerCannotBlockMountReadiness(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	returned := make(chan (<-chan struct{}), 1)
	go func() {
		returned <- suppressSpotlightIndexing("/fuse", func(string, []byte, os.FileMode) error {
			close(entered)
			<-release
			return nil
		})
	}()

	var done <-chan struct{}
	select {
	case done = <-returned:
	case <-time.After(100 * time.Millisecond):
		close(release)
		t.Fatal("Spotlight marker scheduling blocked mount readiness")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("Spotlight marker write was not scheduled")
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Spotlight marker helper did not settle")
	}
}

func TestMountProvesStableDataPlaneBeforeSchedulingSpotlightWrite(t *testing.T) {
	source, fset, function := startupContractFunction(t, "fuse.go", "MountWithReceipt")
	var alias, fallback, gate, marker ast.Stmt
	for _, statement := range function.List {
		body := strings.TrimSpace(string(source[fset.Position(statement.Pos()).Offset:fset.Position(statement.End()).Offset]))
		switch {
		case body == "probe := fm.dataPlaneProbe":
			alias = statement
		case strings.HasPrefix(body, "if probe == nil {") && strings.Contains(body, "probe = WaitForFUSEDataPlaneStable"):
			fallback = statement
		case strings.HasPrefix(body, "if !probe(fm.cfg.MountPoint, fm.mountVerifyTimeout()) {") && strings.Contains(body, "return receipt,"):
			gate = statement
		case body == "fm.scheduleSpotlightIndexing()":
			marker = statement
		}
	}
	if alias == nil || fallback == nil || gate == nil || marker == nil ||
		!(alias.End() < fallback.Pos() && fallback.End() < gate.Pos() && gate.End() < marker.Pos()) {
		t.Fatal("MountWithReceipt must select the default stable probe, return on failure, then schedule Spotlight")
	}
	trackerSource, trackerSet, trackerBody := startupContractFunction(t, "fuse_custody.go", "scheduleSpotlightIndexing")
	tracker := string(trackerSource[trackerSet.Position(trackerBody.Pos()).Offset:trackerSet.Position(trackerBody.End()).Offset])
	if !strings.Contains(tracker, "done := suppressSpotlightIndexing(fm.cfg.MountPoint, write)") ||
		!strings.Contains(tracker, "<-done") || !strings.Contains(tracker, "fm.helperActive--") ||
		!strings.Contains(tracker, "close(fm.helperIdle)") {
		t.Fatal("Spotlight scheduling must retain and join its completion channel")
	}
	quiesceSource, quiesceSet, quiesceBody := startupContractFunction(t, "fuse_custody.go", "quiesceSpotlight")
	quiesce := string(quiesceSource[quiesceSet.Position(quiesceBody.Pos()).Offset:quiesceSet.Position(quiesceBody.End()).Offset])
	if !strings.Contains(quiesce, "waitFUSEChannel(idle, timeout)") {
		t.Fatal("Stop no longer waits for the retained Spotlight completion channel")
	}
}

func startupContractFunction(t *testing.T, path, name string) ([]byte, *token.FileSet, *ast.BlockStmt) {
	t.Helper()
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, source, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if ok && fn.Name.Name == name && fn.Body != nil {
			return source, fset, fn.Body
		}
	}
	t.Fatalf("function %s not found in %s", name, path)
	return nil, nil, nil
}
