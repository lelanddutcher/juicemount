// SLICE 5 tests: cron parser wrapper, scheduler update idempotency,
// and schedule → saved destination → Job env wiring.
package manager

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestCronParserWrapper exercises resolveCronExpr against malformed
// inputs (must reject), the friendly presets (must accept and rewrite
// to the canonical 5-field form), and raw 5-field expressions (pass
// through unchanged). Critical correctness gate: a typo'd cron string
// must NEVER pass validation — the scheduler relies on cron.Parse
// returning a usable Schedule.
func TestCronParserWrapper(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		wantOut string
		wantErr bool
	}{
		{"empty", "", "", true},
		{"whitespace-only", "   ", "", true},
		{"bad-too-few-fields", "0 2 *", "", true},
		{"bad-junk", "not a cron", "", true},
		{"bad-out-of-range-minute", "60 0 * * *", "", true},

		{"preset-nightly-2am", "nightly-2am", "0 2 * * *", false},
		{"preset-weekly-sun-3am", "weekly-sun-3am", "0 3 * * 0", false},
		{"preset-hourly", "hourly", "0 * * * *", false},
		{"preset-every-6-hours", "every-6-hours", "0 */6 * * *", false},

		{"raw-every-minute", "* * * * *", "* * * * *", false},
		{"raw-noon-daily", "0 12 * * *", "0 12 * * *", false},
		// Descriptor support (robfig/cron's "@hourly" shorthand) must
		// also parse since DescriptorOption is enabled on cronParser.
		{"descriptor-hourly", "@hourly", "@hourly", false},
		// Trims surrounding whitespace.
		{"trims-whitespace", "  0 2 * * *  ", "0 2 * * *", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			expr, sched, err := resolveCronExpr(c.in)
			if (err != nil) != c.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, c.wantErr)
			}
			if c.wantErr {
				return
			}
			if expr != c.wantOut {
				t.Errorf("expr=%q want=%q", expr, c.wantOut)
			}
			if sched == nil {
				t.Errorf("schedule nil on success path")
			}
		})
	}
}

// TestSchedulerUpdateIdempotent verifies that calling upsert with
// allowReplace=true on the same name does NOT leave stale cron entries
// in the engine — a re-update must result in exactly ONE entry per
// name (otherwise we'd double-fire on every tick).
func TestSchedulerUpdateIdempotent(t *testing.T) {
	// We don't need a real JobManager for this test — the scheduler's
	// engine bookkeeping is what we're checking. nil mgr is fine
	// because we never let the cron engine actually fire (we don't
	// call Start).
	store := newScheduleStore(nil, nil, "")
	// Start the engine so registerLocked actually adds entries.
	// resolveCronExpr will succeed for our test cases.
	store.cron.Start()
	defer store.cron.Stop()
	store.mu.Lock()
	store.started = true
	store.mu.Unlock()

	// Bypass validateSchedule's dests-exists check by leaving dests=nil
	// (validateSchedule short-circuits the lookup when dests == nil).
	s := Schedule{
		Name: "bk",
		Source: SourceSpec{
			Path:      "/sources/foo",
			Direction: DirectionIn,
		},
		Destination:   DestinationRef{Name: "some-dest"},
		Options:       DefaultSyncOptions(),
		Cron:          "0 2 * * *",
		RetainHistory: 5,
	}

	if err := store.upsert(s, false); err != nil {
		t.Fatalf("upsert 1: %v", err)
	}
	store.mu.RLock()
	id1, ok := store.entries["bk"]
	count1 := len(store.entries)
	count1Rows := len(store.rows)
	store.mu.RUnlock()
	if !ok {
		t.Fatalf("entry not registered after first upsert")
	}
	if count1 != 1 || count1Rows != 1 {
		t.Fatalf("after upsert 1: entries=%d rows=%d, want 1/1", count1, count1Rows)
	}

	// Second upsert with allowReplace=true. The cron entry should be
	// REPLACED, not duplicated — entries map size stays 1, but the
	// underlying entry id must change so the engine isn't double-
	// firing the old + new closure.
	s.Cron = "0 3 * * *"
	if err := store.upsert(s, true); err != nil {
		t.Fatalf("upsert 2: %v", err)
	}
	store.mu.RLock()
	id2, ok := store.entries["bk"]
	count2 := len(store.entries)
	count2Rows := len(store.rows)
	store.mu.RUnlock()
	if !ok {
		t.Fatalf("entry not registered after second upsert")
	}
	if count2 != 1 || count2Rows != 1 {
		t.Fatalf("after upsert 2: entries=%d rows=%d, want 1/1", count2, count2Rows)
	}
	if id1 == id2 {
		t.Errorf("entry id unchanged after update — old cron closure still firing")
	}

	// Third upsert WITHOUT allowReplace should be rejected.
	if err := store.upsert(s, false); err == nil {
		t.Errorf("upsert 3 (allowReplace=false): expected duplicate error, got nil")
	}

	// remove should drop the entry from both the map and the rows.
	store.remove("bk")
	store.mu.RLock()
	_, stillThere := store.entries["bk"]
	rowsAfter := len(store.rows)
	store.mu.RUnlock()
	if stillThere {
		t.Errorf("entry not removed from engine")
	}
	if rowsAfter != 0 {
		t.Errorf("rows after remove = %d, want 0", rowsAfter)
	}

	// remove is idempotent — second call must not panic.
	store.remove("bk")
}

// TestScheduleResolvesDestination is the integration assertion: a
// schedule referencing a kind=s3 destination profile resolves to a
// Submit() call whose extraEnv carries ACCESS_KEY / SECRET_KEY and
// whose argv-form destination does NOT carry credentials. This is the
// security-critical wiring from SLICE-4 (saved destinations) into the
// SLICE-5 scheduler.
func TestScheduleResolvesDestination(t *testing.T) {
	mux := http.NewServeMux()
	mgr := Register(mux, "", Config{
		JuiceFSBin:  "/dev/null",
		FUSEMount:   "/mnt/juicefs",
		SourceRoots: []string{"/sources"},
		DestMount:   "/jfs",
		AdminKey:    "test-admin-key-please-do-not-use-prod",
		StateFile:   "",
	})
	defer mgr.StopAll()

	// Capture the runner's source/destination/extraEnv so we can assert
	// the schedule fire produced the right argument shape.
	type captured struct {
		source, destination string
		env                 []string
	}
	capCh := make(chan captured, 1)
	mgr.SetRunner(func(_ context.Context, _ string, _ RunSyncSpec, src, dst string, _ SyncOptions, env []string, _ chan<- ProgressEvent) error {
		// Defensive copy — extraEnv slice's lifetime is tied to the
		// Job, but the test goroutine reads from `cap` whenever.
		envCopy := append([]string(nil), env...)
		select {
		case capCh <- captured{source: src, destination: dst, env: envCopy}:
		default:
		}
		return nil
	})

	// Create an s3 destination via the public API so the encryption
	// path is exercised end-to-end (this is also slice-4 wiring).
	body := `{
		"name": "test-s3",
		"kind": "s3",
		"config": {
			"endpoint": "https://s3.example.com",
			"bucket": "my-bucket",
			"access_key": "AKIA-FOR-TEST",
			"secret_key": "SECRET-FOR-TEST"
		}
	}`
	postDest := httptest.NewRequest(http.MethodPost, "/api/destinations", strings.NewReader(body))
	postDest.Header.Set("X-JuiceMount-Admin-Key", "test-admin-key-please-do-not-use-prod")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, postDest)
	if w.Code != http.StatusCreated {
		t.Fatalf("destination POST: %d body=%s", w.Code, w.Body.String())
	}

	// Now create a schedule that points at it. Use a far-future cron so
	// the engine doesn't tick on its own — we trigger via /run.
	schedBody := `{
		"name": "nightly-bk",
		"source": {"path": "/sources/foo", "direction": "in"},
		"destination": {"name": "test-s3"},
		"options": {"preserve_structure": true, "threads": 4},
		"cron": "0 2 1 1 *"
	}`
	postSched := httptest.NewRequest(http.MethodPost, "/api/schedules", strings.NewReader(schedBody))
	postSched.Header.Set("X-JuiceMount-Admin-Key", "test-admin-key-please-do-not-use-prod")
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, postSched)
	if w2.Code != http.StatusCreated {
		t.Fatalf("schedule POST: %d body=%s", w2.Code, w2.Body.String())
	}

	// Trigger run-now so we get a deterministic test.
	runReq := httptest.NewRequest(http.MethodPost, "/api/schedules/nightly-bk/run", nil)
	runReq.Header.Set("X-JuiceMount-Admin-Key", "test-admin-key-please-do-not-use-prod")
	w3 := httptest.NewRecorder()
	mux.ServeHTTP(w3, runReq)
	if w3.Code != http.StatusAccepted {
		t.Fatalf("run-now: %d body=%s", w3.Code, w3.Body.String())
	}
	var runResp struct {
		JobID    string `json:"job_id"`
		Schedule string `json:"schedule"`
	}
	if err := json.Unmarshal(w3.Body.Bytes(), &runResp); err != nil {
		t.Fatalf("run-now JSON: %v", err)
	}
	if runResp.Schedule != "nightly-bk" {
		t.Errorf("run-now schedule = %q, want nightly-bk", runResp.Schedule)
	}
	if runResp.JobID == "" {
		t.Fatalf("run-now returned empty job_id")
	}

	// Wait for the runner to receive the submit call.
	got := <-capCh

	// Source should pass through unchanged.
	if got.source != "/sources/foo" {
		t.Errorf("source = %q, want /sources/foo", got.source)
	}
	// Destination should be the s3:// argv form WITHOUT credentials —
	// just the bucket. Endpoint goes via env (S3_ENDPOINT).
	if !strings.HasPrefix(got.destination, "s3://my-bucket") {
		t.Errorf("destination = %q, want s3://my-bucket/ prefix", got.destination)
	}
	if strings.Contains(got.destination, "SECRET-FOR-TEST") || strings.Contains(got.destination, "AKIA-FOR-TEST") {
		t.Errorf("destination URI leaks credentials: %q", got.destination)
	}
	// Env must carry the credentials — this is the security-critical
	// assertion. ps aux would show argv, not env, so credentials there
	// is safe; credentials in argv would not be.
	wantEnv := map[string]string{
		"ACCESS_KEY":  "AKIA-FOR-TEST",
		"SECRET_KEY":  "SECRET-FOR-TEST",
		"S3_ENDPOINT": "https://s3.example.com",
	}
	for k, v := range wantEnv {
		want := k + "=" + v
		found := false
		for _, e := range got.env {
			if e == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("env missing %q (got env=%v)", want, got.env)
		}
	}

	// The submitted Job should also be visible via /api/jobs and carry
	// the schedule_name annotation.
	jobsReq := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
	jobsReq.Header.Set("X-JuiceMount-Admin-Key", "test-admin-key-please-do-not-use-prod")
	w4 := httptest.NewRecorder()
	mux.ServeHTTP(w4, jobsReq)
	if w4.Code != http.StatusOK {
		t.Fatalf("list jobs: %d", w4.Code)
	}
	if !strings.Contains(w4.Body.String(), `"schedule_name": "nightly-bk"`) {
		t.Errorf("job list missing schedule_name annotation:\n%s", w4.Body.String())
	}
}

