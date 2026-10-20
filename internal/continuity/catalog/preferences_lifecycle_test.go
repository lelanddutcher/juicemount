package catalog

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPreferencePostEncodingGenerationFence(t *testing.T) {
	for _, kind := range []string{"select", "clear"} {
		for _, route := range []string{"search", "page", "status"} {
			t.Run(kind+"/"+route, func(t *testing.T) {
				s, _, _, d, bearer, now := preferencesTestStore(t)
				_ = s
				first := recordPreferenceTest(t, d, bearer, preferenceRequestTest("first", "select", testAsset, nil, now.Load()))
				r := discoveryRequestTest("search", "")
				r.PageSize = 1
				r.Version = 2
				if route == "page" {
					c := v2SearchTest(t, d, bearer, r)
					r = discoveryPageRequest(*c.Next)
					r.Version = 2
				}
				reached, release := make(chan struct{}), make(chan struct{})
				d.beforeFence = func(ctx context.Context) error {
					close(reached)
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				type response struct {
					result DisclosureResult
					err    error
					body   string
				}
				done := make(chan response, 1)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				go func() {
					var out bytes.Buffer
					var result DisclosureResult
					var err error
					if route == "status" {
						result, err = d.PreferenceState(ctx, bearer, PreferenceStateRequest{2, testLibrary, testAsset}, &out)
					} else {
						result, err = d.Query(ctx, bearer, r, &out)
					}
					done <- response{result, err, out.String()}
				}()
				select {
				case <-reached:
				case <-ctx.Done():
					close(release)
					<-done
					t.Fatal("response never reached post-encoding fence")
				}
				// Use a separately owned service so its mutation has no test gate.
				writer := discoveryServiceTest(t, s)
				var token *string
				if kind == "clear" {
					token = first.Token
				}
				request := preferenceRequestTest("change", kind, testAsset, token, now.Load())
				var changed bytes.Buffer
				_, mutationErr := writer.RecordPreference(ctx, bearer, request, &changed)
				close(release)
				got := <-done
				if mutationErr != nil {
					t.Fatal(mutationErr)
				}
				if DiscoveryErrorCode(got.err) != "restart_query" || got.result.Started || got.body != "" {
					t.Fatalf("post-encoding preference mutation disclosed stale %s: %+v %v %s", route, got.result, got.err, got.body)
				}
			})
		}
	}
}

func TestPreferenceExpiryAndClockAfterFenceCleanup(t *testing.T) {
	for _, route := range []string{"search", "page", "status"} {
		t.Run(route, func(t *testing.T) {
			s, o, a, d, bearer, now := preferencesTestStore(t)
			selected := now.Load()
			recordPreferenceTest(t, d, bearer, preferenceRequestTest("first", "select", testAsset, nil, selected))
			expiry := selected + preferenceAge
			now.Store(expiry - 100)
			bearer = discoveryIssueTest(t, o, a, "fresh", ownerSubject, Scope{"library", ""}, discoveryAllActions)
			now.Store(expiry - 1)
			r := discoveryRequestTest("search", "")
			r.Version = 2
			r.PageSize = 1
			if route == "page" {
				c := v2SearchTest(t, d, bearer, r)
				r = discoveryPageRequest(*c.Next)
				r.Version = 2
			}
			// The clock invocation after the fresh snapshot's preference-time check
			// occurs only after that read owner closes. Keep auth valid at this time.
			calls := 0
			threshold := 5
			if route == "page" {
				threshold = 4
			}
			s.clock = func() time.Time {
				calls++
				if calls >= threshold {
					return time.Unix(expiry, 0)
				}
				return time.Unix(expiry-1, 0)
			}
			var out bytes.Buffer
			var result DisclosureResult
			var err error
			if route == "status" {
				result, err = d.PreferenceState(context.Background(), bearer, PreferenceStateRequest{2, testLibrary, testAsset}, &out)
			} else {
				result, err = d.Query(context.Background(), bearer, r, &out)
			}
			if DiscoveryErrorCode(err) != "restart_query" || result.Started || out.Len() != 0 || calls != threshold {
				t.Fatalf("post-cleanup expiry released stale %s: calls=%d want=%d %+v %v", route, calls, threshold, result, err)
			}
		})
	}
}

func TestPreferenceConcurrentIdenticalSelectAndOutputUncertainty(t *testing.T) {
	s, _, _, d, bearer, now := preferencesTestStore(t)
	r := preferenceRequestTest("same", "select", testAsset, nil, now.Load())
	start := make(chan struct{})
	var wg sync.WaitGroup
	type response struct {
		data []byte
		err  error
	}
	done := make(chan response, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			var out bytes.Buffer
			_, err := d.RecordPreference(context.Background(), bearer, r, &out)
			done <- response{append([]byte(nil), out.Bytes()...), err}
		}()
	}
	close(start)
	wg.Wait()
	left, right := <-done, <-done
	if left.err != nil || right.err != nil || !bytes.Equal(left.data, right.data) {
		t.Fatalf("concurrent exact retry: %v %v", left.err, right.err)
	}
	sub, err := readPreferenceSubject(context.Background(), s.db, testLibrary, ownerSubject)
	if err != nil || sub.generation != 1 || sub.entries != 1 || sub.receipts != 1 {
		t.Fatalf("duplicate selection state: %+v %v", sub, err)
	}
	failure := errors.New("output failure")
	r.OperationID = "uncertain"
	result, err := d.RecordPreference(context.Background(), bearer, r, discoveryWriterFunc(func(b []byte) (int, error) { return len(b), failure }))
	if !result.Started || !errors.Is(err, failure) {
		t.Fatalf("full output+error: %+v %v", result, err)
	}
	current := preferenceStatusTest(t, d, bearer, testAsset)
	recovered := recordPreferenceTest(t, d, bearer, r)
	if current == nil || *current != *recovered.Token {
		t.Fatal("uncertain acknowledgement was reapplied")
	}
	sub, _ = readPreferenceSubject(context.Background(), s.db, testLibrary, ownerSubject)
	if sub.generation != 2 {
		t.Fatal("uncertain retry bumped generation")
	}
}

func TestPreferenceServicePersonGuardIndependently(t *testing.T) {
	s, o, a, d, _, now := preferencesTestStore(t)
	const service SubjectID = "sub_44444444444444444444444444444444"
	policyTest(t, o, a, "service", NewPolicyChange("create_subject", CreateSubject{service, SubjectValue{"service", "service", true}}))
	sub, err := readSubject(context.Background(), s.db, testLibrary, service)
	if err != nil {
		t.Fatal(err)
	}
	// Privileged synthetic rows deliberately bypass the issuance/policy person
	// restrictions while satisfying each independent action/scope check. This
	// discriminates the domain's person guard rather than an upstream guard.
	actions, _ := json.Marshal(discoveryAllActions)
	secret := bytes.Repeat([]byte{0x99}, 32)
	selector := "cred_" + strings.Repeat("a", 32)
	verifier := credentialVerifier(testLibrary, a.Incarnation, selector, secret)
	if _, err := s.db.Exec(`INSERT INTO policy_grants(grant_id,library_id,subject_id,scope_kind,asset_id,actions,created_sequence,updated_sequence,removed) VALUES('synthetic-service',?,?,'library',NULL,?,?,?,0)`, testLibrary, service, actions, sub.CreatedSequence, sub.CreatedSequence); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO catalog_credentials(selector,library_id,incarnation,subject_id,scope_kind,asset_id,actions,verifier,issued_unix,expires_unix,generation,revoked) VALUES(?,?,?,?,'library',NULL,?,?,?,?,1,0)`, selector, testLibrary, a.Incarnation, service, actions, verifier[:], now.Load()-1, now.Load()+3600); err != nil {
		t.Fatal(err)
	}
	bearer := []byte("jmc1_" + strings.Repeat("a", 32) + "." + hex.EncodeToString(secret))
	var out bytes.Buffer
	result, err := d.PreferenceState(context.Background(), bearer, PreferenceStateRequest{2, testLibrary, testAsset}, &out)
	if DiscoveryErrorCode(err) != "not_found" || result.Started || out.Len() != 0 {
		t.Fatalf("service private preference status admitted: %+v %v", result, err)
	}
	result, err = d.RecordPreference(context.Background(), bearer, preferenceRequestTest("service-select", "select", testAsset, nil, now.Load()), &out)
	if DiscoveryErrorCode(err) != "not_found" || result.Started || out.Len() != 0 {
		t.Fatalf("service person-activity admitted: %+v %v", result, err)
	}
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM preference_subjects`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("denial allocated preference identity: %d %v", count, err)
	}
}

func TestPreferenceOwnSubjectRankingIsolation(t *testing.T) {
	_, o, a, d, bearer, now := preferencesTestStore(t)
	policyTest(t, o, a, "reader", NewPolicyChange("create_subject", CreateSubject{readerSubject, SubjectValue{"person", "reader", true}}))
	discoveryGrantTest(t, o, a, "reader", readerSubject, Scope{"library", ""}, discoveryAllActions)
	reader := discoveryIssueTest(t, o, a, "reader", readerSubject, Scope{"library", ""}, discoveryAllActions)
	recordPreferenceTest(t, d, bearer, preferenceRequestTest("owner-select", "select", testAsset, nil, now.Load()))
	recordPreferenceTest(t, d, reader, preferenceRequestTest("reader-select", "select", otherAsset, nil, now.Load()))
	r := discoveryRequestTest("search", "")
	r.PageSize = 1
	mine := v2SearchTest(t, d, reader, r)
	if !bytes.Contains(mine.Items[0], []byte(otherAsset)) || bytes.Contains(mine.Items[0], []byte(testAsset)) {
		t.Fatalf("another subject's selection entered personal ranking: %s", mine.Items[0])
	}
}

func TestPreferenceAbsentGenerationAndOwnStatusFence(t *testing.T) {
	s, _, _, d, bearer, now := preferencesTestStore(t)
	_ = s
	var out bytes.Buffer
	state, err := d.PreferenceState(context.Background(), bearer, PreferenceStateRequest{2, testLibrary, testAsset}, &out)
	if err != nil || !state.Started {
		t.Fatal(err)
	}
	var identities int
	if err := s.db.QueryRow(`SELECT count(*) FROM preference_subjects`).Scan(&identities); err != nil || identities != 0 {
		t.Fatal("status allocated permanent state")
	}
	r := discoveryRequestTest("search", "")
	r.Version = 2
	r.PageSize = 1
	c := v2SearchTest(t, d, bearer, r)
	if c.Next == nil {
		t.Fatal("missing frozen token")
	}
	recordPreferenceTest(t, d, bearer, preferenceRequestTest("first", "select", otherAsset, nil, now.Load()))
	page := discoveryPageRequest(*c.Next)
	page.Version = 2
	out.Reset()
	result, err := d.Query(context.Background(), bearer, page, &out)
	if DiscoveryErrorCode(err) != "restart_query" || result.Started || out.Len() != 0 {
		t.Fatalf("absent→present preference ABA blessed old page: %+v %v", result, err)
	}
}
