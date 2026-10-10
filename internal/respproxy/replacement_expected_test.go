package respproxy

import (
	"context"
	"strings"
	"testing"

	"github.com/juicedata/juicefs/pkg/replaceguard"
)

func TestReplacementExpectedMissingBeforeRead(t *testing.T) {
	r := startReplacementRig(t)
	mac := r.arm(t)
	raw, err := replaceguard.Encode(r.rec)
	if err != nil {
		t.Fatal(err)
	}
	// The client already observed an armed record. Delete it before the
	// server's first read, not merely between that read and its Lua CAS.
	if v := mac.do("GET", replaceguard.Key(21)); v.Type != '$' || string(v.Str) != string(raw) {
		t.Fatalf("client did not observe armed operation: %+v", v)
	}
	if err := r.db.Del(context.Background(), replaceguard.Key(21)).Err(); err != nil {
		t.Fatal(err)
	}
	if v := mac.do("JM.LEASE", "ARM_REPLACE", string(raw), string(raw)); v.Type != '-' {
		t.Fatalf("recovery recreated missing operation: %+v", v)
	}
	if r.db.Exists(context.Background(), replaceguard.Key(21)).Val() != 0 {
		t.Fatal("missing record recreated")
	}
	r.unchanged(t)
}

func TestReplacementExpectedRejectsUntrustedEvidence(t *testing.T) {
	for _, fault := range []string{"empty", "malformed", "foreign-owner", "committed", "changed-image", "future-epoch", "noncanonical"} {
		t.Run(fault, func(t *testing.T) {
			r := startReplacementRig(t)
			mac := r.arm(t)
			expected := r.rec
			request, err := replaceguard.Encode(r.rec)
			if err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "foreign-owner":
				expected.Owner = "mac2"
			case "committed":
				expected.Phase = "committed"
			case "changed-image":
				expected.ImageSHA256 = strings.Repeat("d", 64)
			case "future-epoch":
				expected.Epoch++
			}
			raw, err := replaceguard.Encode(expected)
			if err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "empty":
				raw = nil
			case "malformed":
				raw = []byte("{bad")
			case "noncanonical":
				raw = append(raw, ' ')
			}
			if v := mac.do("JM.LEASE", "ARM_REPLACE", string(request), string(raw)); v.Type != '-' {
				t.Fatalf("invalid expected evidence accepted: %+v", v)
			}
			assertReplacementStored(t, r, r.rec)
			r.unchanged(t)
		})
	}
}

func TestReplacementExpectedCASAndLostReply(t *testing.T) {
	for _, fault := range []string{"delete", "already-desired", "different-operation", "lost-reply"} {
		t.Run(fault, func(t *testing.T) {
			wrapper := &replacementRefreshFault{}
			r := startReplacementRig(t, func(c *LeaseConfig) { wrapper.inner = c.Side; c.Side = wrapper })
			r.arm(t)
			original := r.rec
			prior, err := replaceguard.Encode(original)
			if err != nil {
				t.Fatal(err)
			}
			mac := replacementRefreshNewTenure(t, r)
			desired, err := replaceguard.Encode(r.rec)
			if err != nil {
				t.Fatal(err)
			}
			raced := r.rec
			if fault == "different-operation" {
				raced.OperationID = strings.Repeat("d", 32)
			}
			racedRaw, err := replaceguard.Encode(raced)
			if err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "delete", "lost-reply":
				wrapper.arm(fault, "")
			default:
				wrapper.arm("replace", string(racedRaw))
			}
			v := mac.do("JM.LEASE", "ARM_REPLACE", string(desired), string(prior))
			if v.Type != '-' || wrapper.count() != 1 {
				t.Fatalf("strict CAS fault did not refuse once: reply=%+v fired=%d", v, wrapper.count())
			}
			ctx := context.Background()
			switch fault {
			case "delete":
				if r.db.Exists(ctx, replaceguard.Key(21)).Val() != 0 {
					t.Fatal("disappeared expected record recreated")
				}
			case "already-desired", "different-operation":
				if got := r.db.Get(ctx, replaceguard.Key(21)).Val(); got != string(racedRaw) {
					t.Fatal("raced record replaced")
				}
			case "lost-reply":
				if got := r.db.Get(ctx, replaceguard.Key(21)).Val(); got != string(desired) {
					t.Fatal("fault did not commit refresh")
				}
				// A stale exact expectation remains stale, even if the desired record
				// is now stored. Recovery first reads and qualifies that record.
				if retry := mac.do("JM.LEASE", "ARM_REPLACE", string(desired), string(prior)); retry.Type != '-' {
					t.Fatalf("stale expectation accepted after lost reply: %+v", retry)
				}
				observed := mac.do("GET", replaceguard.Key(21))
				if observed.Type != '$' || observed.Null || string(observed.Str) != string(desired) {
					t.Fatalf("recovery read: %+v", observed)
				}
				if retry := mac.do("JM.LEASE", "ARM_REPLACE", string(desired), string(observed.Str)); retry.Type != '$' || retry.Null || string(retry.Str) != string(desired) {
					t.Fatalf("qualified same-record retry failed: %+v", retry)
				}
				if wrapper.count() != 1 {
					t.Fatal("fault repeated")
				}
				r.unchanged(t)
				renameRefreshedReplacement(t, r.tp, r, r.rec)
				return
			}
			r.unchanged(t)
		})
	}
}
