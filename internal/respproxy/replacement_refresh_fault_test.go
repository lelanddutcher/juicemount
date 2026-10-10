package respproxy

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/juicedata/juicefs/pkg/replaceguard"
)

// Install the wrapper before engine startup; only the one armed enrollment is
// changed. The actual Redis script still runs through the production SidePool.
type replacementRefreshFault struct {
	inner sideDoer
	mu    sync.Mutex
	fault string
	value string
	fired int
}

func (s *replacementRefreshFault) Do(db int, args ...[]byte) (Value, error) {
	s.mu.Lock()
	fault, value := "", ""
	if len(args) > 1 && string(args[0]) == "EVAL" && string(args[1]) == armReplacementLua && s.fault != "" {
		fault, value = s.fault, s.value
		s.fault = ""
		s.fired++
	}
	s.mu.Unlock()
	if fault == "delete" || fault == "replace" {
		mutation := strArgs("DEL", replaceguard.Key(21))
		if fault == "replace" {
			mutation = strArgs("SET", replaceguard.Key(21), value)
		}
		v, err := s.inner.Do(db, mutation...)
		if err != nil {
			return v, err
		}
		if err = v.Err(); err != nil {
			return v, err
		}
	}
	v, err := s.inner.Do(db, args...)
	if fault == "lost-reply" && err == nil && v.Err() == nil {
		return Value{}, errors.New("injected lost reply after replacement refresh committed")
	}
	return v, err
}

func (s *replacementRefreshFault) arm(fault, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fault, s.value = fault, value
}
func (s *replacementRefreshFault) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fired
}

func replacementRefreshNewTenure(t *testing.T, r *replacementRig) *rawConn {
	t.Helper()
	mac := r.conn(t, "mac1")
	v := mac.do("JM.LEASE", "RELEASE", "ino:3", inoStr(r.rec.Epoch))
	if v.Type != '*' || len(v.Elems) < 2 || string(v.Elems[0].Str) != "RELEASED" {
		t.Fatalf("release: %+v", v)
	}
	v = mac.do("JM.LEASE", "ACQUIRE", "ino:3", "auto")
	if v.Type != '*' || len(v.Elems) < 2 || string(v.Elems[0].Str) != "GRANTED" || v.Elems[1].Int <= int64(r.rec.Epoch) {
		t.Fatalf("new tenure: %+v", v)
	}
	r.rec.Epoch = uint64(v.Elems[1].Int)
	return mac
}

func TestReplacementRefreshCASAndLostReply(t *testing.T) {
	for _, fault := range []string{"delete", "different-operation", "committed", "empty", "lost-reply"} {
		t.Run(fault, func(t *testing.T) {
			wrapper := &replacementRefreshFault{}
			r := startReplacementRig(t, func(c *LeaseConfig) { wrapper.inner = c.Side; c.Side = wrapper })
			r.arm(t)
			mac := replacementRefreshNewTenure(t, r)
			injected := r.rec
			if fault == "different-operation" {
				injected.OperationID = strings.Repeat("d", 32)
			}
			if fault == "committed" {
				injected.Phase = "committed"
			}
			changed, err := replaceguard.Encode(injected)
			if err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "delete", "lost-reply":
				wrapper.arm(fault, "")
			case "empty":
				wrapper.arm("replace", "")
			default:
				wrapper.arm("replace", string(changed))
			}
			raw, err := replaceguard.Encode(r.rec)
			if err != nil {
				t.Fatal(err)
			}
			v := mac.do("JM.LEASE", "ARM_REPLACE", string(raw))
			if v.Type != '-' || wrapper.count() != 1 {
				t.Fatalf("fault did not refuse exactly once: reply=%+v fired=%d", v, wrapper.count())
			}
			ctx := context.Background()
			stored, readErr := r.db.Get(ctx, replaceguard.Key(21)).Bytes()
			switch fault {
			case "delete":
				if r.db.Exists(ctx, replaceguard.Key(21)).Val() != 0 {
					t.Fatal("missing expected record was recreated")
				}
			case "empty":
				if readErr != nil || len(stored) != 0 {
					t.Fatalf("empty existing record overwritten: %q %v", stored, readErr)
				}
			case "lost-reply":
				if readErr != nil || string(stored) != string(raw) {
					t.Fatalf("fixture did not commit refresh: %s %v", stored, readErr)
				}
				retry := mac.do("JM.LEASE", "ARM_REPLACE", string(raw))
				if retry.Type != '$' || retry.Null || string(retry.Str) != string(stored) {
					t.Fatalf("lost reply retry: %+v", retry)
				}
				if wrapper.count() != 1 {
					t.Fatal("fault repeated")
				}
			default:
				if readErr != nil || string(stored) != string(changed) {
					t.Fatalf("CAS replaced raced evidence: %s %v", stored, readErr)
				}
			}
			r.unchanged(t)
		})
	}
}

func TestReplacementInitialArmDoesNotOverwriteRacedRecord(t *testing.T) {
	wrapper := &replacementRefreshFault{}
	r := startReplacementRig(t, func(c *LeaseConfig) { wrapper.inner = c.Side; c.Side = wrapper })
	injected := r.rec
	injected.OperationID = strings.Repeat("d", 32)
	injected.Owner = "mac1"
	injected.Proxy = r.tp.Incarnation()
	changed, err := replaceguard.Encode(injected)
	if err != nil {
		t.Fatal(err)
	}
	wrapper.arm("replace", string(changed))
	raw, err := replaceguard.Encode(r.rec)
	if err != nil {
		t.Fatal(err)
	}
	v := r.conn(t, "mac1").do("JM.LEASE", "ARM_REPLACE", string(raw))
	if v.Type != '-' || wrapper.count() != 1 {
		t.Fatalf("initial enrollment race: %+v fired=%d", v, wrapper.count())
	}
	if got := r.db.Get(context.Background(), replaceguard.Key(21)).Val(); got != string(changed) {
		t.Fatal("initial enrollment replaced raced operation")
	}
	r.unchanged(t)
}
