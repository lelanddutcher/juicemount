package respproxy

import (
	"context"
	"reflect"
	"testing"
)

func historyReply(t *testing.T, mac *rawConn, accepted, current, status, foreign string) {
	t.Helper()
	v := mac.do("JM.LEASE", "CHECK_REPLACE", "path:Projects/A", accepted, current)
	if v.Type != '*' || len(v.Elems) != 4 || string(v.Elems[0].Str) != status || v.Elems[1].Int != 3 ||
		inoStr(uint64(v.Elems[2].Int)) != current || inoStr(uint64(v.Elems[3].Int)) != foreign {
		t.Fatalf("history reply: %+v", v)
	}
}

func TestReplacementHistoryPersistsAcrossRenewalAndProxyRestart(t *testing.T) {
	r := startReplacementRig(t)
	mac := r.conn(t, "mac1")
	historyReply(t, mac, "1", "1", "CLEAN", "0")
	if v := mac.do("JM.LEASE", "RELEASE", "ino:3", "1"); v.Type != '*' || v.Elems[1].Int != 2 {
		t.Fatalf("release: %+v", v)
	}
	peer := r.conn(t, "mac2")
	if v := peer.do("JM.LEASE", "ACQUIRE", "ino:3", "auto"); v.Type != '*' || v.Elems[1].Int != 3 {
		t.Fatalf("peer grant: %+v", v)
	}
	if v := peer.do("JM.LEASE", "RELEASE", "ino:3", "3"); v.Type != '*' || v.Elems[1].Int != 4 {
		t.Fatalf("peer release: %+v", v)
	}
	if v := mac.do("JM.LEASE", "ACQUIRE", "ino:3", "auto"); v.Type != '*' || len(v.Elems) != 5 || v.Elems[1].Int != 5 || v.Elems[4].Int != 1 {
		t.Fatalf("owner grant: %+v", v)
	}
	fresh := r.conn(t, "mac1")
	if v := fresh.do("JM.LEASE", "ACQUIRE", "ino:3", "auto"); v.Type != '*' || string(v.Elems[0].Str) != "RENEWED" || v.Elems[4].Int != 0 {
		t.Fatalf("fresh renewal: %+v", v)
	}
	ctx := context.Background()
	before, err := r.db.HGetAll(ctx, leaseKey(3)).Result()
	if err != nil {
		t.Fatal(err)
	}
	historyReply(t, fresh, "1", "5", "FOREIGN", "3")
	historyReply(t, fresh, "3", "5", "CLEAN", "3")
	historyReply(t, fresh, "5", "5", "CLEAN", "3")
	after, err := r.db.HGetAll(ctx, leaseKey(3)).Result()
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("preflight changed lease: %v", err)
	}
	restarted := restartReplacementProxy(t, r)
	again := restarted.raw(t)
	again.login("mac1")
	historyReply(t, again, "1", "5", "FOREIGN", "3")
	historyReply(t, again, "5", "5", "CLEAN", "3")
	// Releasing/reacquiring by the same owner must not make historical foreign
	// tenure after epoch3 look newer than an edit accepted at epoch5.
	if v := again.do("JM.LEASE", "RELEASE", "ino:3", "5"); v.Type != '*' || v.Elems[1].Int != 6 {
		t.Fatalf("same owner release: %+v", v)
	}
	if v := again.do("JM.LEASE", "ACQUIRE", "ino:3", "auto"); v.Type != '*' || v.Elems[1].Int != 7 {
		t.Fatalf("same owner grant: %+v", v)
	}
	historyReply(t, again, "5", "7", "CLEAN", "3")
	r.unchanged(t)
}

func TestReplacementHistoryRejectsUnqualifiedSnapshots(t *testing.T) {
	for _, kind := range []string{"wrong-principal", "admin", "wrong-db", "stale-epoch", "future-accepted", "noncanonical-accepted", "missing-own", "wrong-own", "malformed-other", "future-other", "counter-changed", "singleton-changed", "observe", "unresolved"} {
		t.Run(kind, func(t *testing.T) {
			r := startReplacementRig(t)
			mac := r.conn(t, "mac1")
			accepted, current := "0", "1"
			ctx := context.Background()
			switch kind {
			case "wrong-principal":
				mac = r.conn(t, "mac2")
			case "admin":
				mac = r.conn(t, "admin1")
			case "wrong-db":
				// This rig permits DB15 only, so isolate the server helper's
				// VolumeDB predicate rather than failing early at SELECT.
				reply := r.eng.checkReplacementHistory(Principal{Name: "mac1", Kind: KindMac}, 0, r.tp.Incarnation(), strArgs("ino:3", accepted, current))
				if len(reply) == 0 || reply[0] != '-' {
					t.Fatalf("wrong DB accepted: %q", reply)
				}
				r.unchanged(t)
				return
			case "stale-epoch":
				current = "3"
			case "future-accepted":
				accepted = "2"
			case "noncanonical-accepted":
				accepted = "00"
			case "missing-own":
				if err := r.db.HDel(ctx, leaseKey(3), "e:mac1").Err(); err != nil {
					t.Fatal(err)
				}
			case "wrong-own":
				if err := r.db.HSet(ctx, leaseKey(3), "e:mac1", "2").Err(); err != nil {
					t.Fatal(err)
				}
			case "malformed-other":
				if err := r.db.HSet(ctx, leaseKey(3), "e:old", "01").Err(); err != nil {
					t.Fatal(err)
				}
			case "future-other":
				if err := r.db.HSet(ctx, leaseKey(3), "e:old", "2").Err(); err != nil {
					t.Fatal(err)
				}
			case "counter-changed":
				if err := r.db.Set(ctx, epochKey(3), "2", 0).Err(); err != nil {
					t.Fatal(err)
				}
			case "singleton-changed":
				if err := r.db.Set(ctx, SingletonKey, "test-foreign-incarnation", 0).Err(); err != nil {
					t.Fatal(err)
				}
				// Both a command refusal and ownership-fencing disconnect are
				// valid fail-closed outcomes. Do not require a RESP error frame.
				if _, err := r.tp.client(t, "mac1").Do(ctx5(t), "JM.LEASE", "CHECK_REPLACE", "ino:3", accepted, current).Result(); err == nil {
					t.Fatal("lost singleton accepted history check")
				}
				r.unchanged(t)
				return
			case "observe":
				r.tp.setMode(ModeObserve, "test")
			case "unresolved":
				r.eng.mu.Lock()
				r.eng.leases[3].unresolved = true
				r.eng.mu.Unlock()
			}
			if v := mac.do("JM.LEASE", "CHECK_REPLACE", "ino:3", accepted, current); v.Type != '-' {
				t.Fatalf("unqualified history accepted: %+v", v)
			}
			r.unchanged(t)
		})
	}
}

func TestReplacementHistoryPreservesEpochPrecision(t *testing.T) {
	r := startReplacementRig(t)
	ctx := context.Background()
	const epoch uint64 = 9007199254740993
	const foreign uint64 = 9007199254740992
	if err := r.db.Set(ctx, epochKey(3), inoStr(epoch), 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := r.db.HSet(ctx, leaseKey(3), "epoch", inoStr(epoch), "e:mac1", inoStr(epoch), "e:mac2", inoStr(foreign)).Err(); err != nil {
		t.Fatal(err)
	}
	r.eng.mu.Lock()
	r.eng.leases[3].epoch = epoch
	r.eng.leases[3].hist["mac1"] = epoch
	r.eng.leases[3].hist["mac2"] = foreign
	r.eng.mu.Unlock()
	mac := r.conn(t, "mac1")
	historyReply(t, mac, "9007199254740991", inoStr(epoch), "FOREIGN", inoStr(foreign))
	historyReply(t, mac, inoStr(foreign), inoStr(epoch), "CLEAN", inoStr(foreign))
	r.unchanged(t)
}

func TestReplacementHistoryRequiresSingleton(t *testing.T) {
	r := startLeaseRig(t, ModeEnforce)
	mac := r.conn(t, "mac1")
	if v := mac.do("JM.LEASE", "ACQUIRE", "ino:3", "auto"); v.Type != '*' {
		t.Fatalf("grant: %+v", v)
	}
	if v := mac.do("JM.LEASE", "CHECK_REPLACE", "ino:3", "0", "1"); v.Type != '-' {
		t.Fatalf("non-singleton history accepted: %+v", v)
	}
}
