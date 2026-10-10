package mutationauthority

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestRenewNeverAdvancesExpiredOrUnknownEpoch(t *testing.T) {
	j, _ := testJournal(t)
	ctx := context.Background()
	now := time.Now()
	j.now = func() time.Time { return now }
	l, err := j.Claim(ctx, "volume", "session", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	next, err := j.Renew(ctx, l, time.Minute)
	if err != nil || next.Epoch != l.Epoch {
		t.Fatalf("renew: %+v %v", next, err)
	}
	wrong := l
	wrong.Session = "other"
	if _, err := j.Renew(ctx, wrong, time.Minute); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("foreign renewal: %v", err)
	}
	now = now.Add(2 * time.Minute)
	if _, err := j.Renew(ctx, l, time.Minute); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expired renewal: %v", err)
	}
	next, err = j.Claim(ctx, "volume", "new", time.Minute)
	if err != nil || next.Epoch != l.Epoch+1 {
		t.Fatalf("new claim: %+v %v", next, err)
	}
}

func TestPreparedEnumerationRetainsAllTypesAcrossResolutionAndRestart(t *testing.T) {
	j, path := testJournal(t)
	ctx := context.Background()
	l, err := j.Claim(ctx, "volume", "owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"z-media", "a-custody", "middle"} {
		_, err := j.Execute(ctx, testRequest(l, id), backendFunc(func(context.Context, Request) (DurabilityProof, error) {
			return DurabilityProof{}, errors.New("uncertain")
		}))
		if !errors.Is(err, ErrUncertain) {
			t.Fatal(err)
		}
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	var got []string
	after := ""
	for {
		rows, err := j.ListPrepared(ctx, "volume", after, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 0 {
			break
		}
		o := rows[0]
		if o.State != Prepared || o.Session != l.Session || o.Epoch != l.Epoch || o.ExpectedRevision != "parent@7" || o.Digest != testRequest(l, o.ID).Digest {
			t.Fatalf("lost immutable request: %+v", o)
		}
		got = append(got, o.ID)
		after = o.ID
		if err := j.ResolveNoEffect(ctx, o.Request, "fixture callback made no effect"); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(got, []string{"a-custody", "middle", "z-media"}) {
		t.Fatal(got)
	}
	if _, err := j.ListPrepared(ctx, "volume", "", 257); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unbounded enumeration: %v", err)
	}
	if _, err := j.Claim(ctx, "volume", "successor", time.Minute); err != nil {
		t.Fatal(err)
	}
}

func TestRenewDeadlineCrossingCannotCreateSuccessor(t *testing.T) {
	j, _ := testJournal(t)
	ctx := context.Background()
	now := time.Now()
	j.now = func() time.Time { return now }
	l, err := j.Claim(ctx, "volume", "session", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	j.now = func() time.Time {
		calls++
		if calls == 1 {
			return now
		}
		return now.Add(2 * time.Second)
	}
	next, err := j.Renew(ctx, l, time.Minute)
	if err != nil || next.Epoch != l.Epoch || next.Session != l.Session {
		t.Fatalf("renewal created successor: %+v %v", next, err)
	}
}
