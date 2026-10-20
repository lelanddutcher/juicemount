package catalog

import (
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestCollectionQuotaEveryCapExactAndPlusOne(t *testing.T) {
	dimensions := []struct {
		name               string
		fields             []int
		global, collection int64
	}{
		{"identities", []int{0}, 1024, 0},
		{"active entries", []int{1, 2}, 65536, 4096},
		{"lifetime entries", []int{2}, 131072, 8192},
		{"lifetime grants", []int{3}, 16384, 128},
		{"content events and receipts", []int{4, 5}, 131072, 8192},
		{"policy events and receipts", []int{6, 7}, 131072, 8192},
		{"canonical bytes", []int{8}, 256 << 20, 16 << 20},
	}
	for _, perCollection := range []bool{false, true} {
		scope := "library"
		if perCollection {
			scope = "collection"
		}
		t.Run(scope, func(t *testing.T) {
			for _, dimension := range dimensions {
				if perCollection && dimension.name == "identities" {
					continue
				}
				t.Run(dimension.name, func(t *testing.T) {
					cap := dimension.global
					if perCollection {
						cap = dimension.collection
					}
					var fields [12]int64
					for _, i := range dimension.fields {
						fields[i] = cap
					}
					counters := quotaFromFields(fields)
					usage := CollectionQuotaUsage{}
					delta := CollectionQuotaDelta{Library: counters}
					if perCollection {
						zero := CollectionQuotaCounters{}
						usage.Collection = &zero
						delta.Collection = &counters
					}
					at, err := planCollectionQuota(usage, delta)
					if err != nil {
						t.Fatalf("exact cap refused: %v", err)
					}
					var extra [12]int64
					for _, i := range dimension.fields {
						extra[i] = 1
					}
					increment := quotaFromFields(extra)
					over := CollectionQuotaDelta{Library: increment}
					if perCollection {
						over.Collection = &increment
					}
					if _, err := planCollectionQuota(at, over); !errors.Is(err, ErrCollectionCapacity) {
						t.Fatalf("cap+1 error: %v", err)
					}
					if _, err := planCollectionQuota(at, CollectionQuotaDelta{}); err != nil {
						t.Fatalf("zero allocation at capacity: %v", err)
					}
				})
			}
		})
	}
}

func TestCollectionQuotaInvalidStatesAndArithmetic(t *testing.T) {
	invalidUsage := []CollectionQuotaCounters{
		{Collections: -1},
		{ActiveEntries: 1},
		{ContentEvents: 1},
		{PolicyReceipts: 1},
		{LifetimeGrants: 1, ReservedPolicyEvents: 1},
		{LifetimeGrants: 1, ReservedPolicyEvents: 1, ReservedPolicyReceipts: 1, ReservedBytes: 3071},
		{ReservedPolicyEvents: 1, ReservedPolicyReceipts: 1, ReservedBytes: 3072},
		{LifetimeGrants: math.MaxInt64, ReservedPolicyEvents: math.MaxInt64, ReservedPolicyReceipts: math.MaxInt64, ReservedBytes: math.MaxInt64},
	}
	for _, bad := range invalidUsage {
		if _, err := planCollectionQuota(CollectionQuotaUsage{Library: bad}, CollectionQuotaDelta{}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid usage admitted: %+v %v", bad, err)
		}
	}
	for i := 0; i < 12; i++ {
		if i == 1 || i >= 9 {
			continue
		}
		fields := [12]int64{}
		fields[i] = -1
		if _, err := planCollectionQuota(CollectionQuotaUsage{}, CollectionQuotaDelta{Library: quotaFromFields(fields)}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("history decrease admitted at field%d: %v", i, err)
		}
	}
	for _, bad := range []CollectionQuotaCounters{{ActiveEntries: -1}, {ReservedPolicyEvents: -1, ReservedPolicyReceipts: -1, ReservedBytes: -3072}, {Collections: math.MaxInt64}} {
		initial := CollectionQuotaUsage{Library: CollectionQuotaCounters{Collections: 1}}
		_, err := planCollectionQuota(initial, CollectionQuotaDelta{Library: bad})
		if !errors.Is(err, ErrInvalid) {
			t.Fatalf("underflow/overflow refused incorrectly: %v", err)
		}
	}
	small := CollectionQuotaCounters{LifetimeEntries: 2}
	if _, err := planCollectionQuota(CollectionQuotaUsage{Library: CollectionQuotaCounters{LifetimeEntries: 1}, Collection: &small}, CollectionQuotaDelta{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("initial containment not enforced")
	}
	zero := CollectionQuotaCounters{}
	if _, err := planCollectionQuota(CollectionQuotaUsage{}, CollectionQuotaDelta{Collection: &zero}); !errors.Is(err, ErrInvalid) {
		t.Fatal("per-collection delta without state admitted")
	}
	if _, err := planCollectionQuota(CollectionQuotaUsage{Library: CollectionQuotaCounters{Collections: 1}, Collection: &zero}, CollectionQuotaDelta{Collection: &CollectionQuotaCounters{Collections: 1}}); !errors.Is(err, ErrInvalid) {
		t.Fatal("per-collection identity count admitted")
	}
	if _, err := planCollectionQuota(CollectionQuotaUsage{Collection: &zero}, CollectionQuotaDelta{Collection: &CollectionQuotaCounters{LifetimeEntries: 1}}); !errors.Is(err, ErrInvalid) {
		t.Fatal("final containment not enforced")
	}
}

func TestCollectionQuotaActivationCorrectionRemovalReactivation(t *testing.T) {
	zero := CollectionQuotaCounters{}
	before := CollectionQuotaUsage{Collection: &zero}
	activation := CollectionQuotaCounters{Collections: 1, LifetimeGrants: 1, PolicyEvents: 1, PolicyReceipts: 1, CanonicalBytes: 400, ReservedPolicyEvents: 1, ReservedPolicyReceipts: 1, ReservedBytes: 3072}
	localActivation := activation
	localActivation.Collections = 0
	active, err := planCollectionQuota(before, CollectionQuotaDelta{activation, &localActivation})
	if err != nil {
		t.Fatal(err)
	}
	if active.Library.CanonicalBytes+active.Library.ReservedBytes != 3472 || active.Library.PolicyEvents+active.Library.ReservedPolicyEvents != 2 {
		t.Fatal("activation did not charge future removal")
	}
	correction := CollectionQuotaCounters{PolicyEvents: 1, PolicyReceipts: 1, CanonicalBytes: 300}
	corrected, err := planCollectionQuota(active, CollectionQuotaDelta{correction, &correction})
	if err != nil {
		t.Fatal(err)
	}
	if corrected.Library.ReservedBytes != 3072 || corrected.Library.LifetimeGrants != 1 {
		t.Fatal("active correction reacquired credit/identity")
	}
	removal := CollectionQuotaCounters{PolicyEvents: 1, PolicyReceipts: 1, CanonicalBytes: 700, ReservedPolicyEvents: -1, ReservedPolicyReceipts: -1, ReservedBytes: -3072}
	removed, err := planCollectionQuota(corrected, CollectionQuotaDelta{removal, &removal})
	if err != nil {
		t.Fatal(err)
	}
	if removed.Library.ReservedBytes != 0 || removed.Library.CanonicalBytes != 1400 || removed.Library.PolicyEvents != 3 {
		t.Fatalf("removal accounting: %+v", removed.Library)
	}
	if _, err := planCollectionQuota(removed, CollectionQuotaDelta{removal, &removal}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("credit spent twice: %v", err)
	}
	reactivation := CollectionQuotaCounters{PolicyEvents: 1, PolicyReceipts: 1, CanonicalBytes: 400, ReservedPolicyEvents: 1, ReservedPolicyReceipts: 1, ReservedBytes: 3072}
	reactivated, err := planCollectionQuota(removed, CollectionQuotaDelta{reactivation, &reactivation})
	if err != nil {
		t.Fatal(err)
	}
	if reactivated.Library.LifetimeGrants != 1 || reactivated.Library.ReservedPolicyEvents != 1 || reactivated.Library.PolicyEvents != 4 {
		t.Fatal("reactivation failed to reacquire exactly one credit")
	}
	// Successful planning never mutates or aliases its borrowed per-collection state.
	if !reflect.DeepEqual(before.Collection, &zero) || active.Collection == before.Collection || corrected.Collection == active.Collection {
		t.Fatal("planner mutated/aliased input")
	}
}

func TestCollectionQuotaSecurityRemovalAtChargedCapacity(t *testing.T) {
	for _, perCollection := range []bool{false, true} {
		capEvents, capBytes := int64(131072), int64(256<<20)
		if perCollection {
			capEvents, capBytes = 8192, 16<<20
		}
		counters := CollectionQuotaCounters{LifetimeGrants: 1, PolicyEvents: capEvents - 1, PolicyReceipts: capEvents - 1, CanonicalBytes: capBytes - 3072, ReservedPolicyEvents: 1, ReservedPolicyReceipts: 1, ReservedBytes: 3072}
		usage := CollectionQuotaUsage{Library: counters}
		if perCollection {
			usage.Collection = &counters
		}
		// Maximum permitted security cost converts prepaid credit without growth.
		removal := CollectionQuotaCounters{PolicyEvents: 1, PolicyReceipts: 1, CanonicalBytes: 3072, ReservedPolicyEvents: -1, ReservedPolicyReceipts: -1, ReservedBytes: -3072}
		delta := CollectionQuotaDelta{Library: removal}
		if perCollection {
			delta.Collection = &removal
		}
		after, err := planCollectionQuota(usage, delta)
		if err != nil {
			t.Fatalf("prepaid removal at capacity: %v", err)
		}
		if after.Library.CanonicalBytes != capBytes || after.Library.PolicyEvents != capEvents || after.Library.ReservedBytes != 0 {
			t.Fatal("prepaid removal did not consume exact credit")
		}
		more := CollectionQuotaCounters{PolicyEvents: 1, PolicyReceipts: 1, CanonicalBytes: 1}
		next := CollectionQuotaDelta{Library: more}
		if perCollection {
			next.Collection = &more
		}
		if _, err := planCollectionQuota(after, next); !errors.Is(err, ErrCollectionCapacity) {
			t.Fatalf("ordinary growth at cap: %v", err)
		}
	}
	// Library-create provisioning reserves the same credit with no collection row.
	at := CollectionQuotaUsage{Library: CollectionQuotaCounters{LifetimeGrants: 1, CanonicalBytes: (256 << 20) - 3072}}
	reserve := CollectionQuotaDelta{Library: CollectionQuotaCounters{ReservedPolicyEvents: 1, ReservedPolicyReceipts: 1, ReservedBytes: 3072}}
	if _, err := planCollectionQuota(at, reserve); err != nil {
		t.Fatalf("library credit exact byte cap: %v", err)
	}
	reserve.Library.CanonicalBytes = 1
	if _, err := planCollectionQuota(at, reserve); !errors.Is(err, ErrCollectionCapacity) {
		t.Fatalf("library credit ignored in byte cap: %v", err)
	}
}

func TestCollectionQuotaOrdinaryBatchNetCreditRelease(t *testing.T) {
	before := CollectionQuotaUsage{Library: CollectionQuotaCounters{LifetimeGrants: 3, PolicyEvents: 131069, PolicyReceipts: 131069, CanonicalBytes: (256 << 20) - 3*3072, ReservedPolicyEvents: 3, ReservedPolicyReceipts: 3, ReservedBytes: 3 * 3072}}
	// Remove three and activate one in one operation: one actual event/receipt,
	// retire three credits, acquire one, allocate one lifetime grant identity.
	delta := CollectionQuotaDelta{Library: CollectionQuotaCounters{LifetimeGrants: 1, PolicyEvents: 1, PolicyReceipts: 1, CanonicalBytes: 2000, ReservedPolicyEvents: -2, ReservedPolicyReceipts: -2, ReservedBytes: -2 * 3072}}
	after, err := planCollectionQuota(before, delta)
	if err != nil {
		t.Fatalf("net release refused: %v", err)
	}
	if after.Library.PolicyEvents != 131070 || after.Library.ReservedPolicyEvents != 1 || after.Library.LifetimeGrants != 4 {
		t.Fatalf("batch fabricated removal events: %+v", after.Library)
	}
	if after.Library.CanonicalBytes+after.Library.ReservedBytes > 256<<20 {
		t.Fatal("net batch escaped charged-byte cap")
	}
	// Active-entry removal frees only the active count; lifetime history remains.
	entries := CollectionQuotaUsage{Library: CollectionQuotaCounters{ActiveEntries: 2, LifetimeEntries: 2}}
	out, err := planCollectionQuota(entries, CollectionQuotaDelta{Library: CollectionQuotaCounters{ActiveEntries: -1}})
	if err != nil || out.Library.ActiveEntries != 1 || out.Library.LifetimeEntries != 2 {
		t.Fatalf("entry tombstone accounting: %+v %v", out, err)
	}
}

func TestCollectionQuotaReservedPolicyCapacity(t *testing.T) {
	for _, perCollection := range []bool{false, true} {
		name, limit := "library", int64(131072)
		if perCollection {
			name, limit = "collection", 8192
		}
		t.Run(name, func(t *testing.T) {
			counters := CollectionQuotaCounters{
				LifetimeGrants:         1,
				PolicyEvents:           limit - 1,
				PolicyReceipts:         limit - 1,
				ReservedPolicyEvents:   1,
				ReservedPolicyReceipts: 1,
				ReservedBytes:          3072,
			}
			usage := CollectionQuotaUsage{Library: counters}
			before := CollectionQuotaUsage{Library: counters}
			if perCollection {
				local, saved := counters, counters
				usage.Collection, before.Collection = &local, &saved
			}
			at, err := planCollectionQuota(usage, CollectionQuotaDelta{})
			if err != nil || !reflect.DeepEqual(at, before) {
				t.Fatalf("zero delta at reserved policy cap: %+v %v", at, err)
			}
			if !reflect.DeepEqual(usage, before) {
				t.Fatal("zero-delta planning mutated usage")
			}
			// Only policy event/receipt occupancy exceeds its cap; 3072 charged bytes
			// remain well below both byte limits, isolating reserved event accounting.
			increment := CollectionQuotaCounters{PolicyEvents: 1, PolicyReceipts: 1}
			delta := CollectionQuotaDelta{Library: increment}
			if perCollection {
				delta.Collection = &increment
			}
			if _, err := planCollectionQuota(usage, delta); !errors.Is(err, ErrCollectionCapacity) {
				t.Fatalf("actual event plus retained reserve admitted: %v", err)
			}
			if !reflect.DeepEqual(usage, before) {
				t.Fatal("refused planning mutated usage")
			}
		})
	}
}
