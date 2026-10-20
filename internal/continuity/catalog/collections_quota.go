package catalog

import (
	"errors"
	"math"
)

var ErrCollectionCapacity = errors.New("collection capacity reached")

// Logical request/receipt bytes and prepaid removal credits are not physical
// SQLite/WAL size. These values must eventually come from transaction-owned rows.
type CollectionQuotaCounters struct {
	Collections            int64
	ActiveEntries          int64
	LifetimeEntries        int64
	LifetimeGrants         int64
	ContentEvents          int64
	ContentReceipts        int64
	PolicyEvents           int64
	PolicyReceipts         int64
	CanonicalBytes         int64
	ReservedPolicyEvents   int64
	ReservedPolicyReceipts int64
	ReservedBytes          int64
}
type CollectionQuotaUsage struct {
	Library    CollectionQuotaCounters
	Collection *CollectionQuotaCounters
}
type CollectionQuotaDelta struct {
	Library    CollectionQuotaCounters
	Collection *CollectionQuotaCounters
}

func quotaFields(c CollectionQuotaCounters) [12]int64 {
	return [12]int64{c.Collections, c.ActiveEntries, c.LifetimeEntries, c.LifetimeGrants, c.ContentEvents, c.ContentReceipts, c.PolicyEvents, c.PolicyReceipts, c.CanonicalBytes, c.ReservedPolicyEvents, c.ReservedPolicyReceipts, c.ReservedBytes}
}
func quotaFromFields(f [12]int64) CollectionQuotaCounters {
	return CollectionQuotaCounters{f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7], f[8], f[9], f[10], f[11]}
}
func checkedQuotaAdd(a, b int64) (int64, bool) {
	if b > 0 && a > math.MaxInt64-b || b < 0 && a < math.MinInt64-b {
		return 0, false
	}
	return a + b, true
}
func validQuotaCounters(c CollectionQuotaCounters, perCollection bool) bool {
	for _, n := range quotaFields(c) {
		if n < 0 {
			return false
		}
	}
	if perCollection && c.Collections != 0 || c.ActiveEntries > c.LifetimeEntries || c.ContentEvents != c.ContentReceipts || c.PolicyEvents != c.PolicyReceipts || c.ReservedPolicyEvents != c.ReservedPolicyReceipts || c.ReservedPolicyEvents > c.LifetimeGrants || c.ReservedPolicyEvents > math.MaxInt64/CollectionRemovalReserveBytes {
		return false
	}
	return c.ReservedBytes == c.ReservedPolicyEvents*CollectionRemovalReserveBytes
}
func quotaWithinCaps(c CollectionQuotaCounters, perCollection bool) bool {
	limits := [9]int64{1024, 65536, 131072, 16384, 131072, 131072, 131072, 131072, 256 << 20}
	if perCollection {
		limits = [9]int64{0, 4096, 8192, 128, 8192, 8192, 8192, 8192, 16 << 20}
	}
	fields := quotaFields(c)
	for i, limit := range limits {
		n := fields[i]
		var ok bool
		switch i {
		case 6:
			n, ok = checkedQuotaAdd(n, c.ReservedPolicyEvents)
		case 7:
			n, ok = checkedQuotaAdd(n, c.ReservedPolicyReceipts)
		case 8:
			n, ok = checkedQuotaAdd(n, c.ReservedBytes)
		default:
			ok = true
		}
		if !ok || n > limit {
			return false
		}
	}
	return true
}
func quotaContained(small, big CollectionQuotaCounters) bool {
	a, b := quotaFields(small), quotaFields(big)
	for i, n := range a {
		if n > b[i] {
			return false
		}
	}
	return true
}
func applyQuotaDelta(c, delta CollectionQuotaCounters, perCollection bool) (CollectionQuotaCounters, error) {
	a, d := quotaFields(c), quotaFields(delta)
	for i, n := range d {
		// Only active entries and attached reserves may shrink; history never does.
		if n < 0 && i != 1 && i != 9 && i != 10 && i != 11 {
			return CollectionQuotaCounters{}, invalid("collection quota delta")
		}
		value, ok := checkedQuotaAdd(a[i], n)
		if !ok || value < 0 {
			return CollectionQuotaCounters{}, invalid("collection quota arithmetic")
		}
		a[i] = value
	}
	out := quotaFromFields(a)
	if !validQuotaCounters(out, perCollection) {
		return CollectionQuotaCounters{}, invalid("collection quota state")
	}
	if !quotaWithinCaps(out, perCollection) {
		return CollectionQuotaCounters{}, ErrCollectionCapacity
	}
	return out, nil
}

// planCollectionQuota is arithmetic, not an admission API: caller authority,
// exact retry, actual attached credits and frozen byte costs remain transaction
// checks. It accepts ordinary net credit release but no security bypass flag.
func planCollectionQuota(usage CollectionQuotaUsage, delta CollectionQuotaDelta) (CollectionQuotaUsage, error) {
	if !validQuotaCounters(usage.Library, false) || usage.Collection == nil && delta.Collection != nil {
		return CollectionQuotaUsage{}, invalid("collection quota input")
	}
	if usage.Collection != nil && (!validQuotaCounters(*usage.Collection, true) || !quotaContained(*usage.Collection, usage.Library)) {
		return CollectionQuotaUsage{}, invalid("collection quota input")
	}
	if !quotaWithinCaps(usage.Library, false) || usage.Collection != nil && !quotaWithinCaps(*usage.Collection, true) {
		return CollectionQuotaUsage{}, ErrCollectionCapacity
	}
	library, err := applyQuotaDelta(usage.Library, delta.Library, false)
	if err != nil {
		return CollectionQuotaUsage{}, err
	}
	out := CollectionQuotaUsage{Library: library}
	if usage.Collection != nil {
		var d CollectionQuotaCounters
		if delta.Collection != nil {
			d = *delta.Collection
		}
		collection, err := applyQuotaDelta(*usage.Collection, d, true)
		if err != nil {
			return CollectionQuotaUsage{}, err
		}
		if !quotaContained(collection, library) {
			return CollectionQuotaUsage{}, invalid("collection quota containment")
		}
		out.Collection = &collection
	}
	return out, nil
}

type collectionSecurityCost struct {
	RequestBytes   int64
	ReceiptBytes   int64
	PolicyEvents   int64
	PolicyReceipts int64
}

// collectionSecurityRemovalCost establishes bounded cost and receipt matching,
// never owner authority, grant existence, a usable credit, or successful CAS.
func collectionSecurityRemovalCost(request CollectionPolicyRequest, receipt CollectionPolicyReceipt) (collectionSecurityCost, error) {
	if request.Category != "security_remove" {
		return collectionSecurityCost{}, invalid("collection security cost")
	}
	b, digest, err := CanonicalCollectionPolicyRequest(request)
	if err != nil {
		return collectionSecurityCost{}, err
	}
	r, err := CanonicalCollectionPolicyReceipt(receipt)
	if err != nil || receipt.LibraryID != request.LibraryID || receipt.OperationID != request.ID || receipt.PayloadSHA256 != digest || len(b) > MaxCollectionSecurityRequestBytes || len(r) > MaxCollectionReceiptBytes {
		return collectionSecurityCost{}, invalid("collection security receipt")
	}
	return collectionSecurityCost{int64(len(b)), int64(len(r)), 1, 1}, nil
}
