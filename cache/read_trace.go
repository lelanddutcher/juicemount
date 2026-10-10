package cache

import "time"

// ReadTrace records only synchronous phases of one Reader call. Durations are
// nested within the caller's cache-read duration; they are not additive to it.
// The caller owns the value and must not share it between concurrent reads.
type ReadTrace struct {
	MappingLookupNS          int64
	MappingLookupCount       uint32
	MappingRemoteSharedCount uint32
	VerifiedBlockNS          int64
	Outcome                  string
}

func (tr *ReadTrace) finishMappingLookup(start time.Time) {
	tr.MappingLookupNS += time.Since(start).Nanoseconds()
}
