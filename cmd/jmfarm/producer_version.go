package main

import "strconv"

// defaultProducerVersion turns the immutable release commit into the integer
// producer generation carried by derivative rows. Ready rows remain valid when
// a new build ships, but a failed row is only terminal for the exact producer
// generation that observed the failure. That makes a codec/runtime upgrade
// retry old failures once without making a permanently unsupported source run
// on every sweep.
//
// Seven hex digits fit on every supported 32/64-bit target. Adding two keeps
// release generations above the legacy/default version 1 even for an all-zero
// prefix. Development builds retain 1 so local fixtures and manual tools remain
// backward compatible.
func defaultProducerVersion(commit string) int {
	if len(commit) != 40 {
		return 1
	}
	n, err := strconv.ParseUint(commit[:7], 16, 32)
	if err != nil {
		return 1
	}
	return int(n) + 2
}
