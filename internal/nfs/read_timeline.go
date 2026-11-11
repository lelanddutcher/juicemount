package nfs

// Per-read timeline (slow-link diagnosis, 2026-11-11 laptop lab).
//
// The juicemount nfs package installs these hooks. ReadTimelineOn reports
// whether a timeline line would be written (lab switch or Debug level), so
// a READ that nobody traces pays one function call and no allocation.
var (
	ReadTimelineOn = func() bool { return false }
	ReadTimeline   = func(msg string, args ...any) {}
)

// errString is "" for nil, so a timeline line has a stable field type.
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
