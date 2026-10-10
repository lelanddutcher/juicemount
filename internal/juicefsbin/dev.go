//go:build jmdevjuicefs

package juicefsbin

// devAnyExplicitBinary lets a developer build run an explicitly configured
// juicefs (go build -tags jmdevjuicefs). scripts/verify-build.sh refuses a
// service binary whose build settings carry this tag.
const devAnyExplicitBinary = true
