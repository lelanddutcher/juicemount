//go:build race

package metadata

// raceEnabled: timing bounds do not hold under the race detector.
const raceEnabled = true
