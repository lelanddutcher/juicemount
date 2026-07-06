//go:build race

package metadata

// bigDirPageBudgetMultiplier scales the ListChildrenPage wall-clock budget
// under the race detector, whose instrumentation inflates per-op latency
// ~40x. Scaling keeps the index-loss regression gate meaningful under -race
// (a paginated seek measured ~12ms with the index present, well under the
// 80ms scaled budget; a temp-b-tree-per-page regression lands far higher)
// while stopping a plain `go test -race ./metadata/` from redding on a pure
// timing artifact and masking a real future data-race regression.
const bigDirPageBudgetMultiplier = 40
