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

// pruneLadderScaleBudgetMultiplier scales the durable-prune-ladder 120k-scale
// wall-clock budget (#10) the same way: race instrumentation inflates the
// chunked-tx persist ~25x (observed 0.4s clean → ~10s under -race), so the
// tight 5s budget is asserted by the non-race run while the -race run keeps
// the same test exercising correctness (rows land, counts round-trip) without
// redding on instrumentation overhead.
const pruneLadderScaleBudgetMultiplier = 40
