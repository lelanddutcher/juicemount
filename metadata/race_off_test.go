//go:build !race

package metadata

// bigDirPageBudgetMultiplier scales the ListChildrenPage wall-clock budget.
// Without race instrumentation the page is a true O(limit) index seek, so the
// budget stays tight (multiplier 1) to catch an idx_parent_name regression
// (temp-b-tree ORDER BY re-sorting all children per page lands far above 2ms).
const bigDirPageBudgetMultiplier = 1

// pruneLadderScaleBudgetMultiplier keeps the durable-prune-ladder 120k-scale
// budget (#10) tight without race instrumentation: 5s against an observed
// ~0.4s persist+load, so a mechanism regression (per-row tx, lost chunking)
// reds loudly. See race_on_test.go for the -race scaling rationale.
const pruneLadderScaleBudgetMultiplier = 1
