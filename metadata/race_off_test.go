//go:build !race

package metadata

// bigDirPageBudgetMultiplier scales the ListChildrenPage wall-clock budget.
// Without race instrumentation the page is a true O(limit) index seek, so the
// budget stays tight (multiplier 1) to catch an idx_parent_name regression
// (temp-b-tree ORDER BY re-sorting all children per page lands far above 2ms).
const bigDirPageBudgetMultiplier = 1
