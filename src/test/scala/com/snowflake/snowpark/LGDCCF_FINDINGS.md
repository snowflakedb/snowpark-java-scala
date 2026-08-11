# SNOW-3894042 — Findings, Internal-Fix Design, and User Optimizations

Companion to `LgdccfReproSuite` (this directory), which reproduces the customer's
plan pathologies against the real Snowpark Scala client.

## Reproduction evidence (measured)

At SMOKE scale the repro suite confirms, at the client level:

- Baseline fused SQL: **16,799 bytes, 11 JOINs** in a single statement.
- Scaling `numHorizons`/`numFinalJoins` 4->12: SQL grows 9,953 -> 28,834 bytes.
- With `materializeStages=true`, the non-dedup run emits **two byte-identical**
  `INSERT INTO SNOWPARK_TEMP_TABLE_*` for the account rollup (retail + `_nr`) —
  direct proof of the P1 duplicate-computation root cause in the Scala client.

The Scala client (v1.22.0-SNAPSHOT) has **no CTE optimization and no large query
breakdown** (only a local `Simplifier`). Both exist in the Python client.

---

## Part 1 — Internal Snowpark Scala fixes (DESIGN)

These are analyzer-core features. Each is Medium-to-Large. Feasibility and exact
insertion points below; implementation is a dedicated project, not a patch.

### Fix A — CTE / common-subplan elimination

Insertion point: a new rewrite stage in `Analyzer.resolve`
(`src/main/scala/com/snowflake/snowpark/internal/analyzer/Analyzer.scala:11-12`),
between `Simplifier.simplify` and `SqlGenerator.generateSqlQuery`:

```
val optimized = new Simplifier(session).simplify(resolved)
val withCte   = new RepeatedSubqueryElimination(session).apply(optimized)  // NEW, flag-gated
val result    = SqlGenerator.generateSqlQuery(withCte, session)
```

What must be built:
- Duplicate-subtree finder over `LogicalPlan` (use `children` + case-class
  structural equality; count occurrences into a `HashMap[LogicalPlan, Int]`).
- A stable subtree identity that survives alias differences — mirror Python's
  `encode_node_id_with_query` by hashing the resolved `queries.last.sql`.
- A new `WithQueryBlock` LogicalPlan node + a CTE-reference leaf.
- `WITH` keyword + `withStatement` builder in `package.scala`; a `SqlGenerator`
  case that prepends `WITH cte AS (...)` to the final query and swaps duplicate
  inline subqueries for the CTE name.

Hard part: alias/`ExprId`/`internalRenamedColumns` identity so a CTE reference
yields exactly the columns the inlined subtree did. This is why Python hashes SQL.

Expected improvement: removes the duplicated subtrees inside each fused statement
(e.g. the 125-min temp assembly that is structurally identical to the 271-min
final). **~15-25% on CustomerLdf.**

### Fix B — Large Query Breakdown (highest leverage)

Compute a plan-complexity score (sum of join/window/group-by/etc. node
categories). When it exceeds a bound, materialize the highest-complexity
pipeline-breaker subtree to a scoped temp table and rewire the parent to read it;
repeat. Same insertion point as Fix A (a resolve-stage pass), plus complexity
scoring on `LogicalPlan`.

Expected improvement: splits the 185-join / 300-window mega-statement into
3-5 memory-fitting statements, cutting the 8-13x spill. **~30-50% on the top
fused statements (~80% of each run).** This is the single biggest lever.

### Fix C — cross-statement subplan reuse

When `.cacheResult()` is called on two DataFrames with identical resolved plans,
issue one CREATE+INSERT and reuse the temp table for the second reference.
Requires the same content-hash primitive as Fix A, applied at cacheResult time.
Expected improvement: **~10-15%** (removes the duplicate temp-table writes).

Files touched (all fixes): `Analyzer.scala`, new `RepeatedSubqueryElimination.scala`
/ `LargeQueryBreakdown.scala`, `SnowflakePlanNode.scala` (new node), `SqlGenerator.scala`,
`package.scala` (WITH builder), `Session.scala` (flags).

---

## Part 2 — User code optimizations (workaround)

Validated: the repro `dedupNrBranch` knob demonstrates Opt 1 at the client level.

### Opt 1 — compute shared stages once (retail vs _nr)

Before (recomputes the branch — two identical subtrees):
```scala
val retail = featureBranch(session, config)
val nr     = featureBranch(session, config)   // fresh table reads, duplicate plan
```
After (bind once, reuse):
```scala
val shared = featureBranch(session, config).cacheResult()  // materialize once
val retail = shared
val nr     = shared
```
Expected: **10-15%** (eliminates the second materialization / recompute).

### Opt 2 — reduce cacheResult to strategic points

Remove `.cacheResult()` on stages read only once (they add CREATE+INSERT with no
reuse benefit). Keep it only where a DF is read 2+ times or to deliberately cap
fused-statement size. Expected: **15-25%** of the 18.8h temp-table bucket.

### Opt 3 — fix non-SARGable join keys

Before: `join(b, a("start") === last_day(add_months(b("d"), -1)))` — blocks pruning.
After: precompute `month_end` as a stored/generated column and join on equality.
Expected: **10-15%** of scan time (restores pruning; today -1854% on some tables).

### Opt 4 — replace Scala UDFs with native SQL

Replace `MaxConseqInc/Dec` (row-by-row over `collect_list` arrays) with a native
window expression (`CONDITIONAL_CHANGE_EVENT` + running sum). Expected: **5-10%**
of Account pipeline time.

---

## Total expected improvement

| Category | Est. reduction |
|---|---|
| Internal fixes (A+B+C) | 40-60% |
| User optimizations (1-4) | 30-40% |
| Combined | ~60-75% (2.5-4x); 7.68h worst run -> ~2-3h |

Caveats: estimates from workload-profile + directional reproduction, not a
full-scale re-run with fixes applied; A/B overlap (both address duplication);
LQB gain depends on complexity thresholds and memory fit.
