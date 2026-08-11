package com.snowflake.snowpark.internal.analyzer

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * SNOW-3894042 Fix B: Large Query Breakdown.
 *
 * The Scala client emits the entire pipeline as one fused statement (up to 185 joins / 300+ windows
 * / 1.45MB SQL in the customer workload), which the engine executes as a single giant DAG that
 * spills heavily (686TB local spill vs 190TB scanned). There is no plan-splitting today.
 *
 * This pass breaks a too-large SELECT into stages: while the statement exceeds a size bound, it
 * picks the largest self-contained `(SELECT ...)` subquery (preferring one that is repeated, so the
 * materialization is also reused), physically materializes it into a TEMPORARY table, and rewrites
 * the statement to read from that table. Unlike a textual CTE (which Snowflake may re-inline), a
 * temp table FORCES the subtree to be computed exactly once, shrinking the fused statement and
 * cutting spill. Gated behind `snowpark_large_query_breakdown_enabled` (default off).
 */
private[analyzer] object LargeQueryBreakdown {

  /**
   * Materialized-stage plan: temp-table CREATEs to run first, the rewritten final SELECT, DROPs.
   */
  case class Result(createSqls: Seq[String], newSql: String, dropSqls: Seq[String])

  private val TmpPrefix = "SNOWPARK_LQB_TMP_"

  def breakdown(
      sql: String,
      charBound: Int,
      minFragmentLen: Int = 2000,
      maxStages: Int = 8): Result = {
    if (sql == null) return Result(Seq.empty, sql, Seq.empty)
    val trimmed = sql.trim
    val isSelect =
      trimmed.regionMatches(true, 0, "SELECT", 0, 6) || trimmed.regionMatches(true, 0, "WITH", 0, 4)
    if (!isSelect) return Result(Seq.empty, sql, Seq.empty)

    val runId = Random.alphanumeric.take(6).mkString.toUpperCase
    val creates = ArrayBuffer.empty[String]
    val drops = ArrayBuffer.empty[String]
    var body = sql
    var stage = 0
    var progressing = true

    while (stage < maxStages && body.length > charBound && progressing) {
      progressing = false
      val spans = RepeatedSubqueryElimination.findSelectSubquerySpans(body)
      // Candidate fragments: substantial, and not the whole statement.
      val frags =
        spans.map { case (s, e) => body.substring(s, e) }.filter { f =>
          f.length >= minFragmentLen && f.length < body.length
        }
      if (frags.nonEmpty) {
        val counts = frags.groupBy(identity).map { case (f, l) => f -> l.size }
        // Prefer a repeated fragment (materialize once, reuse many); else the single largest.
        val repeated = counts.collect { case (f, c) if c >= 2 => f }.toSeq.sortBy(-_.length)
        val chosen = repeated.headOption
          .orElse(frags.distinct.sortBy(-_.length).headOption)
        chosen.foreach { frag =>
          val name = s"$TmpPrefix${runId}_$stage"
          val inner = frag.substring(1, frag.length - 1) // strip outer parens
          creates += s"CREATE OR REPLACE TEMPORARY TABLE $name AS $inner"
          drops += s"DROP TABLE IF EXISTS $name"
          body = body.replace(frag, s"(SELECT * FROM $name)")
          progressing = true
        }
      }
      stage += 1
    }
    Result(creates.toSeq, body, drops.toSeq)
  }
}
