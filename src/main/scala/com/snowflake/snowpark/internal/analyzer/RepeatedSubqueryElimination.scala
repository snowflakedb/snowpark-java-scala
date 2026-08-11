package com.snowflake.snowpark.internal.analyzer

import scala.collection.mutable.ArrayBuffer

/**
 * SNOW-3894042 Fix A: repeated-subquery (common sub-expression) elimination.
 *
 * The Scala client's SQL builders inline every child DataFrame as a nested `FROM (subquery)` and
 * never emit CTEs (see package.scala projectStatement). When the same subtree is referenced more
 * than once (e.g. the customer pipeline's retail and _nr branches, which are structurally identical
 * and therefore produce byte-identical SQL), the whole computation - including every window - is
 * emitted twice and executed twice.
 *
 * This pass detects maximal identical `(SELECT ...)` fragments that occur >= 2 times in a generated
 * SELECT and hoists them into a real `WITH cte AS (...)` clause, replacing each occurrence with
 * `(SELECT * FROM cte)`. Because Snowpark's FROM-subqueries are self-contained (no outer
 * correlation), this is semantics-preserving. It is gated behind the client toggle
 * `snowpark_cte_optimization_enabled` (default off) so existing behavior is unchanged.
 */
private[analyzer] object RepeatedSubqueryElimination {

  private val CtePrefix = "SNOWPARK_CSE_CTE_"

  /** Rewrite a single SELECT statement, hoisting repeated subqueries into a WITH clause. */
  def rewrite(sql: String, minFragmentLen: Int = 400, maxCtes: Int = 16): String = {
    if (sql == null) return sql
    val trimmed = sql.trim
    // Only rewrite plain SELECTs; skip DDL/DML and already-rewritten SQL.
    if (!trimmed.regionMatches(true, 0, "SELECT", 0, 6)) return sql
    if (sql.contains(CtePrefix)) return sql

    val spans = findSelectSubquerySpans(sql)
    if (spans.isEmpty) return sql

    val frags = spans.map { case (s, e) => sql.substring(s, e) }
    val counts = frags.groupBy(identity).map { case (f, list) => f -> list.size }

    // Candidates: substantial fragments repeated at least twice.
    var candidates =
      counts.collect { case (f, c) if c >= 2 && f.length >= minFragmentLen => f }.toSeq
    if (candidates.isEmpty) return sql

    // Prefer the largest fragments; skip any fragment fully contained in an already-picked one
    // (hoisting the parent collapses the child automatically).
    candidates = candidates.sortBy(-_.length)
    val picked = ArrayBuffer.empty[String]
    for (f <- candidates if picked.size < maxCtes) {
      if (!picked.exists(p => p.contains(f))) picked += f
    }
    if (picked.isEmpty) return sql

    var body = sql
    val cteDefs = ArrayBuffer.empty[String]
    picked.zipWithIndex.foreach { case (frag, i) =>
      val name = s"$CtePrefix$i"
      val inner = frag.substring(1, frag.length - 1) // strip the outer parentheses
      cteDefs += s"$name AS ($inner)"
      body = body.replace(frag, s"(SELECT * FROM $name)")
    }
    "WITH " + cteDefs.mkString(", ") + " " + body
  }

  /**
   * Find balanced `(SELECT ...)` spans (start index of '(' to index just past matching ')'),
   * skipping single-quoted string literals. Nested spans are included so repeats at any depth are
   * counted. Shared with [[LargeQueryBreakdown]].
   */
  private[analyzer] def findSelectSubquerySpans(s: String): Seq[(Int, Int)] = {
    val res = ArrayBuffer.empty[(Int, Int)]
    val n = s.length
    var i = 0
    while (i < n) {
      val c = s.charAt(i)
      if (c == '\'') {
        i = skipStringLiteral(s, i)
      } else if (c == '(') {
        var j = i + 1
        while (j < n && s.charAt(j).isWhitespace) j += 1
        if (s.regionMatches(true, j, "SELECT", 0, 6)) {
          val end = matchingParen(s, i)
          if (end > 0) res += ((i, end))
        }
        i += 1
      } else {
        i += 1
      }
    }
    res.toSeq
  }

  // Given index of '(', return index just past the matching ')', quote-aware. -1 if unbalanced.
  private def matchingParen(s: String, open: Int): Int = {
    val n = s.length
    var depth = 1
    var k = open + 1
    while (k < n && depth > 0) {
      val c = s.charAt(k)
      if (c == '\'') {
        k = skipStringLiteral(s, k)
      } else {
        if (c == '(') depth += 1 else if (c == ')') depth -= 1
        k += 1
      }
    }
    if (depth == 0) k else -1
  }

  // Given index of opening quote, return index just past the closing quote.
  private def skipStringLiteral(s: String, quote: Int): Int = {
    val n = s.length
    var k = quote + 1
    while (k < n && s.charAt(k) != '\'') {
      if (s.charAt(k) == '\\') k += 1
      k += 1
    }
    k + 1
  }
}
