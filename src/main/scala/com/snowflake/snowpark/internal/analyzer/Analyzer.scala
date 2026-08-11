package com.snowflake.snowpark.internal.analyzer

import com.snowflake.snowpark.Session
import com.snowflake.snowpark.internal.Logging

private[snowpark] class Analyzer(session: Session) extends Logging {
  def resolve(plan: LogicalPlan): SnowflakePlan = {
    plan.getOrUpdateSnowflakePlan {
      val result = session.withAnalysisMode(isLazyMode = true) {
        val resolved = plan.analyzed
        val optimized = new Simplifier(session).simplify(resolved)
        val result = SqlGenerator.generateSqlQuery(optimized, session)

        // telemetry
        val summaryBefore: String = resolved.summarize
        val summaryAfter: String = optimized.summarize
        if (summaryAfter != summaryBefore) {
          result.setSimplifierUsageGenerator(queryId =>
            session.conn.telemetry.reportSimplifierUsage(queryId, summaryBefore, summaryAfter))
        }
        result
      }
      result.analyzeIfneeded()
      result
    }
  }
}

/**
 * SNOW-3894042: execution-time plan optimizer (large-query breakdown + CTE elimination).
 *
 * Applied ONCE, at execution time, on the final plan about to run - NOT during analysis. Doing it
 * at analysis time is unsafe because in eager mode every intermediate DataFrame is resolved and
 * schema-analyzed, so materializing temp tables there causes schema DESCRIBEs to reference
 * not-yet-created temps. At execution the schema is already known and we only rewrite SQL strings.
 */
private[snowpark] object PlanOptimizer extends Logging {

  def optimize(plan: SnowflakePlan, session: Session): SnowflakePlan = {
    val lqbOn = session.conn.largeQueryBreakdownEnabled
    val cteOn = session.conn.cteOptimizationEnabled
    if ((!lqbOn && !cteOn) || plan.queries.isEmpty) return plan
    val last = plan.queries.last
    val sql = last.sql
    val trimmed = sql.trim

    // Split off any leading "CREATE ... AS " so we transform only the SELECT/WITH body.
    val (prefix, selectPart): (String, String) =
      if (trimmed.regionMatches(true, 0, "SELECT", 0, 6) ||
        trimmed.regionMatches(true, 0, "WITH", 0, 4)) {
        ("", sql)
      } else {
        "(?is)\\bAS\\s+(SELECT|WITH)\\b".r.findFirstMatchIn(sql) match {
          case Some(m) => (sql.substring(0, m.start(1)), sql.substring(m.start(1)))
          case None => return plan
        }
      }

    var creates = Seq.empty[String]
    var drops = Seq.empty[String]
    var newSelect = selectPart
    if (lqbOn) {
      val charBound = math.max(4000, session.conn.largeQueryBreakdownBound * 100)
      val r = LargeQueryBreakdown.breakdown(newSelect, charBound)
      creates = r.createSqls
      drops = r.dropSqls
      newSelect = r.newSql
    }
    if (cteOn) {
      newSelect = RepeatedSubqueryElimination.rewrite(newSelect)
    }
    if (creates.isEmpty && newSelect == selectPart) return plan

    val createQueries = creates.map(s => Query(s, isDDLOnTempObject = true))
    val dropQueries = drops.map(s => Query(s, isDDLOnTempObject = true))
    val newLast =
      new Query(prefix + newSelect, last.queryIdPlaceHolder, last.isDDLOnTempObject, last.params)
    SnowflakePlan(
      plan.queries.dropRight(1) ++ createQueries :+ newLast,
      plan.schemaQuery,
      plan.postActions ++ dropQueries,
      session,
      plan.sourcePlan,
      plan.supportAsyncMode)
  }
}
