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
            session.conn.telemetry.reportSimplifierUsage(queryId, summaryBefore, summaryAfter)
          )
        }
        result
      }
      result.analyzeIfneeded()
      result
    }
  }
}
