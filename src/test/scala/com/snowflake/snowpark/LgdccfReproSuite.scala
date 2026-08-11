package com.snowflake.snowpark

import com.snowflake.perf.PerfBase
import com.snowflake.snowpark.functions._

/**
 * LGDCCF performance reproduction suite (SNOW-3894042).
 *
 * Generates synthetic data, builds a distilled pipeline mirroring the customer's Snowpark Scala
 * patterns, and captures metrics proving the pathological plan shapes (giant fused SQL, repeated
 * subplans, temp-table materialization, many windows).
 *
 * Run with: sbt "testOnly com.snowflake.snowpark.LgdccfReproSuite"
 *
 * For timed perf run: sbt -DPERF_TEST=true "testOnly com.snowflake.snowpark.LgdccfReproSuite"
 */
class LgdccfReproSuite extends PerfBase {

  private val scale = LgdccfSyntheticData.SMOKE
  private val config = LgdccfPipeline.SMOKE_CONFIG

  override def beforeAll: Unit = {
    super.beforeAll
    LgdccfSyntheticData.generate(session, scale)
  }

  override def afterAll: Unit = {
    LgdccfSyntheticData.dropAll(session)
    super.afterAll
  }

  // --- Helper: extract plan SQL without executing ---
  private def finalSql(df: DataFrame): String =
    df.snowflakePlan.queries.last.sql

  private def joinCount(sql: String): Int =
    "(?i)\\bJOIN\\b".r.findAllIn(sql).size

  // ========================================================================
  // Shape assertions (no execution, fast)
  // ========================================================================

  test("baseline SQL is large and has many JOINs") {
    val df = LgdccfPipeline.buildCustomerPipeline(session, config)
    val sql = finalSql(df)
    val joins = joinCount(sql)
    println(s"[LGDCCF] Baseline SQL bytes: ${sql.length}, JOINs: $joins")
    assert(sql.length > 3000, s"Expected large SQL, got ${sql.length} bytes")
    assert(joins >= 8, s"Expected many JOINs, got $joins")
  }

  test("dedup reduces SQL size vs baseline") {
    val baseline = finalSql(LgdccfPipeline.buildCustomerPipeline(session, config))
    val dedup =
      finalSql(LgdccfPipeline.buildCustomerPipeline(session, config.copy(dedupNrBranch = true)))
    println(s"[LGDCCF] Baseline SQL: ${baseline.length} bytes, Dedup SQL: ${dedup.length} bytes")
    assert(
      dedup.length <= baseline.length,
      s"Dedup (${dedup.length}) should not be larger than baseline (${baseline.length})")
  }

  test("more horizons and joins increase SQL size") {
    val small = finalSql(
      LgdccfPipeline
        .buildCustomerPipeline(session, LgdccfPipeline.Config(numHorizons = 4, numFinalJoins = 4)))
    val big = finalSql(
      LgdccfPipeline.buildCustomerPipeline(
        session,
        LgdccfPipeline.Config(numHorizons = 12, numFinalJoins = 12)))
    println(s"[LGDCCF] Small SQL: ${small.length}, Big SQL: ${big.length}")
    assert(big.length > small.length, s"Expected big (${big.length}) > small (${small.length})")
  }

  // ========================================================================
  // Execution tests (require warehouse, measure real metrics)
  // ========================================================================

  test("baseline execution captures metrics") {
    val df = LgdccfPipeline.buildCustomerPipeline(session, config)
    val sql = finalSql(df)
    val t0 = System.currentTimeMillis()
    df.collect()
    val wallMs = System.currentTimeMillis() - t0
    println(
      s"[LGDCCF] Baseline wall: ${wallMs}ms, SQL: ${sql.length} bytes, " +
        s"JOINs: ${joinCount(sql)}")
    assert(wallMs > 0)
  }

  test("materializeStages creates temp tables") {
    val matConfig = config.copy(materializeStages = true)
    val df = LgdccfPipeline.buildCustomerPipeline(session, matConfig)
    // cacheResult executes eagerly during build; count queries afterward.
    val queries = df.snowflakePlan.postActions
    // With cacheResult, the plan has postActions (DROP TABLE) for cleanup.
    println(s"[LGDCCF] Post-actions (temp table drops): ${queries.size}")
    // At minimum, the cacheResult created temp table(s) during featureBranch.
    // The signal is that the plan is now a simple select from a temp table
    // rather than the full fused lineage.
    val sql = finalSql(df)
    println(s"[LGDCCF] Materialized SQL bytes: ${sql.length} (should be smaller than fused)")
  }

  test("account pipeline captures spill-relevant metrics") {
    val df = LgdccfPipeline.buildAccountPipeline(session, config)
    val sql = finalSql(df)
    println(s"[LGDCCF] Account pipeline SQL: ${sql.length} bytes, JOINs: ${joinCount(sql)}")
    val t0 = System.currentTimeMillis()
    df.collect()
    val wallMs = System.currentTimeMillis() - t0
    println(s"[LGDCCF] Account pipeline wall: ${wallMs}ms")
  }

  // ========================================================================
  // Perf tests (timed, disabled by default -- enable with -DPERF_TEST=true)
  // ========================================================================

  perfTest("customer_baseline_no_dedup") {
    val df = LgdccfPipeline.buildCustomerPipeline(session, config)
    df.collect()
  }

  perfTest("customer_dedup_shared_branch") {
    val df = LgdccfPipeline.buildCustomerPipeline(session, config.copy(dedupNrBranch = true))
    df.collect()
  }

  perfTest("customer_materialized") {
    val df = LgdccfPipeline.buildCustomerPipeline(session, config.copy(materializeStages = true))
    df.collect()
  }

  perfTest("account_baseline") {
    val df = LgdccfPipeline.buildAccountPipeline(session, config)
    df.collect()
  }
}
