package com.snowflake.snowpark

import com.snowflake.snowpark.functions._

/**
 * Distilled Snowpark Scala pipeline reproducing the LGDCCF plan pathologies (SNOW-3894042). Mirrors
 * the customer's actual Snowpark Scala patterns:
 *
 * P1: Duplicate retail vs _nr branches (same computation, separate subtrees). P2: Wide multi-join
 * chain + trailing .distinct() (the 185-join fused INSERT). P3: Per-stage .cacheResult() ->
 * SNOWPARK_TEMP_TABLE materialization. P4: Many rolling-window features over the same
 * partition/order (repeated sort).
 */
object LgdccfPipeline {

  case class Config(
      numHorizons: Int = 8,
      numFinalJoins: Int = 6,
      dedupNrBranch: Boolean = false,
      materializeStages: Boolean = false)

  val SMOKE_CONFIG: Config = Config()
  val MEDFAST_CONFIG: Config = Config(numHorizons = 12, numFinalJoins = 10)
  val MEDIUM_CONFIG: Config = Config(numHorizons = 18, numFinalJoins = 12)
  val LARGE_CONFIG: Config = Config(numHorizons = 40, numFinalJoins = 24)

  private val CANONICAL_HORIZONS: Seq[Int] =
    Seq(1, 2, 3, 4, 6, 9, 12, 18, 24, 36, 42, 48, 54, 60, 66, 72, 78, 84, 90, 96, 102, 108, 114,
      120, 126, 132, 138, 144, 150, 156, 162, 168, 174, 180, 186, 192, 198, 204, 210, 216)

  private def horizons(n: Int): Seq[Int] = CANONICAL_HORIZONS.take(n)

  /**
   * P4: Rolling features over N horizons. Each horizon adds its own Window scan over the SAME
   * partitionBy(customer_id).orderBy(month_index), faithfully reproducing the FactorsWindow pattern
   * that generates 300+ windows.
   */
  def rollingFeatures(base: DataFrame, numHorizons: Int): DataFrame = {
    val w = Window.partitionBy(col("customer_id")).orderBy(col("month_index"))
    horizons(numHorizons).foldLeft(base) { (df, h) =>
      val wr = w.rowsBetween(-h, Window.currentRow)
      df.withColumn(s"util_avg_$h", avg(col("util_amt")).over(wr))
        .withColumn(s"util_max_$h", max(col("util_amt")).over(wr))
        .withColumn(s"util_chg_$h", col("util_amt") - lag(col("util_amt"), h).over(w))
    }
  }

  /**
   * Account rollup: account_month aggregated to customer x month. Fanout (account_month) -> groupBy
   * regroup = the join-fanout-then-aggregate pattern that inflates rows before collapsing them.
   */
  def accountRollup(session: Session): DataFrame = {
    session
      .table("lgdccf_account_month")
      .groupBy(col("customer_id"), col("month_index"))
      .agg(
        sum(col("util_amt")).as("util_amt"),
        sum(col("limit_amt")).as("limit_amt"),
        sum(col("balance_amt")).as("balance_amt"),
        sum(col("arrears_amt")).as("arrears_amt"))
  }

  /**
   * One complete feature branch: spine -> account rollup -> rolling features. Called TWICE for
   * retail and _nr when dedupNrBranch=false (P1 duplication).
   */
  def featureBranch(session: Session, config: Config): DataFrame = {
    val spine = session
      .table("lgdccf_customer")
      .crossJoin(session.table("lgdccf_month_calendar"))
      .select(
        col("customer_id"),
        col("month_index"),
        col("month_end"),
        col("segment"),
        col("industry_code"),
        col("scoring_country"))

    var agg: DataFrame = accountRollup(session)
    if (config.materializeStages) {
      agg = agg.cacheResult()
    }

    val base = spine.join(agg, Seq("customer_id", "month_index"), "left")

    var rolling: DataFrame = rollingFeatures(base, config.numHorizons)
    if (config.materializeStages) {
      rolling = rolling.cacheResult()
    }
    rolling
  }

  /**
   * Full Customer pipeline reproducing P1-P4. Returns the final unexecuted DataFrame.
   */
  def buildCustomerPipeline(session: Session, config: Config): DataFrame = {
    val retail = featureBranch(session, config)

    // P1: the non-retail (_nr) branch.
    // When dedupNrBranch=false, call featureBranch again -> fresh subtree
    // (Snowpark re-inlines the entire computation).
    // When true, reuse the same DataFrame object -> if CTE opt existed, it
    // would collapse them; currently it just generates a self-reference.
    val nr: DataFrame = if (config.dedupNrBranch) retail else featureBranch(session, config)

    val nrSummary =
      nr.select(col("customer_id"), col("month_index"), col("util_avg_1").as("nr_util_avg_1"))
    var df = retail.join(nrSummary, Seq("customer_id", "month_index"), "left")

    // P2: wide join chain + trailing .distinct().
    // Cycle through customer x month dims, adding one uniquely-named measure
    // per join (mirrors the 18 sequential LEFT joins in finalDatasetProjection).
    val dimSpecs: Seq[(String, String)] = Seq(
      ("lgdccf_customer_segmentation", "subsegment"),
      ("lgdccf_exposure", "exposure_amt"),
      ("lgdccf_collateral", "collateral_val"),
      ("lgdccf_monthly_default_flag", "default_flag"))
    for (j <- 0 until config.numFinalJoins) {
      val (tbl, measure) = dimSpecs(j % dimSpecs.size)
      val dim =
        session.table(tbl).select(col("customer_id"), col("month_index"), col(measure).as(s"m_$j"))
      df = df.join(dim, Seq("customer_id", "month_index"), "left")
    }

    df.distinct()
  }

  /**
   * Account pipeline: account-grain rolling features + customer rollup. Reproduces the 8-13x
   * spill:scan Account shape.
   */
  def buildAccountPipeline(session: Session, config: Config): DataFrame = {
    val am = session.table("lgdccf_account_month")
    val w = Window.partitionBy(col("account_id")).orderBy(col("month_index"))

    var df: DataFrame = horizons(config.numHorizons).foldLeft(am: DataFrame) { (acc, h) =>
      val wr = w.rowsBetween(-h, Window.currentRow)
      acc
        .withColumn(s"acc_util_avg_$h", avg(col("util_amt")).over(wr))
        .withColumn(s"acc_bal_max_$h", max(col("balance_amt")).over(wr))
    }
    if (config.materializeStages) {
      df = df.cacheResult()
    }

    // Top-account rollup to customer x month, then join back (fanout -> regroup).
    val top = df
      .groupBy(col("customer_id"), col("month_index"))
      .agg(sum(col("util_amt")).as("top_util_amt"), max(col("acc_util_avg_1")).as("top_util_avg_1"))

    var out = df.join(top, Seq("customer_id", "month_index"), "left")
    for (j <- 0 until config.numFinalJoins) {
      val (tbl, measure) =
        Seq(("lgdccf_customer_segmentation", "subsegment"), ("lgdccf_exposure", "exposure_amt"))(
          j % 2)
      val dim =
        session.table(tbl).select(col("customer_id"), col("month_index"), col(measure).as(s"am_$j"))
      out = out.join(dim, Seq("customer_id", "month_index"), "left")
    }
    out.distinct()
  }

  /**
   * Reproduces RC3 (serial execution) + RC4 (redundant/serial temp materializations): K INDEPENDENT
   * feature branches, each eagerly `.cacheResult()`-ed (one CTAS per branch, executed serially in
   * driver order), then combined. Mirrors the customer's ~35 independent eager checkpoints. The
   * branches read DISJOINT account_id slices so they are genuinely independent (parallelizable).
   */
  def manyCheckpoints(session: Session, k: Int, config: Config): DataFrame = {
    val branches: Seq[DataFrame] = (0 until k).map { i =>
      session
        .table("lgdccf_account_month")
        .filter((col("account_id") % k) === lit(i))
        .groupBy(col("customer_id"), col("month_index"))
        .agg(
          sum(col("util_amt")).as(s"util_$i"),
          max(col("balance_amt")).as(s"bal_$i"),
          avg(col("arrears_amt")).as(s"arr_$i"))
    }
    // RC3/RC4: baseline materializes each branch with an eager, serial `cacheResult()`. The fix
    // (parallel toggle on) submits them concurrently via `cacheResult(async = true)`; the pending
    // async CTAS are drained automatically at the terminal action (saveAsTable).
    val cached: Seq[DataFrame] =
      if (session.conn.parallelPlanExecution) branches.map(_.cacheResult(async = true))
      else branches.map(_.cacheResult())
    cached.reduce((a, b) => a.join(b, Seq("customer_id", "month_index"), "outer"))
  }
}
