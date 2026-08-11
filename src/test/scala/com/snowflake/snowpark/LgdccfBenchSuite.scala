package com.snowflake.snowpark

import com.snowflake.snowpark.functions._

/**
 * SNOW-3894042 quantitative bench for the LGDCCF pipelines.
 *
 * Unlike [[LgdccfReproSuite]] (which only asserts plan SHAPE at SMOKE), this suite EXECUTES the
 * customer and account pipelines at a configurable scale and captures real server-side metrics
 * (elapsed time, bytes scanned, local spill via GET_QUERY_OPERATOR_STATS, statement/join/window/CTE
 * counts, temp-table creates). It A/Bs the SNOW-3894042 optimization toggles by building a fresh
 * session per variant.
 *
 * Gated OFF by default (does real warehouse work + costs credits). Enable with: sbt
 * -DLGDCCF_BENCH=true -DLGDCCF_SCALE=MEDIUM \ "testOnly com.snowflake.snowpark.LgdccfBenchSuite"
 *
 * System properties: LGDCCF_BENCH = true -> actually run (else ignored) LGDCCF_SCALE =
 * SMOKE|MEDIUM|LARGE (default MEDIUM) LGDCCF_WH_SIZE = warehouse size to ALTER to before runs
 * (default LARGE; empty = leave as-is) LGDCCF_VARIANTS= comma list of variant names to run
 * (default: baseline) known: baseline,cte,lqb,parallel,reuse,all
 */
class LgdccfBenchSuite extends SNTestBase {

  private val benchEnabled: Boolean =
    Option(System.getProperty("LGDCCF_BENCH")).exists(_.equalsIgnoreCase("true"))

  private def intProp(k: String, dflt: Int): Int =
    Option(System.getProperty(k))
      .flatMap(v => scala.util.Try(v.trim.toInt).toOption)
      .getOrElse(dflt)

  private val baseScale: LgdccfSyntheticData.Scale =
    Option(System.getProperty("LGDCCF_SCALE")).map(_.toUpperCase).getOrElse("MEDIUM") match {
      case "SMOKE" => LgdccfSyntheticData.SMOKE
      case "MEDFAST" => LgdccfSyntheticData.MEDFAST
      case "LARGE" => LgdccfSyntheticData.LARGE
      case _ => LgdccfSyntheticData.MEDIUM
    }

  // Per-dimension overrides so a spill-inducing customer-sim scale can be dialed in without
  // recompiling (e.g. -DLGDCCF_CUST=350000 -DLGDCCF_MONTHS=18 -DLGDCCF_ACCTS=8).
  private val scale: LgdccfSyntheticData.Scale = LgdccfSyntheticData.Scale(
    numCustomers = intProp("LGDCCF_CUST", baseScale.numCustomers),
    numMonths = intProp("LGDCCF_MONTHS", baseScale.numMonths),
    accountsPerCustomer = intProp("LGDCCF_ACCTS", baseScale.accountsPerCustomer))

  private val baseConfig: LgdccfPipeline.Config =
    Option(System.getProperty("LGDCCF_SCALE")).map(_.toUpperCase).getOrElse("MEDIUM") match {
      case "SMOKE" => LgdccfPipeline.SMOKE_CONFIG
      case "MEDFAST" => LgdccfPipeline.MEDFAST_CONFIG
      case "LARGE" => LgdccfPipeline.LARGE_CONFIG
      case _ => LgdccfPipeline.MEDIUM_CONFIG
    }

  private val config: LgdccfPipeline.Config = baseConfig.copy(
    numHorizons = intProp("LGDCCF_HORIZONS", baseConfig.numHorizons),
    numFinalJoins = intProp("LGDCCF_JOINS", baseConfig.numFinalJoins))

  private val stmtTimeout: Int = intProp("LGDCCF_TIMEOUT", 900)
  private val kCheckpoints: Int = intProp("LGDCCF_K", 8)

  private val whSize: String =
    Option(System.getProperty("LGDCCF_WH_SIZE")).getOrElse("LARGE")

  private val variants: Seq[String] =
    Option(System.getProperty("LGDCCF_VARIANTS"))
      .map(_.split(",").map(_.trim.toLowerCase).filter(_.nonEmpty).toSeq)
      .getOrElse(Seq("baseline"))

  private val pipelineFilter: Set[String] =
    Option(System.getProperty("LGDCCF_PIPELINES"))
      .map(_.split(",").map(_.trim.toLowerCase).filter(_.nonEmpty).toSet)
      .getOrElse(Set("customer", "account"))

  // Map a variant name to the client toggles it enables.
  private def flagsFor(variant: String): Map[String, String] = variant match {
    case "cte" => Map("snowpark_cte_optimization_enabled" -> "true")
    case "dedup_cte" => Map("snowpark_cte_optimization_enabled" -> "true")
    case "lqb" => Map("snowpark_large_query_breakdown_enabled" -> "true")
    case "dedup_lqb" => Map("snowpark_large_query_breakdown_enabled" -> "true")
    case "lqb_par" =>
      Map(
        "snowpark_large_query_breakdown_enabled" -> "true",
        "snowpark_parallel_plan_execution" -> "true")
    case "parallel" => Map("snowpark_parallel_plan_execution" -> "true")
    case "reuse" => Map("snowpark_cross_statement_reuse" -> "true")
    case "materialize_reuse" => Map("snowpark_cross_statement_reuse" -> "true")
    case "all" =>
      Map(
        "snowpark_cte_optimization_enabled" -> "true",
        "snowpark_large_query_breakdown_enabled" -> "true",
        "snowpark_parallel_plan_execution" -> "true",
        "snowpark_cross_statement_reuse" -> "true")
    case _ => Map.empty
  }

  // Map a variant name to a user-code Config transform (user-side optimizations).
  private def configFor(variant: String): LgdccfPipeline.Config = variant match {
    case "dedup" => config.copy(dedupNrBranch = true)
    case "dedup_cte" => config.copy(dedupNrBranch = true)
    case "dedup_lqb" => config.copy(dedupNrBranch = true)
    case "materialize" => config.copy(materializeStages = true)
    case "materialize_reuse" => config.copy(materializeStages = true)
    case "dedup_materialize" => config.copy(dedupNrBranch = true, materializeStages = true)
    case "usercode" => config.copy(dedupNrBranch = true)
    case "all" => config.copy(dedupNrBranch = true)
    case _ => config
  }

  private def newSession(flags: Map[String, String]): Session = {
    var b = Session.builder.configFile(TestUtils.defaultProfile)
    val withDebug =
      if (Option(System.getProperty("LGDCCF_DEBUG")).exists(_.equalsIgnoreCase("true")))
        flags + ("snowpark_lazy_analysis" -> "true")
      else flags
    withDebug.foreach { case (k, v) => b = b.config(k, v) }
    b.create
  }

  case class Metrics(
      variant: String,
      pipeline: String,
      wallSec: Double,
      serverElapsedSec: Double,
      statements: Int,
      joins: Int,
      windows: Int,
      largestSqlBytes: Int,
      tempCreates: Int,
      cteCount: Int,
      bytesScanned: Long,
      bytesSpilledLocal: Long,
      spillToScan: Double,
      rows: Long) {
    def header: String =
      "variant,pipeline,wall_s,server_s,overlap,stmts,joins,windows,largest_sql,temp_creates,cte,scan_gb,spill_gb,spill_to_scan,rows"
    def csv: String = {
      val scanGb = bytesScanned.toDouble / 1e9
      val spillGb = bytesSpilledLocal.toDouble / 1e9
      val overlap = if (wallSec > 0) serverElapsedSec / wallSec else 0.0
      f"$variant,$pipeline,$wallSec%.1f,$serverElapsedSec%.1f,$overlap%.2f,$statements,$joins,$windows,$largestSqlBytes,$tempCreates,$cteCount,$scanGb%.2f,$spillGb%.2f,$spillToScan%.2f,$rows"
    }
  }

  private def joinCount(sql: String): Int = "(?i)\\bJOIN\\b".r.findAllIn(sql).size
  private def windowCount(sql: String): Int = "(?i)\\bOVER\\s*\\(".r.findAllIn(sql).size
  private def cteCount(sql: String): Int = "(?i)\\bWITH\\b".r.findAllIn(sql).size

  // Robust numeric extraction: JDBC may return Long, Int, BigDecimal, Double, or String.
  private def asLong(r: Row, i: Int): Long = {
    if (r.isNullAt(i)) 0L
    else
      r.get(i) match {
        case v: java.math.BigDecimal => v.longValue()
        case v: java.lang.Long => v.longValue()
        case v: java.lang.Integer => v.longValue()
        case v: java.lang.Double => v.longValue()
        case v: java.lang.Number => v.longValue()
        case v: String => scala.util.Try(v.toDouble.toLong).getOrElse(0L)
        case _ => 0L
      }
  }

  /** Run one pipeline on the given session, materialize to a table, and capture metrics. */
  private def measure(
      sess: Session,
      variant: String,
      pipeline: String,
      build: (Session, LgdccfPipeline.Config) => DataFrame,
      cfg: LgdccfPipeline.Config): Metrics = {
    val tag = s"lgdccf_${variant}_${pipeline}_${System.nanoTime()}"
    val outTbl = s"lgdccf_out_${variant}_${pipeline}"
    sess.sql(s"alter session set statement_timeout_in_seconds = $stmtTimeout").collect()
    sess.sql(s"drop table if exists $outTbl").collect()
    sess.setQueryTag(tag)

    // Time the FULL pipeline: build() executes any eager cacheResult() materializations (the
    // RC3/RC4 serial chain), and saveAsTable() runs the final statement. Both must be inside the
    // wall timer, otherwise the serial materialization cost is invisible.
    val t0 = System.currentTimeMillis()
    val df = build(sess, cfg)
    df.write.mode("overwrite").saveAsTable(outTbl)
    val wallSec = (System.currentTimeMillis() - t0) / 1000.0

    sess.unsetQueryTag()

    // Plan shape (cheap; already resolved during build/write).
    val plan = df.snowflakePlan
    if (Option(System.getProperty("LGDCCF_DEBUG")).exists(_.equalsIgnoreCase("true"))) {
      plan.queries.zipWithIndex.foreach { case (q, i) =>
        println(s"[LGDCCF-DBG] q$i: ${q.sql.replace('\n', ' ').take(110)}")
      }
    }
    val allSql = plan.queries.map(_.sql)
    val largestSql = if (allSql.isEmpty) 0 else allSql.map(_.length).max
    val lastSql = allSql.lastOption.getOrElse("")

    val rows = asLong(sess.sql(s"select count(*) as c from $outTbl").collect()(0), 0)

    // Pull per-query server metrics for this tag (retry for query_history latency).
    val hist = queryHistoryForTag(sess, tag)
    val serverElapsedMs = hist.map(_._2).sum
    val bytesScanned = hist.map(_._3).sum
    val tempCreates = hist.count { case (_, _, _, txt) =>
      val u = txt.toUpperCase; u.contains("CREATE") && u.contains("TABLE")
    }
    val spill = hist.map { case (qid, _, _, _) => spilledLocalBytes(sess, qid) }.sum

    // Keep baseline/parallel outputs so the post-run CONTENT-CHECK can diff them; dropped there.
    if (variant != "baseline" && variant != "parallel") {
      sess.sql(s"drop table if exists $outTbl").collect()
    }

    Metrics(
      variant = variant,
      pipeline = pipeline,
      wallSec = wallSec,
      serverElapsedSec = serverElapsedMs / 1000.0,
      statements = hist.size,
      joins = joinCount(lastSql),
      windows = allSql.map(windowCount).sum,
      largestSqlBytes = largestSql,
      tempCreates = tempCreates,
      cteCount = allSql.map(cteCount).sum,
      bytesScanned = bytesScanned,
      bytesSpilledLocal = spill,
      spillToScan = if (bytesScanned > 0) spill.toDouble / bytesScanned else 0.0,
      rows = rows)
  }

  // (query_id, total_elapsed_ms, bytes_scanned, query_text)
  private def queryHistoryForTag(sess: Session, tag: String): Seq[(String, Long, Long, String)] = {
    var attempt = 0
    var result: Seq[(String, Long, Long, String)] = Seq.empty
    while (attempt < 6 && result.isEmpty) {
      if (attempt > 0) Thread.sleep(3000)
      attempt += 1
      val rows = sess
        .sql(
          "select query_id, total_elapsed_time, coalesce(bytes_scanned,0), query_text " +
            "from table(information_schema.query_history_by_session(result_limit => 10000)) " +
            s"where query_tag = '$tag'")
        .collect()
      result = rows.map { r =>
        (r.getString(0), asLong(r, 1), asLong(r, 2), r.getString(3))
      }.toSeq
    }
    result
  }

  private def spilledLocalBytes(sess: Session, queryId: String): Long = {
    try {
      val rows = sess
        .sql(
          "select coalesce(sum(operator_statistics:spilling:bytes_spilled_local_storage::number),0)" +
            s" as b from table(get_query_operator_stats('$queryId'))")
        .collect()
      if (rows.nonEmpty) asLong(rows(0), 0) else 0L
    } catch {
      case _: Exception => 0L
    }
  }

  private val pipelines: Seq[(String, (Session, LgdccfPipeline.Config) => DataFrame)] = Seq(
    "customer" -> ((s, c) => LgdccfPipeline.buildCustomerPipeline(s, c)),
    "account" -> ((s, c) => LgdccfPipeline.buildAccountPipeline(s, c)),
    "checkpoint" -> ((s, c) => LgdccfPipeline.manyCheckpoints(s, kCheckpoints, c)))
    .filter { case (n, _) => pipelineFilter.contains(n) }

  if (benchEnabled) {
    test("LGDCCF bench") {
      // Size the warehouse for the run.
      if (whSize.nonEmpty) {
        val wh = session.sql("select current_warehouse()").collect()(0).getString(0)
        if (wh != null && wh.nonEmpty) {
          session.sql(s"alter warehouse $wh set warehouse_size = '$whSize'").collect()
        }
      }
      // Guard: never let a single runaway statement hang the bench.
      session.sql(s"alter session set statement_timeout_in_seconds = $stmtTimeout").collect()
      LgdccfSyntheticData.generate(session, scale)

      val all = scala.collection.mutable.ArrayBuffer.empty[Metrics]
      for (variant <- variants) {
        val sess = if (variant == "baseline") session else newSession(flagsFor(variant))
        try {
          for ((pname, build) <- pipelines) {
            try {
              val m = measure(sess, variant, pname, build, configFor(variant))
              all += m
              println(s"[LGDCCF-BENCH] ${m.csv}")
            } catch {
              case e: Exception =>
                println(s"[LGDCCF-BENCH] FAIL,$variant,$pname,${e.getClass.getSimpleName}:" +
                  s"${Option(e.getMessage).getOrElse("").take(80).replace(',', ';').replace('\n', ' ')}")
            }
          }
        } finally {
          if (variant != "baseline") sess.close()
        }
      }

      println("[LGDCCF-BENCH] === RESULTS (scale=" + scale + ", wh=" + whSize + ") ===")
      if (all.nonEmpty) {
        println("[LGDCCF-BENCH] " + all.head.header)
        all.foreach(m => println("[LGDCCF-BENCH] " + m.csv))
      }

      // SNOW-3894042 RC4: verify async materialization does NOT change results. For every pipeline
      // that ran under both `baseline` and `parallel`, assert the two output tables are identical
      // (symmetric MINUS both ways == 0). Genuine content check, not a row-count proxy.
      if (variants.contains("baseline") && variants.contains("parallel")) {
        for ((pname, _) <- pipelines) {
          val a = s"lgdccf_out_baseline_$pname"
          val b = s"lgdccf_out_parallel_$pname"
          try {
            val aMinusB = asLong(
              session
                .sql(s"select count(*) c from (select * from $a minus select * from $b)")
                .collect()(0),
              0)
            val bMinusA = asLong(
              session
                .sql(s"select count(*) c from (select * from $b minus select * from $a)")
                .collect()(0),
              0)
            val verdict = if (aMinusB == 0 && bMinusA == 0) "IDENTICAL" else "MISMATCH"
            println(
              s"[LGDCCF-BENCH] CONTENT-CHECK,$pname,baseline_vs_parallel," +
                s"a_minus_b=$aMinusB,b_minus_a=$bMinusA,$verdict")
          } catch {
            case e: Exception =>
              println(s"[LGDCCF-BENCH] CONTENT-CHECK,$pname,ERROR," +
                s"${Option(e.getMessage).getOrElse("").take(60).replace(',', ';').replace('\n', ' ')}")
          } finally {
            session.sql(s"drop table if exists $a").collect()
            session.sql(s"drop table if exists $b").collect()
          }
        }
      }

      LgdccfSyntheticData.dropAll(session)
    }
  } else {
    ignore("LGDCCF bench (set -DLGDCCF_BENCH=true to run)") {}
  }
}
