package com.snowflake.snowpark

import com.snowflake.snowpark.functions._

/**
 * Scalable, deterministic synthetic data generator for the LGDCCF perf reproduction (SNOW-3894042).
 *
 * All measures are derived from HASH (not RNG) for reproducibility. Tables are TRANSIENT and
 * created in the session's current schema.
 */
object LgdccfSyntheticData {

  case class Scale(numCustomers: Int, numMonths: Int, accountsPerCustomer: Int)

  val SMOKE: Scale = Scale(numCustomers = 2000, numMonths = 6, accountsPerCustomer = 3)
  // MEDFAST: fast iteration proxy - still triggers the pathologies but finishes in ~1-3 min.
  val MEDFAST: Scale = Scale(numCustomers = 150000, numMonths = 12, accountsPerCustomer = 4)
  val MEDIUM: Scale = Scale(numCustomers = 500000, numMonths = 12, accountsPerCustomer = 5)
  val LARGE: Scale = Scale(numCustomers = 8000000, numMonths = 18, accountsPerCustomer = 8)

  val ALL_TABLES: Seq[String] = Seq(
    "lgdccf_customer",
    "lgdccf_month_calendar",
    "lgdccf_account_customer",
    "lgdccf_account_month",
    "lgdccf_customer_segmentation",
    "lgdccf_exposure",
    "lgdccf_collateral",
    "lgdccf_monthly_default_flag")

  def generate(session: Session, scale: Scale): Unit = {
    val s = scale

    session
      .sql(s"""create or replace transient table lgdccf_customer as
         |select
         |  seq8() as customer_id,
         |  mod(abs(hash(seq8())), 5) + 1 as segment,
         |  mod(abs(hash(seq8(), 'ind')), 20) + 1 as industry_code,
         |  mod(abs(hash(seq8(), 'cty')), 10) + 1 as scoring_country
         |from table(generator(rowcount => ${s.numCustomers}))""".stripMargin)
      .collect()

    session
      .sql(s"""create or replace transient table lgdccf_month_calendar as
         |select
         |  seq8() as month_index,
         |  last_day(dateadd(month, -seq8(), current_date())) as month_end
         |from table(generator(rowcount => ${s.numMonths}))""".stripMargin)
      .collect()

    val numAccounts = s.numCustomers.toLong * s.accountsPerCustomer
    session
      .sql(s"""create or replace transient table lgdccf_account_customer as
         |select
         |  seq8() as account_id,
         |  mod(seq8(), ${s.numCustomers}) as customer_id
         |from table(generator(rowcount => $numAccounts))""".stripMargin)
      .collect()

    session
      .sql(s"""create or replace transient table lgdccf_account_month as
         |select
         |  a.account_id,
         |  a.customer_id,
         |  m.month_index,
         |  m.month_end,
         |  mod(abs(hash(a.account_id, m.month_index, 'util')), 1000) as util_amt,
         |  mod(abs(hash(a.account_id, m.month_index, 'lim')), 4000) + 1000 as limit_amt,
         |  mod(abs(hash(a.account_id, m.month_index, 'bal')), 900) as balance_amt,
         |  mod(abs(hash(a.account_id, m.month_index, 'arr')), 100) as arrears_amt
         |from lgdccf_account_customer a
         |cross join lgdccf_month_calendar m""".stripMargin)
      .collect()

    session
      .sql(s"""create or replace transient table lgdccf_customer_segmentation as
         |select
         |  c.customer_id,
         |  m.month_index,
         |  c.segment,
         |  mod(abs(hash(c.customer_id, m.month_index, 'sub')), 30) + 1 as subsegment
         |from lgdccf_customer c
         |cross join lgdccf_month_calendar m""".stripMargin)
      .collect()

    session
      .sql(s"""create or replace transient table lgdccf_exposure as
         |select
         |  c.customer_id,
         |  m.month_index,
         |  mod(abs(hash(c.customer_id, m.month_index, 'exp')), 100000) as exposure_amt
         |from lgdccf_customer c
         |cross join lgdccf_month_calendar m""".stripMargin)
      .collect()

    session
      .sql(s"""create or replace transient table lgdccf_collateral as
         |select
         |  c.customer_id,
         |  m.month_index,
         |  mod(abs(hash(c.customer_id, m.month_index, 'col')), 50000) as collateral_val
         |from lgdccf_customer c
         |cross join lgdccf_month_calendar m""".stripMargin)
      .collect()

    session
      .sql(s"""create or replace transient table lgdccf_monthly_default_flag as
         |select
         |  c.customer_id,
         |  m.month_index,
         |  case when mod(abs(hash(c.customer_id, m.month_index, 'def')), 100) < 5
         |       then 1 else 0 end as default_flag
         |from lgdccf_customer c
         |cross join lgdccf_month_calendar m""".stripMargin)
      .collect()
  }

  def dropAll(session: Session): Unit = {
    ALL_TABLES.foreach(t => session.sql(s"drop table if exists $t").collect())
  }
}
