package com.snowflake.snowpark

import com.snowflake.snowpark.internal.{Logging, StmtNode}
import com.snowflake.snowpark.proto.ast._

import scala.reflect.ClassTag

/**
 * Represents a lazily-evaluated relational dataset that contains a collection of [[Row]] objects
 * with columns defined by a schema (column name and type).
 *
 * A DataFrame is considered lazy because it encapsulates the computation or query required to
 * produce a relational dataset. The computation is not performed until you call a method that
 * performs an action (e.g. [[collect]]).
 *
 * '''Creating a DataFrame'''
 *
 * You can create a DataFrame in a number of different ways, as shown in the examples below.
 *
 * Example 1: Creating a DataFrame by reading a table.
 * {{{
 *   val dfPrices = session.table("itemsdb.publicschema.prices")
 * }}}
 *
 * Example 2: Creating a DataFrame by reading files from a stage.
 * {{{
 *   val dfCatalog = session.read.csv("@stage/some_dir")
 * }}}
 *
 * Example 3: Creating a DataFrame by specifying a sequence or a range.
 * {{{
 *   val df = session.createDataFrame(Seq((1, "one"), (2, "two")))
 * }}}
 * {{{
 *   val df = session.range(1, 10, 2)
 * }}}
 *
 * Example 4: Create a new DataFrame by applying transformations to other existing DataFrames.
 * {{{
 *   val dfMergedData = dfCatalog.join(dfPrices, dfCatalog("itemId") === dfPrices("ID"))
 * }}}
 *
 * '''Performing operations on a DataFrame'''
 *
 * Broadly, the operations on DataFrame can be divided into two types:
 *
 *   - '''Transformations''' produce a new DataFrame from one or more existing DataFrames. Note that
 *     tranformations are lazy and don't cause the DataFrame to be evaluated. If the API does not
 *     provide a method to express the SQL that you want to use, you can use [[functions.sqlExpr]]
 *     as a workaround.
 *
 *   - '''Actions''' cause the DataFrame to be evaluated. When you call a method that performs an
 *     action, Snowpark sends the SQL query for the DataFrame to the server for evaluation.
 *
 * '''Transforming a DataFrame'''
 *
 * The following examples demonstrate how you can transform a DataFrame.
 *
 * Example 5. Using the [[select(first:com\.snowflake\.snowpark\.Column* select]] method to select
 * the columns that should be in the DataFrame (similar to adding a `SELECT` clause).
 *
 * {{{
 *   // Return a new DataFrame containing the ID and amount columns of the prices table. This is
 *   // equivalent to:
 *   //   SELECT ID, AMOUNT FROM PRICES;
 *   val dfPriceIdsAndAmounts = dfPrices.select(col("ID"), col("amount"))
 * }}}
 *
 * Example 6. Using the [[Column.as]] method to rename a column in a DataFrame (similar to using
 * `SELECT col AS alias`).
 *
 * {{{
 *   // Return a new DataFrame containing the ID column of the prices table as a column named
 *   // itemId. This is equivalent to:
 *   //   SELECT ID AS itemId FROM PRICES;
 *   val dfPriceItemIds = dfPrices.select(col("ID").as("itemId"))
 * }}}
 *
 * Example 7. Using the [[filter]] method to filter data (similar to adding a `WHERE` clause).
 *
 * {{{
 *   // Return a new DataFrame containing the row from the prices table with the ID 1. This is
 *   // equivalent to:
 *   //   SELECT * FROM PRICES WHERE ID = 1;
 *   val dfPrice1 = dfPrices.filter((col("ID") === 1))
 * }}}
 *
 * Example 8. Using the [[sort(first* sort]] method to specify the sort order of the data (similar
 * to adding an `ORDER BY` clause).
 *
 * {{{
 *   // Return a new DataFrame for the prices table with the rows sorted by ID. This is equivalent
 *   // to:
 *   //   SELECT * FROM PRICES ORDER BY ID;
 *   val dfSortedPrices = dfPrices.sort(col("ID"))
 * }}}
 *
 * Example 9. Using the [[groupBy(first:com\.snowflake\.snowpark\.Column* groupBy]] method to return
 * a [[RelationalGroupedDataFrame]] that you can use to group and aggregate results (similar to
 * adding a `GROUP BY` clause).
 *
 * [[RelationalGroupedDataFrame]] provides methods for aggregating results, including:
 *
 *   - [[RelationalGroupedDataFrame.avg(cols* avg]] (equivalent to AVG(column))
 *   - [[RelationalGroupedDataFrame.count count]] (equivalent to COUNT())
 *   - [[RelationalGroupedDataFrame.max(cols* max]] (equivalent to MAX(column))
 *   - [[RelationalGroupedDataFrame.median(cols* median]] (equivalent to MEDIAN(column))
 *   - [[RelationalGroupedDataFrame.min(cols* min]] (equivalent to MIN(column))
 *   - [[RelationalGroupedDataFrame.sum(cols* sum]] (equivalent to SUM(column))
 *
 * {{{
 *   // Return a new DataFrame for the prices table that computes the sum of the prices by
 *   // category. This is equivalent to:
 *   //   SELECT CATEGORY, SUM(AMOUNT) FROM PRICES GROUP BY CATEGORY;
 *   val dfTotalPricePerCategory = dfPrices.groupBy(col("category")).sum(col("amount"))
 * }}}
 *
 * Example 10. Using a [[Window]] to build a [[WindowSpec]] object that you can use for
 * [[https://docs.snowflake.com/en/user-guide/functions-window-using.html windowing functions]]
 * (similar to using '<function> OVER ... PARTITION BY ... ORDER BY').
 *
 * {{{
 *   // Define a window that partitions prices by category and sorts the prices by date within the
 *   // partition.
 *   val window = Window.partitionBy(col("category")).orderBy(col("price_date"))
 *   // Calculate the running sum of prices over this window. This is equivalent to:
 *   //   SELECT CATEGORY, PRICE_DATE, SUM(AMOUNT) OVER
 *   //       (PARTITION BY CATEGORY ORDER BY PRICE_DATE)
 *   //       FROM PRICES ORDER BY PRICE_DATE;
 *   val dfCumulativePrices = dfPrices.select(
 *       col("category"), col("price_date"),
 *       sum(col("amount")).over(window)).sort(col("price_date"))
 * }}}
 *
 * '''Performing an action on a DataFrame'''
 *
 * The following examples demonstrate how you can perform an action on a DataFrame.
 *
 * Example 11: Performing a query and returning an array of Rows.
 * {{{
 *   val results = dfPrices.collect()
 * }}}
 *
 * Example 12: Performing a query and print the results.
 * {{{
 *   dfPrices.show()
 * }}}
 *
 * @groupname basic Basic DataFrame Functions
 * @groupname actions Actions
 * @groupname transform Transformations
 * @since 0.1.0
 */
class DataFrame private[snowpark] (override private[snowpark] val varId: VarId)
    extends StmtNode
    with Logging {

  // todo: verify whether this function is still needed
  // it is required by self join in v1
  /**
   * Returns a clone of this DataFrame.
   *
   * @group basic
   * @since 0.4.0
   * @return
   *   A [[DataFrame]]
   */
  override def clone: DataFrame = new DataFrame(varId)

  // todo: SNOW-1975241
  /**
   * Returns the definition of the columns in this DataFrame (the "relational schema" for the
   * DataFrame).
   *
   * @group basic
   * @since 0.1.0
   * @return
   *   [[com.snowflake.snowpark.types.StructType]]
   */
  lazy val schema: StructType = null

  //  /**
  //   * Caches the content of this DataFrame to create a new cached DataFrame.
  //   *
  //   * All subsequent operations on the returned cached
  //   DataFrame are performed on the cached data and
  //   * have no effect on the original DataFrame.
  //   *
  //   * @since 0.4.0
  //   * @group actions
  //   * @return
  //   *   A [[HasCachedResult]]
  //   */
  //  def cacheResult(): HasCachedResult = null

  /**
   * Prints the list of queries that will be executed to evaluate this DataFrame. Prints the query
   * execution plan if only one SELECT/DML/DDL statement will be executed.
   *
   * For more information about the query execution plan, see the
   * [[https://docs.snowflake.com/en/sql-reference/sql/explain.html EXPLAIN]] command.
   *
   * @since 0.1.0
   * @group basic
   */
  def explain(): Unit = {
    // scalastyle:off println
    println(explainString)
    // scalastyle:on println
  }

  private[snowpark] def explainString: String = null

  /**
   * Creates a new DataFrame containing the columns with the specified names.
   *
   * You can use this method to assign column names when constructing a DataFrame. For example:
   *
   * For example:
   *
   * {{{
   *     var df = session.createDataFrame(Seq((1, "a")).toDF(Seq("a", "b"))
   * }}}
   *
   * This returns a DataFrame containing the following:
   *
   * {{{
   *     -------------
   *     |"A"  |"B"  |
   *     -------------
   *     |1    |2    |
   *     |3    |4    |
   *     -------------
   * }}}
   *
   * if you imported [[Session.implicits <session_var>.implicits._]], you can use the following
   * syntax to create the DataFrame from a `Seq` and call `toDF` to assign column names to the
   * returned DataFrame:
   *
   * {{{
   *     import mysession.implicits_
   *     var df = Seq((1, 2), (3, 4)).toDF(Seq("a", "b"))
   * }}}
   *
   * The number of column names that you pass in must match the number of columns in the current
   * DataFrame.
   *
   * @group basic
   * @since 0.1.0
   * @param first
   *   The name of the first column.
   * @param remaining
   *   A list of the rest of the column names.
   * @return
   *   A [[DataFrame]]
   */
  def toDF(first: String, remaining: String*): DataFrame = null

  /**
   * Creates a new DataFrame containing the data in the current DataFrame but in columns with the
   * specified names.
   *
   * You can use this method to assign column names when constructing a DataFrame. For example:
   *
   * For example:
   *
   * {{{
   *     var df = session.createDataFrame(Seq((1, 2), (3, 4))).toDF(Seq("a", "b"))
   * }}}
   *
   * This returns a DataFrame containing the following:
   *
   * {{{
   *     -------------
   *     |"A"  |"B"  |
   *     -------------
   *     |1    |2    |
   *     |3    |4    |
   *     -------------
   * }}}
   *
   * If you imported [[Session.implicits <session_var>.implicits._]], you can use the following
   * syntax to create the DataFrame from a `Seq` and call `toDF` to assign column names to the
   * returned DataFrame:
   *
   * {{{
   *     import mysession.implicits_
   *     var df = Seq((1, 2), (3, 4)).toDF(Seq("a", "b"))
   * }}}
   *
   * The number of column names that you pass in must match the number of columns in the current
   * DataFrame.
   *
   * @group basic
   * @since 0.2.0
   * @param colNames
   *   A list of column names.
   * @return
   *   A [[DataFrame]]
   */
  def toDF(colNames: Seq[String]): DataFrame = null

  /**
   * Creates a new DataFrame containing the data in the current DataFrame but in columns with the
   * specified names.
   *
   * You can use this method to assign column names when constructing a DataFrame. For example:
   *
   * For example:
   *
   * {{{
   *     val df = session.createDataFrame(Seq((1, "a"))).toDF(Array("a", "b"))
   * }}}
   *
   * This returns a DataFrame containing the following:
   *
   * {{{
   *     -------------
   *     |"A"  |"B"  |
   *     -------------
   *     |1    |2    |
   *     |3    |4    |
   *     -------------
   * }}}
   *
   * If you imported [[Session.implicits <session_var>.implicits._]], you can use the following
   * syntax to create the DataFrame from a `Seq` and call `toDF` to assign column names to the
   * returned DataFrame:
   *
   * {{{
   *     import mysession.implicits_
   *     var df = Seq((1, 2), (3, 4)).toDF(Array("a", "b"))
   * }}}
   *
   * The number of column names that you pass in must match the number of columns in the current
   * DataFrame.
   *
   * @group basic
   * @since 0.7.0
   * @param colNames
   *   An array of column names.
   * @return
   *   A [[DataFrame]]
   */
  def toDF(colNames: Array[String]): DataFrame = null

  /**
   * Sorts a DataFrame by the specified expressions (similar to ORDER BY in SQL).
   *
   * For example:
   *
   * {{{
   *   val dfSorted = df.sort($"colA", $"colB".asc)
   * }}}
   *
   * @group transform
   * @since 0.1.0
   * @param first
   *   The first Column expression for sorting the DataFrame.
   * @param remaining
   *   Additional Column expressions for sorting the DataFrame.
   * @return
   *   A [[DataFrame]]
   */
  def sort(first: Column, remaining: Column*): DataFrame = null

  /**
   * Sorts a DataFrame by the specified expressions (similar to ORDER BY in SQL).
   *
   * For example:
   * {{{
   *   val dfSorted = df.sort(Seq($"colA", $"colB".desc))
   * }}}
   *
   * @group transform
   * @since 0.2.0
   * @param sortExprs
   *   A list of Column expressions for sorting the DataFrame.
   * @return
   *   A [[DataFrame]]
   */
  def sort(sortExprs: Seq[Column]): DataFrame = null

  /**
   * Sorts a DataFrame by the specified expressions (similar to ORDER BY in SQL).
   *
   * For example:
   *
   * {{{
   *   val dfSorted = df.sort(Array(col("col1").asc, col("col2").desc, col("col3")))
   * }}}
   *
   * @group transform
   * @since 0.7.0
   * @param sortExprs
   *   An array of Column expressions for sorting the DataFrame.
   * @return
   *   A [[DataFrame]]
   */
  def sort(sortExprs: Array[Column]): DataFrame = sort(sortExprs.toSeq)

  /**
   * Returns a reference to a column in the DataFrame. This method is identical to
   * [[col DataFrame.col]].
   *
   * @group transform
   * @since 0.1.0
   * @param colName
   *   The name of the column.
   * @return
   *   A [[Column]]
   */
  def apply(colName: String): Column = col(colName)

  /**
   * Returns a reference to a column in the DataFrame.
   *
   * @group transform
   * @since 0.1.0
   * @param colName
   *   The name of the column.
   * @return
   *   A [[Column]]
   */
  def col(colName: String): Column = null

  /**
   * Returns the current DataFrame aliased as the input alias name.
   *
   * For example:
   *
   * {{{
   *   val df2 = df.alias("A")
   *   df2.select(df2.col("A.num"))
   * }}}
   *
   * @group basic
   * @since 1.10.0
   * @param alias
   *   The alias name of the dataframe
   * @return
   *   a [[DataFrame]]
   */
  def alias(alias: String): DataFrame = null

  /**
   * Returns a new DataFrame with the specified Column expressions as output (similar to SELECT in
   * SQL). Only the Columns specified as arguments will be present in the resulting DataFrame.
   *
   * You can use any Column expression.
   *
   * For example:
   *
   * {{{
   *   val dfSelected = df.select($"col1", substring($"col2", 0, 10), df("col3") + df("col4"))
   * }}}
   *
   * @group transform
   * @since 0.1.0
   * @param first
   *   The expression for the first column to return.
   * @param remaining
   *   A list of expressions for the additional columns to return.
   * @return
   *   A [[DataFrame]]
   */
  def select(first: Column, remaining: Column*): DataFrame = null

  /**
   * Returns a new DataFrame with the specified Column expressions as output (similar to SELECT in
   * SQL). Only the Columns specified as arguments will be present in the resulting DataFrame.
   *
   * You can use any Column expression.
   *
   * For example:
   * {{{
   *   val dfSelected = df.select(Seq($"col1", substring($"col2", 0, 10), df("col3") + df("col4")))
   * }}}
   *
   * @group transform
   * @since 0.2.0
   * @param columns
   *   A list of expressions for the columns to return.
   * @return
   *   A [[DataFrame]]
   */
  def select[T: ClassTag](columns: Seq[Column]): DataFrame = null

  /**
   * Returns a new DataFrame with the specified Column expressions as output (similar to SELECT in
   * SQL). Only the Columns specified as arguments will be present in the resulting DataFrame.
   *
   * You can use any Column expression.
   *
   * For example:
   *
   * {{{
   *   val dfSelected =
   *     df.select(Array(df.col("col1"), lit("abc"), df.col("col1") + df.col("col2")))
   * }}}
   *
   * @group transform
   * @since 0.7.0
   * @param columns
   *   An array of expressions for the columns to return.
   * @return
   *   A [[DataFrame]]
   */
  def select(columns: Array[Column]): DataFrame = null

  /**
   * Returns a new DataFrame with a subset of named columns (similar to SELECT in SQL).
   *
   * For example:
   *
   * {{{
   *   val dfSelected = df.select("col1", "col2", "col3")
   * }}}
   *
   * @group transform
   * @since 0.1.0
   * @param first
   *   The name of the first column to return.
   * @param remaining
   *   A list of the names of the additional columns to return.
   * @return
   *   A [[DataFrame]]
   */
  def select(first: String, remaining: String*): DataFrame = null

  /**
   * Returns a new DataFrame with a subset of named columns (similar to SELECT in SQL).
   *
   * For example:
   * {{{
   *   val dfSelected = df.select(Seq("col1", "col2", "col3"))
   * }}}
   *
   * @group transform
   * @since 0.2.0
   * @param columns
   *   A list of the names of columns to return.
   * @return
   *   A [[DataFrame]]
   */
  def select(columns: Seq[String]): DataFrame = null

  /**
   * Returns a new DataFrame with a subset of named columns (similar to SELECT in SQL).
   *
   * For example:
   *
   * {{{
   *   val dfSelected = df.select(Array("col1", "col2"))
   * }}}
   *
   * @group transform
   * @since 0.7.0
   * @param columns
   *   An array of the names of columns to return.
   * @return
   *   A [[DataFrame]]
   */
  def select(columns: Array[String]): DataFrame = null

  /**
   * Returns a new DataFrame that excludes the columns with the specified names from the output.
   *
   * This is functionally equivalent to calling [[select(first:String* select]] and passing in all
   * columns except the ones to exclude.
   *
   * Throws [[SnowparkClientException]] if the resulting DataFrame contains no output columns.
   * @group transform
   * @since 0.1.0
   * @param first
   *   The name of the first column to exclude.
   * @param remaining
   *   A list of the names of additional columns to exclude.
   * @return
   *   A [[DataFrame]]
   */
  def drop(first: String, remaining: String*): DataFrame = null

  /**
   * Returns a new DataFrame that excludes the columns with the specified names from the output.
   *
   * This is functionally equivalent to calling [[select(columns:Seq* select]] and passing in all
   * columns except the ones to exclude.
   *
   * Throws [[SnowparkClientException]] if the resulting DataFrame contains no output columns.
   *
   * @group transform
   * @since 0.2.0
   * @param colNames
   *   A list of the names of columns to exclude.
   * @return
   *   A [[DataFrame]]
   */
  def drop(colNames: Seq[String]): DataFrame = null

  /**
   * Returns a new DataFrame that excludes the columns with the specified names from the output.
   *
   * This is functionally equivalent to calling [[select(columns:Array[String* select]] and passing
   * in all columns except the ones to exclude.
   *
   * Throws [[SnowparkClientException]] if the resulting DataFrame contains no output columns.
   *
   * @group transform
   * @since 0.7.0
   * @param colNames
   *   An array of the names of columns to exclude.
   * @return
   *   A [[DataFrame]]
   */
  def drop(colNames: Array[String]): DataFrame = null

  /**
   * Returns a new DataFrame that excludes the columns specified by the expressions from the output.
   *
   * This is functionally equivalent to calling [[select(first:String* select]] and passing in all
   * columns except the ones to exclude.
   *
   * This method throws a [[SnowparkClientException]] if:
   *   - A specified column does not have a name, or
   *   - The resulting DataFrame has no output columns.
   *
   * @group transform
   * @since 0.1.0
   * @param first
   *   The expression for the first column to exclude.
   * @param remaining
   *   A list of expressions for additional columns to exclude.
   * @return
   *   A [[DataFrame]]
   */
  def drop(first: Column, remaining: Column*): DataFrame = null

  /**
   * Returns a new DataFrame that excludes the specified column expressions from the output.
   *
   * This is functionally equivalent to calling [[select(columns:Seq* select]] and passing in all
   * columns except the ones to exclude.
   *
   * This method throws a [[SnowparkClientException]] if:
   *   - A specified column does not have a name, or
   *   - The resulting DataFrame has no output columns.
   *
   * @group transform
   * @since 0.2.0
   * @param cols
   *   A list of the names of the columns to exclude.
   * @return
   *   A [[DataFrame]]
   */
  def drop[T: ClassTag](cols: Seq[Column]): DataFrame = null

  /**
   * Returns a new DataFrame that excludes the specified column expressions from the output.
   *
   * This is functionally equivalent to calling [[select(columns:Array[String* select]] and passing
   * in all columns except the ones to exclude.
   *
   * This method throws a [[SnowparkClientException]] if:
   *   - A specified column does not have a name, or
   *   - The resulting DataFrame has no output columns.
   *
   * @group transform
   * @since 0.7.0
   * @param cols
   *   An array of the names of the columns to exclude.
   * @return
   *   A [[DataFrame]]
   */
  def drop(cols: Array[Column]): DataFrame = null

  /**
   * Filters rows based on the specified conditional expression (similar to WHERE in SQL).
   *
   * For example:
   *
   * {{{
   *   val dfFiltered = df.filter($"colA" > 1 && $"colB" < 100)
   * }}}
   *
   * @group transform
   * @since 0.1.0
   * @param condition
   *   Filter condition defined as an expression on columns.
   * @return
   *   A filtered [[DataFrame]]
   */
  def filter(condition: Column): DataFrame = null

  /**
   * Filters rows based on the specified conditional expression (similar to WHERE in SQL). This is
   * equivalent to calling [[filter]].
   *
   * For example:
   *
   * {{{
   *   // The following two result in the same SQL query:
   *   pricesDF.filter($"price" > 100)
   *   pricesDF.where($"price" > 100)
   * }}}
   *
   * @group transform
   * @since 0.1.0
   * @param condition
   *   Filter condition defined as an expression on columns.
   * @return
   *   A filtered [[DataFrame]]
   */
  def where(condition: Column): DataFrame = null

  /**
   * Aggregate the data in the DataFrame. Use this method if you don't need to group the data
   * (`groupBy`).
   *
   * For the input, pass in a Map that specifies the column names and aggregation functions. For
   * each pair in the Map:
   *   - Set the key to the name of the column to aggregate.
   *   - Set the value to the name of the aggregation function to use on that column.
   *
   * The following example calculates the maximum value of the `num_sales` column and the average
   * value of the `price` column:
   * {{{
   *   val dfAgg = df.agg("num_sales" -> "max", "price" -> "mean")
   * }}}
   *
   * This is equivalent to calling `agg` after calling `groupBy` without a column name:
   * {{{
   *   val dfAgg = df.groupBy().agg(df("num_sales") -> "max", df("price") -> "mean")
   * }}}
   *
   * @group transform
   * @since 0.1.0
   * @param expr
   *   A map of column names and aggregate functions.
   * @return
   *   A [[DataFrame]]
   */
  def agg(expr: (String, String), exprs: (String, String)*): DataFrame = null

  /**
   * Aggregate the data in the DataFrame. Use this method if you don't need to group the data
   * (`groupBy`).
   *
   * For the input, pass in a Map that specifies the column names and aggregation functions. For
   * each pair in the Map:
   *   - Set the key to the name of the column to aggregate.
   *   - Set the value to the name of the aggregation function to use on that column.
   *
   * The following example calculates the maximum value of the `num_sales` column and the average
   * value of the `price` column:
   * {{{
   *   val dfAgg = df.agg(Seq("num_sales" -> "max", "price" -> "mean"))
   * }}}
   *
   * This is equivalent to calling `agg` after calling `groupBy` without a column name:
   * {{{
   *   val dfAgg = df.groupBy().agg(Seq(df("num_sales") -> "max", df("price") -> "mean"))
   * }}}
   *
   * @group transform
   * @since 0.2.0
   * @param exprs
   *   A map of column names and aggregate functions.
   * @return
   *   A [[DataFrame]]
   */
  def agg(exprs: Seq[(String, String)]): DataFrame = null

  /**
   * Aggregate the data in the DataFrame. Use this method if you don't need to group the data
   * (`groupBy`).
   *
   * For the input value, pass in expressions that apply aggregation functions to columns (functions
   * that are defined in the [[functions]] object).
   *
   * The following example calculates the maximum value of the `num_sales` column and the mean value
   * of the `price` column:
   *
   * For example:
   *
   * {{{
   *   import com.snowflake.snowpark.functions._
   *
   *   val dfAgg = df.agg(max($"num_sales"), mean($"price"))
   * }}}
   *
   * @group transform
   * @since 0.1.0
   * @param expr
   *   A list of expressions on columns.
   * @return
   *   A [[DataFrame]]
   */
  def agg(expr: Column, exprs: Column*): DataFrame = null

  /**
   * Aggregate the data in the DataFrame. Use this method if you don't need to group the data
   * (`groupBy`).
   *
   * For the input value, pass in expressions that apply aggregation functions to columns (functions
   * that are defined in the [[functions]] object).
   *
   * The following example calculates the maximum value of the `num_sales` column and the mean value
   * of the `price` column:
   * {{{
   *   import com.snowflake.snowpark.functions._
   *
   *   val dfAgg = df.agg(Seq(max($"num_sales"), mean($"price")))
   * }}}
   *
   * @group transform
   * @since 0.2.0
   * @param exprs
   *   A list of expressions on columns.
   * @return
   *   A [[DataFrame]]
   */
  def agg[T: ClassTag](exprs: Seq[Column]): DataFrame = null

  /**
   * Aggregate the data in the DataFrame. Use this method if you don't need to group the data
   * (`groupBy`).
   *
   * For the input value, pass in expressions that apply aggregation functions to columns (functions
   * that are defined in the [[functions]] object).
   *
   * The following example calculates the maximum value of the `num_sales` column and the mean value
   * of the `price` column:
   *
   * For example:
   *
   * {{{
   *   import com.snowflake.snowpark.functions._
   *
   *   val dfAgg = df.agg(Array(max($"num_sales"), mean($"price")))
   * }}}
   *
   * @group transform
   * @since 0.7.0
   * @param exprs
   *   An array of expressions on columns.
   * @return
   *   A [[DataFrame]]
   */
  def agg(exprs: Array[Column]): DataFrame = null

  //  /**
  //   * Performs an SQL
  //   * [[https://docs.snowflake.com/en/sql-reference/
  //   constructs/group-by-rollup.html GROUP BY ROLLUP]]
  //   * on the DataFrame.
  //   *
  //   * @group transform
  //   * @since 0.1.0
  //   * @param first The expression for the first column.
  //   * @param remaining A list of expressions for additional columns.
  //   * @return A [[RelationalGroupedDataFrame]]
  //   */
  //  def rollup(first: Column, remaining: Column*): RelationalGroupedDataFrame =
  //    rollup(first +: remaining)

  //  /**
  //   * Performs an SQL
  //   * [[https://docs.snowflake.com/en/sql-reference/
  //   constructs/group-by-rollup.html GROUP BY ROLLUP]]
  //   * on the DataFrame.
  //   *
  //   * @group transform
  //   * @since 0.2.0
  //   * @param cols A list of expressions on columns.
  //   * @return A [[RelationalGroupedDataFrame]]
  //   */
  //  def rollup[T: ClassTag](cols: Seq[Column]): RelationalGroupedDataFrame =
  //    RelationalGroupedDataFrame(this, cols.map(_.expr), RelationalGroupedDataFrame.RollupType)

  //  /**
  //   * Performs an SQL
  //   * [[https://docs.snowflake.com/en/sql-reference/constructs/
  //   group-by-rollup.html GROUP BY ROLLUP]]
  //   * on the DataFrame.
  //   *
  //   * @group transform
  //   * @since 0.7.0
  //   * @param cols An array of expressions on columns.
  //   * @return A [[RelationalGroupedDataFrame]]
  //   */
  //  def rollup(cols: Array[Column]): RelationalGroupedDataFrame = rollup(cols.toSeq)
  //
  //  /**
  //   * Performs an SQL
  //   * [[https://docs.snowflake.com/en/sql-reference/
  //   constructs/group-by-rollup.html GROUP BY ROLLUP]]
  //   * on the DataFrame.
  //   *
  //   * @group transform
  //   * @since 0.1.0
  //   * @param first The name of the first column.
  //   * @param remaining A list of the names of additional columns.
  //   * @return A [[RelationalGroupedDataFrame]]
  //   */
  //  def rollup(first: String, remaining: String*): RelationalGroupedDataFrame =
  //    rollup(first +: remaining)

  //  /**
  //   * Performs an SQL
  //   * [[https://docs.snowflake.com/en/sql-reference/
  //   constructs/group-by-rollup.html GROUP BY ROLLUP]]
  //   * on the DataFrame.
  //   *
  //   * @group transform
  //   * @since 0.2.0
  //   * @param cols A list of column names.
  //   * @return A [[RelationalGroupedDataFrame]]
  //   */
  //  def rollup(cols: Seq[String]): RelationalGroupedDataFrame =
  //    RelationalGroupedDataFrame(this, cols.map(resolve), RelationalGroupedDataFrame.RollupType)
  //
  //  /**
  //   * Performs an SQL
  //   * [[https://docs.snowflake.com/en/
  //   sql-reference/constructs/group-by-rollup.html GROUP BY ROLLUP]]
  //   * on the DataFrame.
  //   *
  //   * @group transform
  //   * @since 0.7.0
  //   * @param cols An array of column names.
  //   * @return A [[RelationalGroupedDataFrame]]
  //   */
  //  def rollup(cols: Array[String]): RelationalGroupedDataFrame = rollup(cols.toSeq)

  //  /**
  //   * Groups rows by the columns specified by expressions (similar to GROUP BY in SQL).
  //   *
  //   * This method returns a [[RelationalGroupedDataFrame]]
  //   that you can use to perform aggregations
  //   * on each group of data.
  //   *
  //   * @group transform
  //   * @since 0.1.0
  //   * @param first The expression for the first column to group by.
  //   * @param remaining A list of expressions for additional columns to group by.
  //   * @return A [[RelationalGroupedDataFrame]]
  //   */
  //  def groupBy(first: Column, remaining: Column*): RelationalGroupedDataFrame =
  //    groupBy(first +: remaining)

  //  /**
  //   * Returns a [[RelationalGroupedDataFrame]] that you can use to perform aggregations on the
  //   * underlying DataFrame.
  //   *
  //   * @group transform
  //   * @since 0.1.0
  //   * @return A [[RelationalGroupedDataFrame]]
  //   */
  //  def groupBy(): RelationalGroupedDataFrame = groupBy(Seq.empty[Column])
  //
  //  /**
  //   * Groups rows by the columns specified by expressions
  //   * (similar to GROUP BY in SQL).
  //   *
  //   * This method returns a [[RelationalGroupedDataFrame]]
  //   that you can use to perform aggregations
  //   * on each group of data.
  //   *
  //   * @group transform
  //   * @since 0.2.0
  //   * @param cols A list of expressions on columns.
  //   * @return A [[RelationalGroupedDataFrame]]
  //   */
  //  def groupBy[T: ClassTag](cols: Seq[Column]): RelationalGroupedDataFrame =
  //    RelationalGroupedDataFrame(this, cols.map(_.expr), RelationalGroupedDataFrame.GroupByType)

  //  /**
  //   * Groups rows by the columns specified by expressions
  //   * (similar to GROUP BY in SQL).
  //   *
  //   * This method returns a [[RelationalGroupedDataFrame]]
  //   that you can use to perform aggregations
  //   * on each group of data.
  //   *
  //   * @group transform
  //   * @since 0.7.0
  //   * @param cols An array of expressions on columns.
  //   * @return A [[RelationalGroupedDataFrame]]
  //   */
  //  def groupBy(cols: Array[Column]): RelationalGroupedDataFrame = groupBy(cols.toSeq)
  //
  //  /**
  //   * Groups rows by the columns specified by name (similar to GROUP BY in SQL).
  //   *
  //   * This method returns a [[RelationalGroupedDataFrame]]
  //   that you can use to perform aggregations
  //   * on each group of data.
  //   *
  //   * @group transform
  //   * @since 0.1.0
  //   * @param first The name of the first column to group by.
  //   * @param remaining A list of the names of additional columns to group by.
  //   * @return A [[RelationalGroupedDataFrame]]
  //   */
  //  def groupBy(first: String, remaining: String*): RelationalGroupedDataFrame =
  //    groupBy(first +: remaining)

  //  /**
  //   * Groups rows by the columns specified by name (similar to GROUP BY in SQL).
  //   *
  //   * This method returns a [[RelationalGroupedDataFrame]]
  //   that you can use to perform aggregations
  //   * on each group of data.
  //   *
  //   * @group transform
  //   * @since 0.2.0
  //   * @param cols A list of the names of columns to group by.
  //   * @return A [[RelationalGroupedDataFrame]]
  //   */
  //  def groupBy(cols: Seq[String]): RelationalGroupedDataFrame =
  //    RelationalGroupedDataFrame(this, cols.map(resolve), RelationalGroupedDataFrame.GroupByType)
  //
  //  /**
  //   * Groups rows by the columns specified by name (similar to GROUP BY in SQL).
  //   *
  //   * This method returns a [[RelationalGroupedDataFrame]]
  //   that you can use to perform aggregations
  //   * on each group of data.
  //   *
  //   * @group transform
  //   * @since 0.7.0
  //   * @param cols An array of the names of columns to group by.
  //   * @return A [[RelationalGroupedDataFrame]]
  //   */
  //  def groupBy(cols: Array[String]): RelationalGroupedDataFrame = groupBy(cols.toSeq)

  // scalastyle:off line.size.limit
  //  /**
  //   * Performs an SQL
  //   * [[https://docs.snowflake.com/en/sql-reference/constructs/group-by-rollup.html GROUP BY GROUPING SETS]]
  //   * on the DataFrame.
  //   *
  //   * GROUP BY GROUPING SETS is an extension of the GROUP BY clause
  //   * that allows computing multiple GROUP BY clauses in a single statement.
  //   * The group set is a set of dimension columns.
  //   *
  //   * GROUP BY GROUPING SETS is equivalent to the UNION of two or
  //   * more GROUP BY operations in the same result set:
  //   *
  //   * `df.groupByGroupingSets(GroupingSets(Set(col("a"))))` is equivalent to
  //   * `df.groupBy("a")`
  //   *
  //   * and
  //   *
  //   * `df.groupByGroupingSets(GroupingSets(Set(col("a")), Set(col("b"))))` is equivalent to
  //   * `df.groupBy("a")` union `df.groupBy("b")`
  //   *
  //   * @param first A [[GroupingSets]] object.
  //   * @param remaining A list of additional [[GroupingSets]] objects.
  //   * @since 0.4.0
  //   */
  //  // scalastyle:on line.size.limit
  //  def groupByGroupingSets(
  //                           first: GroupingSets,
  //                           remaining: GroupingSets*): RelationalGroupedDataFrame =
  //    groupByGroupingSets(first +: remaining)

  // scalastyle:off line.size.limit
  //  /**
  //   * Performs an SQL
  //   * [[https://docs.snowflake.com/en/sql-reference/constructs/group-by-rollup.html GROUP BY GROUPING SETS]]
  //   * on the DataFrame.
  //   *
  //   * GROUP BY GROUPING SETS is an extension of the GROUP BY clause
  //   * that allows computing multiple group-by clauses in a single statement.
  //   * The group set is a set of dimension columns.
  //   *
  //   * GROUP BY GROUPING SETS is equivalent to the UNION of two or
  //   * more GROUP BY operations in the same result set:
  //   *
  //   * `df.groupByGroupingSets(GroupingSets(Set(col("a"))))` is equivalent to
  //   * `df.groupBy("a")`
  //   *
  //   * and
  //   *
  //   * `df.groupByGroupingSets(GroupingSets(Set(col("a")), Set(col("b"))))` is equivalent to
  //   * `df.groupBy("a")` union `df.groupBy("b")`
  //   *
  //   * @param groupingSets A list of [[GroupingSets]] objects.
  //   * @since 0.4.0
  //   */
  //  // scalastyle:on line.size.limit
  //  def groupByGroupingSets(groupingSets: Seq[GroupingSets]): RelationalGroupedDataFrame =
  //    RelationalGroupedDataFrame(
  //      this,
  //      groupingSets.map(_.toExpression),
  //      RelationalGroupedDataFrame.GroupByGroupingSetsType)

  //  /**
  //   * Performs an SQL
  //   * [[https://docs.snowflake.com/en/sql-reference/constructs/group-by-rollup.html GROUP BY CUBE]]
  //   * on the DataFrame.
  //   *
  //   * @group transform
  //   * @since 0.1.0
  //   * @param first The expression for the first column to use.
  //   * @param remaining A list of expressions for additional columns to use.
  //   * @return A [[RelationalGroupedDataFrame]]
  //   */
  //  def cube(first: Column, remaining: Column*): RelationalGroupedDataFrame =
  //    cube(first +: remaining)

  //  /**
  //   * Performs an SQL
  //   * [[https://docs.snowflake.com/en/sql-reference/constructs/group-by-rollup.html GROUP BY CUBE]]
  //   * on the DataFrame.
  //   *
  //   * @group transform
  //   * @since 0.2.0
  //   * @param cols A list of expressions for columns to use.
  //   * @return A [[RelationalGroupedDataFrame]]
  //   */
  //  def cube[T: ClassTag](cols: Seq[Column]): RelationalGroupedDataFrame =
  //    RelationalGroupedDataFrame(this, cols.map(_.expr), RelationalGroupedDataFrame.CubeType)

  //  /**
  //   * Performs an SQL
  //   * [[https://docs.snowflake.com/en/sql-reference/constructs/group-by-rollup.html GROUP BY CUBE]]
  //   * on the DataFrame.
  //   *
  //   * @group transform
  //   * @since 0.9.0
  //   * @param cols A list of expressions for columns to use.
  //   * @return A [[RelationalGroupedDataFrame]]
  //   */
  //  def cube(cols: Array[Column]): RelationalGroupedDataFrame = cube(cols.toSeq)

  //  /**
  //   * Performs an SQL
  //   * [[https://docs.snowflake.com/en/sql-reference/constructs/group-by-rollup.html GROUP BY CUBE]]
  //   * on the DataFrame.
  //   *
  //   * @group transform
  //   * @since 0.1.0
  //   * @param first The name of the first column to use.
  //   * @param remaining A list of the names of additional columns to use.
  //   * @return A [[RelationalGroupedDataFrame]]
  //   */
  //  def cube(first: String, remaining: String*): RelationalGroupedDataFrame =
  //    cube(first +: remaining)

  //  /**
  //   * Performs an SQL
  //   * [[https://docs.snowflake.com/en/sql-reference/constructs/group-by-rollup.html GROUP BY CUBE]]
  //   *
  //   * @group transform
  //   * @since 0.2.0
  //   * @param cols A list of the names of columns to use.
  //   * @return A [[RelationalGroupedDataFrame]]
  //   */
  //  def cube(cols: Seq[String]): RelationalGroupedDataFrame =
  //    RelationalGroupedDataFrame(this, cols.map(resolve), RelationalGroupedDataFrame.CubeType)

  //  /**
  //   * Returns a new DataFrame that contains only the rows with distinct values from the current
  //   * DataFrame.
  //   *
  //   * This is equivalent to performing a SELECT DISTINCT in SQL.
  //   *
  //   * @group transform
  //   * @since 0.1.0
  //   * @return A [[DataFrame]]
  //   */
  //  def distinct(): DataFrame = transformation("distinct") {
  //    groupBy(output.map(att => quoteName(att.name)).map(this.col)).agg(Map.empty[Column, String])
  //  }

  //  /**
  //   * Creates a new DataFrame by removing duplicated rows on given subset of columns.
  //   * If no subset of columns specified, this function is same as [[distinct()]] function.
  //   * The result is non-deterministic when removing duplicated rows from the subset of
  //   * columns but not all columns.
  //   * For example:
  //   * Supposes we have a DataFrame `df`, which contains three rows (a, b, c):
  //   * (1, 1, 1), (1, 1, 2), (1, 2, 3)
  //   * The result of df.dropDuplicates("a", "b") can be either
  //   * (1, 1, 1), (1, 2, 3)
  //   * or
  //   * (1, 1, 2), (1, 2, 3)
  //   *
  //   * @group transform
  //   * @since 0.10.0
  //   * @return A [[DataFrame]]
  //   */
  //  def dropDuplicates(colNames: String*): DataFrame = transformation("dropDuplicates") {
  //    if (colNames.isEmpty) {
  //      this.distinct()
  //    } else {
  //      val filterCols = colNames.map(col)
  //      val outputCols = output.map(att => col(att.name))
  //      val rowNumber = functions
  //        .row_number()
  //        .over(Window.partitionBy(filterCols: _*).orderBy(filterCols: _*))
  //      val rowNumberName = Random.alphanumeric.take(10).mkString
  //      this
  //        .select(outputCols :+ rowNumber.as(rowNumberName))
  //        .where(functions.col(rowNumberName) === 1)
  //        .select(outputCols)
  //    }
  //  }

  //  /**
  //   * Rotates this DataFrame by turning the unique values from one column in the input
  //   * expression into multiple columns and aggregating results where required on any
  //   * remaining column values.
  //   *
  //   * Only one aggregate is supported with pivot.
  //   *
  //   * For example:
  //   * {{{
  //   *   val dfPivoted = df.pivot("col_1", Seq(1,2,3)).agg(sum(col("col_2")))
  //   * }}}
  //   *
  //   * @group transform
  //   * @since 0.1.0
  //   * @param pivotColumn The name of the column to use.
  //   * @param values A list of values in the column.
  //   * @return A [[RelationalGroupedDataFrame]]
  //   */
  //  def pivot(pivotColumn: String, values: Seq[Any]): RelationalGroupedDataFrame =
  //    pivot(Column(pivotColumn), values)

  //  /**
  //   * Rotates this DataFrame by turning the unique values from one column in the input
  //   * expression into multiple columns and aggregating results where required on any
  //   * remaining column values.
  //   *
  //   * Only one aggregate is supported with pivot.
  //   *
  //   * For example:
  //   * {{{
  //   *   val dfPivoted = df.pivot(col("col_1"), Seq(1,2,3)).agg(sum(col("col_2")))
  //   * }}}
  //   *
  //   * @group transform
  //   * @since 0.1.0
  //   * @param pivotColumn Expression for the column that you want to use.
  //   * @param values A list of values in the column.
  //   * @return A [[RelationalGroupedDataFrame]]
  //   */
  //  def pivot(pivotColumn: Column, values: Seq[Any]): RelationalGroupedDataFrame = {
  //    val valueExprs = values.map {
  //      case c: Column => c.expr
  //      case v => Literal(v)
  //    }
  //    RelationalGroupedDataFrame(
  //      this,
  //      Seq.empty,
  //      RelationalGroupedDataFrame.PivotType(pivotColumn.expr, valueExprs))
  //  }

  /**
   * Returns a new DataFrame that contains at most ''n'' rows from the current DataFrame (similar to
   * LIMIT in SQL).
   *
   * Note that this is a transformation method and not an action method.
   *
   * @group transform
   * @since 0.1.0
   * @param n
   *   Number of rows to return.
   * @return
   *   A [[DataFrame]]
   */
  def limit(n: Int): DataFrame = null

  /**
   * Returns a new DataFrame that contains all the rows in the current DataFrame and another
   * DataFrame (`other`), excluding any duplicate rows. Both input DataFrames must contain the same
   * number of columns.
   *
   * For example:
   *
   * {{{
   *   val df1and2 = df1.union(df2)
   * }}}
   *
   * @group transform
   * @since 0.1.0
   * @param other
   *   The other [[DataFrame]] that contains the rows to include.
   * @return
   *   A [[DataFrame]]
   */
  def union(other: DataFrame): DataFrame = null

  /**
   * Returns a new DataFrame that contains all the rows in the current DataFrame and another
   * DataFrame (`other`), including any duplicate rows. Both input DataFrames must contain the same
   * number of columns.
   *
   * For example:
   *
   * {{{
   *   val df1and2 = df1.unionAll(df2)
   * }}}
   *
   * @group transform
   * @since 0.1.0
   * @param other
   *   The other [[DataFrame]] that contains the rows to include.
   * @return
   *   A [[DataFrame]]
   */
  def unionAll(other: DataFrame): DataFrame = null

  /**
   * Returns a new DataFrame that contains all the rows in the current DataFrame and another
   * DataFrame (`other`), excluding any duplicate rows.
   *
   * This method matches the columns in the two DataFrames by their names, not by their positions.
   * The columns in the other DataFrame are rearranged to match the order of columns in the current
   * DataFrame.
   *
   * For example:
   *
   * {{{
   *   val df1and2 = df1.unionByName(df2)
   * }}}
   *
   * @group transform
   * @since 0.1.0
   * @param other
   *   The other [[DataFrame]] that contains the rows to include.
   * @return
   *   A [[DataFrame]]
   */
  def unionByName(other: DataFrame): DataFrame = null

  /**
   * Returns a new DataFrame that contains all the rows in the current DataFrame and another
   * DataFrame (`other`), including any duplicate rows.
   *
   * This method matches the columns in the two DataFrames by their names, not by their positions.
   * The columns in the other DataFrame are rearranged to match the order of columns in the current
   * DataFrame.
   *
   * For example:
   *
   * {{{
   *   val df1and2 = df1.unionAllByName(df2)
   * }}}
   *
   * @group transform
   * @since 0.9.0
   * @param other
   *   The other [[DataFrame]] that contains the rows to include.
   * @return
   *   A [[DataFrame]]
   */
  def unionAllByName(other: DataFrame): DataFrame = null

  /**
   * Returns a new DataFrame that contains the intersection of rows from the current DataFrame and
   * another DataFrame (`other`). Duplicate rows are eliminated.
   *
   * For example:
   *
   * {{{
   *   val dfIntersectionOf1and2 = df1.intersect(df2)
   * }}}
   *
   * @group transform
   * @since 0.1.0
   * @param other
   *   The other [[DataFrame]] that contains the rows to use for the intersection.
   * @return
   *   A [[DataFrame]]
   */
  def intersect(other: DataFrame): DataFrame = null

  /**
   * Returns a new DataFrame that contains all the rows from the current DataFrame except for the
   * rows that also appear in another DataFrame (`other`). Duplicate rows are eliminated.
   *
   * For example:
   *
   * {{{
   *   val df1except2 = df1.except(df2)
   * }}}
   *
   * @group transform
   * @since 0.1.0
   * @param other
   *   The [[DataFrame]] that contains the rows to exclude.
   * @return
   *   A [[DataFrame]]
   */
  def except(other: DataFrame): DataFrame = null

  /**
   * Performs a default inner join of the current DataFrame and another DataFrame (`right`).
   *
   * Because this method does not specify a join condition, the returned DataFrame is a cartesian
   * product of the two DataFrames.
   *
   * If the current and `right` DataFrames have columns with the same name, and you need to refer to
   * one of these columns in the returned DataFrame, use the [[apply]] or [[col]] function on the
   * current or `right` DataFrame to disambiguate references to these columns.
   *
   * For example:
   *
   * {{{
   *   val result = left.join(right)
   *   val project = result.select(left("common_col") + right("common_col"))
   * }}}
   *
   * @group transform
   * @since 0.1.0
   * @param right
   *   The other [[DataFrame]] to join.
   * @return
   *   A [[DataFrame]]
   */
  def join(right: DataFrame): DataFrame = null

  /**
   * Performs a default inner join of the current DataFrame and another DataFrame (`right`) on a
   * column (`usingColumn`).
   *
   * The method assumes that the `usingColumn` column has the same meaning in the left and right
   * DataFrames.
   *
   * For example:
   *
   * {{{
   *   val result = left.join(right, "a")
   * }}}
   *
   * @group transform
   * @since 0.1.0
   * @param right
   *   The other [[DataFrame]] to join.
   * @param usingColumn
   *   The name of the column to use for the join.
   * @return
   *   A [[DataFrame]]
   */
  def join(right: DataFrame, usingColumn: String): DataFrame = null

  /**
   * Performs a default inner join of the current DataFrame and another DataFrame (`right`) on a
   * list of columns (`usingColumns`).
   *
   * The method assumes that the columns in `usingColumns` have the same meaning in the left and
   * right DataFrames.
   *
   * For example:
   *
   * {{{
   *   val dfJoinOnColA = df.join(df2, Seq("a"))
   *   val dfJoinOnColAAndColB = df.join(df2, Seq("a", "b"))
   * }}}
   *
   * @group transform
   * @since 0.1.0
   * @param right
   *   The other [[DataFrame]] to join.
   * @param usingColumns
   *   A list of the names of the columns to use for the join.
   * @return
   *   A [[DataFrame]]
   */
  def join(right: DataFrame, usingColumns: Seq[String]): DataFrame = null

  /**
   * Performs a join of the specified type (`joinType`) with the current DataFrame and another
   * DataFrame (`right`) on a list of columns (`usingColumns`).
   *
   * The method assumes that the columns in `usingColumns` have the same meaning in the left and
   * right DataFrames.
   *
   * For example:
   *
   * {{{
   *   val dfLeftJoin = df1.join(df2, Seq("a"), "left")
   *   val dfOuterJoin = df1.join(df2, Seq("a", "b"), "outer")
   * }}}
   *
   * @group transform
   * @since 0.1.0
   * @param right
   *   The other [[DataFrame]] to join.
   * @param usingColumns
   *   A list of the names of the columns to use for the join.
   * @param joinType
   *   The type of join (e.g. {@code "right"} , {@code "outer"} , etc.).
   * @return
   *   A [[DataFrame]]
   */
  def join(right: DataFrame, usingColumns: Seq[String], joinType: String): DataFrame =
    null

  // scalastyle:off line.size.limit
  /**
   * Performs a default inner join of the current DataFrame and another DataFrame (`right`) using
   * the join condition specified in an expression (`joinExpr`).
   *
   * To disambiguate columns with the same name in the left DataFrame and right DataFrame, use the
   * [[apply]] or [[col]] method of each DataFrame (`df("col")` or `df.col("col")`). You can use
   * this approach to disambiguate columns in the `joinExprs` parameter and to refer to columns in
   * the returned DataFrame.
   *
   * For example:
   *
   * {{{
   *   val dfJoin = df1.join(df2, df1("a") === df2("b"))
   *   val dfJoin2 = df1.join(df2, df1("a") === df2("b") && df1("c" === df2("d"))
   *   val dfJoin3 = df1.join(df2, df1("a") === df2("a") && df1("b" === df2("b"))
   *   // If both df1 and df2 contain column 'c'
   *   val project = dfJoin3.select(df1("c") + df2("c"))
   * }}}
   *
   * If you need to join a DataFrame with itself, keep in mind that there is no way to distinguish
   * between columns on the left and right sides in a join expression. For example:
   * {{{
   *   val dfJoined = df.join(df, df("a") === df("b")) // Column references are ambiguous
   * }}}
   * As a workaround, you can either construct the left and right DataFrames separately, or you can
   * call a
   * [[join(right:com\.snowflake\.snowpark\.DataFrame,usingColumns:Seq[String]):com\.snowflake\.snowpark\.DataFrame* join]]
   * method that allows you to pass in 'usingColumns' parameter.
   *
   * @group transform
   * @since 0.1.0
   * @param right
   *   The other [[DataFrame]] to join.
   * @param joinExprs
   *   Expression that specifies the join condition.
   * @return
   *   A [[DataFrame]]
   */
  // scalastyle:on line.size.limit
  def join(right: DataFrame, joinExprs: Column): DataFrame = null

  // scalastyle:off line.size.limit
  /**
   * Performs a join of the specified type (`joinType`) with the current DataFrame and another
   * DataFrame (`right`) using the join condition specified in an expression (`joinExpr`).
   *
   * To disambiguate columns with the same name in the left DataFrame and right DataFrame, use the
   * [[apply]] or [[col]] method of each DataFrame (`df("col")` or `df.col("col")`). You can use
   * this approach to disambiguate columns in the `joinExprs` parameter and to refer to columns in
   * the returned DataFrame.
   *
   * For example:
   *
   * {{{
   *   val dfJoin = df1.join(df2, df1("a") === df2("b"), "left")
   *   val dfJoin2 = df1.join(df2, df1("a") === df2("b") && df1("c" === df2("d"), "outer")
   *   val dfJoin3 = df1.join(df2, df1("a") === df2("a") && df1("b" === df2("b"), "outer")
   *   // If both df1 and df2 contain column 'c'
   *   val project = dfJoin3.select(df1("c") + df2("c"))
   * }}}
   *
   * If you need to join a DataFrame with itself, keep in mind that there is no way to distinguish
   * between columns on the left and right sides in a join expression. For example:
   * {{{
   *   val dfJoined = df.join(df, df("a") === df("b"), joinType) // Column references are ambiguous
   * }}}
   * To do a self-join, you can you either clone([[clone]]) the DataFrame as follows,
   * {{{
   *   val clonedDf = df.clone
   *   val dfJoined = df.join(clonedDf, df("a") === clonedDf("b"), joinType)
   * }}}
   * or you can call a
   * [[join(right:com\.snowflake\.snowpark\.DataFrame,usingColumns:Seq[String],joinType:String):com\.snowflake\.snowpark\.DataFrame* join]]
   * method that allows you to pass in 'usingColumns' parameter.
   *
   * @group transform
   * @since 0.1.0
   * @param right
   *   The other [[DataFrame]] to join.
   * @param joinExprs
   *   Expression that specifies the join condition.
   * @param joinType
   *   The type of join (e.g. {@code "right"} , {@code "outer"} , etc.).
   * @return
   *   A [[DataFrame]]
   */
  // scalastyle:on line.size.limit
  def join(right: DataFrame, joinExprs: Column, joinType: String): DataFrame =
    null

  //  /**
  //   * Joins the current DataFrame with the output of the specified table function `func`.
  //   *
  //   * To pass arguments to the table function, use the
  //   `firstArg` and `remaining` arguments of this
  //   * method. In the table function arguments, you can include references to columns in this
  //   * DataFrame.
  //   *
  //   * For example:
  //   * {{{
  //   *   // The following example uses the split_to_table function to split
  //   *   // column 'a' in this DataFrame on the character ','.
  //   *   // Each row in the current DataFrame will produce N rows in the resulting DataFrame,
  //   *   // where N is the number of tokens in the column 'a'.
  //   *
  //   *   import com.snowflake.snowpark.functions._
  //   *   import com.snowflake.snowpark.tableFunctions._
  //   *
  //   *   df.join(split_to_table, df("a"), lit(","))
  //   * }}}
  //   *
  //   * @group transform
  //   * @since 0.4.0
  //   * @param func [[TableFunction]] object,
  //   which can be one of the values in the [[tableFunctions]]
  //   *   object or an object that you create from the [[TableFunction]] class.
  //   * @param firstArg The first argument to pass to the specified table function.
  //   * @param remaining A list of any additional arguments for the specified table function.
  //   */
  //  def join(func: TableFunction, firstArg: Column, remaining: Column*): DataFrame =
  //    transformation("join") {
  //      join(func, firstArg +: remaining)
  //    }

  //  /**
  //   * Joins the current DataFrame with the output of the specified table function `func`.
  //   *
  //   * To pass arguments to the table function, use the
  //   `args` argument of this method. In the table
  //   * function arguments, you can include references to columns in this DataFrame.
  //   *
  //   * For example:
  //   * {{{
  //   *   // The following example uses the split_to_table function to split
  //   *   // column 'a' in this DataFrame on the character ','.
  //   *   // Each row in this DataFrame will produce N rows in the resulting DataFrame,
  //   *   // where N is the number of tokens in the column 'a'.
  //   *   import com.snowflake.snowpark.functions._
  //   *   import com.snowflake.snowpark.tableFunctions._
  //   *
  //   *   df.join(split_to_table, Seq(df("a"), lit(",")))
  //   * }}}
  //   *
  //   * @group transform
  //   * @since 0.4.0
  //   * @param func [[TableFunction]] object,
  //   which can be one of the values in the [[tableFunctions]]
  //   *   object or an object that you create from the [[TableFunction]] class.
  //   * @param args A list of arguments to pass to the specified table function.
  //   */
  //  def join(func: TableFunction, args: Seq[Column]): DataFrame = transformation("join") {
  //    joinTableFunction(func.call(args: _*), None)
  //  }

  //  /**
  //   * Joins the current DataFrame with the output of the specified user-defined table
  //   * function (UDTF) `func`.
  //   *
  //   * To pass arguments to the table function,
  //   use the `args` argument of this method. In the table
  //   * function arguments, you can include references to columns in this DataFrame.
  //   *
  //   * To specify a PARTITION BY or ORDER BY clause,
  //   use the `partitionBy` and `orderBy` arguments.
  //   *
  //   * For example:
  //   * {{{
  //   *   // The following example passes the values in the column `col1` to the
  //   *   // user-defined tabular function (UDTF) `udtf`, partitioning the
  //   *   // data by `col2` and sorting the data by `col1`. The example returns
  //   *   // a new DataFrame that joins the contents of the current DataFrame with
  //   *   // the output of the UDTF.
  //   *   df.join(TableFunction("udtf"), Seq(df("col1")), Seq(df("col2")), Seq(df("col1")))
  //   * }}}
  //   *
  //   * @group transform
  //   * @since 1.7.0
  //   * @param func [[TableFunction]] object that represents a user-defined table function (UDTF).
  //   * @param args A list of arguments to pass to the specified table function.
  //   * @param partitionBy A list of columns partitioned by.
  //   * @param orderBy A list of columns ordered by.
  //   */
  //  def join(
  //            func: TableFunction,
  //            args: Seq[Column],
  //            partitionBy: Seq[Column],
  //            orderBy: Seq[Column]): DataFrame = transformation("join") {
  //    joinTableFunction(
  //      func.call(args: _*),
  //      Some(Window.partitionBy(partitionBy: _*).orderBy(orderBy: _*).getWindowSpecDefinition))
  //  }

  //  /**
  //   * Joins the current DataFrame with the output of
  //   the specified table function `func` that takes
  //   * named parameters (e.g. `flatten`).
  //   *
  //   * To pass arguments to the table function, use the `args` argument of this method. Pass in a
  //   * `Map` of parameter names and values.
  //   In these values, you can include references to columns in
  //   * this DataFrame.
  //   *
  //   * For example:
  //   * {{{
  //   *   // The following example uses the flatten function to explode compound values from
  //   *   // column 'a' in this DataFrame into multiple columns.
  //   *
  //   *   import com.snowflake.snowpark.functions._
  //   *   import com.snowflake.snowpark.tableFunctions._
  //   *
  //   *   df.join(
  //   *     tableFunction("flatten"),
  //   *     Map("input" -> parse_json(df("a")))
  //   *   )
  //   * }}}
  //   *
  //   * @group transform
  //   * @since 0.4.0
  //   * @param func [[TableFunction]]
  //   object, which can be one of the values in the [[tableFunctions]]
  //   *   object or an object that you create from the [[TableFunction]] class.
  //   *  @param args Map of arguments to pass to the specified table function.
  //   *              Some functions, like `flatten`, have named parameters.
  //   *              Use this map to specify the parameter names and their corresponding values.
  //   */
  //  def join(func: TableFunction, args: Map[String, Column]): DataFrame =
  //    transformation("join") {
  //      joinTableFunction(func.call(args), None)
  //    }
  //
  //  /**
  //   * Joins the current DataFrame with the output of the specified user-defined table function
  //   * (UDTF) `func`.
  //   *
  //   * To pass arguments to the table function, use the `args` argument of this method. Pass in a
  //   * `Map` of parameter
  //   names and values. In these values, you can include references to columns in
  //   * this DataFrame.
  //   *
  //   * To specify a
  //   PARTITION BY or ORDER BY clause, use the `partitionBy` and `orderBy` arguments.
  //   *
  //   * For example:
  //   * {{{
  //   *   // The following example passes the values in the column `col1` to the
  //   *   // user-defined tabular function (UDTF) `udtf`, partitioning the
  //   *   // data by `col2` and sorting the data by `col1`. The example returns
  //   *   // a new DataFrame that joins the contents of the current DataFrame with
  //   *   // the output of the UDTF.
  //   *   df.join(
  //   *     tableFunction("udtf"),
  //   *     Map("arg1" -> df("col1"),
  //   *     Seq(df("col2")), Seq(df("col1")))
  //   *   )
  //   * }}}
  //   *
  //   * @group transform
  //   * @since 1.7.0
  //   * @param func [[TableFunction]] object that represents a user-defined table function (UDTF).
  //   * @param args Map of arguments to pass to the specified table function.
  //   *              Some functions, like `flatten`, have named parameters.
  //   *              Use this map to specify the parameter names and their corresponding values.
  //   * @param partitionBy A list of columns partitioned by.
  //   * @param orderBy A list of columns ordered by.
  //   */
  //  def join(
  //            func: TableFunction,
  //            args: Map[String, Column],
  //            partitionBy: Seq[Column],
  //            orderBy: Seq[Column]): DataFrame = transformation("join") {
  //    joinTableFunction(
  //      func.call(args),
  //      Some(Window.partitionBy(partitionBy: _*).orderBy(orderBy: _*).getWindowSpecDefinition))
  //  }

  //  /**
  //   * Joins the current DataFrame with the output of the specified table function `func`.
  //   *
  //   *
  //   * For example:
  //   * {{{
  //   *   // The following example uses the flatten function to explode compound values from
  //   *   // column 'a' in this DataFrame into multiple columns.
  //   *
  //   *   import com.snowflake.snowpark.functions._
  //   *   import com.snowflake.snowpark.tableFunctions._
  //   *
  //   *   df.join(
  //   *     tableFunctions.flatten(parse_json(df("a")))
  //   *   )
  //   * }}}
  //   *
  //   * @group transform
  //   * @since 1.10.0
  //   * @param func
  //   [[TableFunction]] object, which can be one of the values in the [[tableFunctions]]
  //   *             object or an object that you create from the [[TableFunction.apply()]].
  //   */
  //  def join(func: Column): DataFrame = transformation("join") {
  //    joinTableFunction(getTableFunctionExpression(func), None)
  //  }

  /**
   * Joins the current DataFrame with the output of the specified user-defined table function (UDTF)
   * `func`.
   *
   * To specify a PARTITION BY or ORDER BY clause, use the `partitionBy` and `orderBy` arguments.
   *
   * For example:
   * {{{
   *   val tf = session.udtf.registerTemporary(TableFunc1)
   *   df.join(tf(Map("arg1" -> df("col1")),Seq(df("col2")), Seq(df("col1"))))
   * }}}
   *
   * @group transform
   * @since 1.10.0
   * @param func
   *   [[TableFunction]] object that represents a user-defined table function.
   * @param partitionBy
   *   A list of columns partitioned by.
   * @param orderBy
   *   A list of columns ordered by.
   */
  def join(func: Column, partitionBy: Seq[Column], orderBy: Seq[Column]): DataFrame =
    null

  /**
   * Performs a cross join, which returns the cartesian product of the current DataFrame and another
   * DataFrame (`right`).
   *
   * If the current and `right` DataFrames have columns with the same name, and you need to refer to
   * one of these columns in the returned DataFrame, use the [[apply]] or [[col]] function on the
   * current or `right` DataFrame to disambiguate references to these columns.
   *
   * For example:
   *
   * {{{
   *   val dfCrossJoin = left.crossJoin(right)
   *   val project = dfCrossJoin.select(left("common_col") + right("common_col"))
   * }}}
   *
   * @group transform
   * @since 0.1.0
   * @param right
   *   The other [[DataFrame]] to join.
   * @return
   *   A [[DataFrame]]
   */
  def crossJoin(right: DataFrame): DataFrame = null

  /**
   * Performs a natural join (a default inner join) of the current DataFrame and another DataFrame
   * (`right`).
   *
   * For example:
   * {{{
   *   val dfNaturalJoin = df.naturalJoin(df2)
   * }}}
   *
   * Note that this is equivalent to:
   * {{{
   *   val dfNaturalJoin = df.naturalJoin(df2, "inner")
   * }}}
   *
   * @group transform
   * @since 0.1.0
   * @param right
   *   The other [[DataFrame]] to join.
   * @return
   *   A [[DataFrame]]
   */
  def naturalJoin(right: DataFrame): DataFrame = null

  /**
   * Performs a natural join of the specified type (`joinType`) with the current DataFrame and
   * another DataFrame (`right`).
   *
   * For example:
   *
   * {{{
   *   val dfNaturalJoin = df.naturalJoin(df2, "left")
   * }}}
   *
   * @group transform
   * @since 0.1.0
   * @param right
   *   The other [[DataFrame]] to join.
   * @param joinType
   *   The type of join (e.g. {@code "right"} , {@code "outer"} , etc.).
   * @return
   *   A [[DataFrame]]
   */
  def naturalJoin(right: DataFrame, joinType: String): DataFrame = null

  /**
   * Returns a DataFrame with an additional column with the specified name (`colName`). The column
   * is computed by using the specified expression (`col`).
   *
   * If a column with the same name already exists in the DataFrame, that column is replaced by the
   * new column.
   *
   * This example adds a new column named `mean_price` that contains the mean of the existing
   * `price` column in the DataFrame.
   *
   * {{{
   *   val dfWithMeanPriceCol = df.withColumn("mean_price", mean($"price"))
   * }}}
   * @group transform
   * @since 0.1.0
   * @param colName
   *   The name of the column to add or replace.
   * @param col
   *   The [[Column]] to add or replace.
   * @return
   *   A [[DataFrame]]
   */
  def withColumn(colName: String, col: Column): DataFrame = null

  /**
   * Returns a DataFrame with additional columns with the specified names (`colNames`). The columns
   * are computed by using the specified expressions (`cols`).
   *
   * If columns with the same names already exist in the DataFrame, those columns are replaced by
   * the new columns.
   *
   * This example adds new columns named `mean_price` and `avg_price` that contain the mean and
   * average of the existing `price` column.
   *
   * {{{
   *   val dfWithAddedColumns = df.withColumn(
   *       Seq("mean_price", "avg_price"), Seq(mean($"price"), avg($"price") )
   * }}}
   * @group transform
   * @since 0.1.0
   * @param colNames
   *   A list of the names of the columns to add or replace.
   * @param values
   *   A list of the [[Column]] objects to add or replace.
   * @return
   *   A [[DataFrame]]
   */
  def withColumns(colNames: Seq[String], values: Seq[Column]): DataFrame =
    null

  /**
   * Returns a DataFrame with the specified column `col` renamed as `newName`.
   *
   * This example renames the column `A` as `NEW_A` in the DataFrame.
   *
   * {{{
   *   val df = session.sql("select 1 as A, 2 as B")
   *   val dfRenamed = df.rename("NEW_A", col("A"))
   * }}}
   * @group transform
   * @since 0.9.0
   * @param newName
   *   The new name for the column
   * @param col
   *   The [[Column]] to be renamed
   * @return
   *   A [[DataFrame]]
   */
  def rename(newName: String, col: Column): DataFrame = null

  /**
   * Executes the query representing this DataFrame and returns the result as an Array of [[Row]]
   * objects.
   *
   * @group actions
   * @since 0.1.0
   * @return
   *   An Array of [[Row]]
   */
  def collect(): Array[Row] = null

  /**
   * Executes the query representing this DataFrame and returns an iterator of [[Row]] objects that
   * you can use to retrieve the results.
   *
   * Unlike the [[collect]] method, this method does not load all data into memory at once.
   *
   * @group actions
   * @since 0.5.0
   * @return
   *   An Iterator of [[Row]]
   */
  def toLocalIterator: Iterator[Row] = null

  /**
   * Executes the query representing this DataFrame and returns the number of rows in the result
   * (similar to the COUNT function in SQL).
   *
   * @group actions
   * @since 0.1.0
   * @return
   *   The number of rows.
   */
  def count(): Long = 0L

  //  /**
  //   * Returns a [[DataFrameWriter]] object that you can use to write the data in the DataFrame to
  //   * any supported destination. The Default [[SaveMode]] for the returned [[DataFrameWriter]] is
  //   * [[SaveMode.Append Append]].
  //   *
  //   * Example:
  //   * {{{
  //   *   df.write.saveAsTable("table1")
  //   * }}}
  //   *
  //   * @group basic
  //   * @since 0.1.0
  //   * @return A [[DataFrameWriter]]
  //   */
  //  def write: DataFrameWriter = new DataFrameWriter(this)
  //
  //  /**
  //   * Returns a [[DataFrameAsyncActor]] object that can be used to execute DataFrame actions
  //   * asynchronously.
  //   *
  //   * Example:
  //   * {{{
  //   *   val asyncJob = df.async.collect()
  //   *   // At this point, the thread is not blocked. You can perform additional work before
  //   *   // calling asyncJob.getResult() to retrieve the results of the action.
  //   *   // NOTE: getResult() is a blocking call.
  //   *   val rows = asyncJob.getResult()
  //   * }}}
  //   *
  //   * @since 0.11.0
  //   * @group basic
  //   * @return
  //   *   A [[DataFrameAsyncActor]] object
  //   */
  //  def async: DataFrameAsyncActor = new DataFrameAsyncActor(this)

  /**
   * Evaluates this DataFrame and prints out the first ten rows.
   *
   * @group actions
   * @since 0.1.0
   */
  def show(): Unit = null

  /**
   * Evaluates this DataFrame and prints out the first `''n''` rows.
   *
   * @group actions
   * @since 0.1.0
   * @param n
   *   The number of rows to print out.
   */
  def show(n: Int): Unit = null

  /**
   * Evaluates this DataFrame and prints out the first `''n''` rows with the specified maximum
   * number of characters per column.
   *
   * @group actions
   * @since 0.5.0
   * @param n
   *   The number of rows to print out.
   * @param maxWidth
   *   The maximum number of characters to print out for each column. If the number of characters
   *   exceeds the maximum, the method prints out an ellipsis (...) at the end of the column.
   */
  def show(n: Int, maxWidth: Int): Unit = null

  //  private[snowpark] def showString(n: Int, maxWidth: Int = 50): String = {
  //    // scalastyle:off
  //    val query: String = snowflakePlan.queries.last.sql.trim. toLowerCase
  //    // scalastyle:on
  //
  //    // limit clause only works on SELECT,
  //    // for all other queries, we have to truncate result from client
  //    val (result, meta) =
  //      // only apply LIMIT statement to SELECT
  //      if (query.startsWith("select")) {
  //        session.conn.getResultAndMetadata(this.limit(n).snowflakePlan)
  //      } else {
  //        val (res, met) = session.conn.getResultAndMetadata(snowflakePlan)
  //        (res.take(n), met)
  //      }
  //
  //    // The query has been executed
  //    val metaWithDisplayName = if (session.conn.hideInternalAlias) {
  //      Utils.getDisplayColumnNames(meta, plan.internalRenamedColumns)
  //    } else {
  //      meta
  //    }
  //    val colCount = meta.size
  //    val colWidth: Array[Int] = new Array[Int](colCount)
  //
  //    val header: Seq[String] = metaWithDisplayName.zipWithIndex.map {
  //      case (field, index) =>
  //        val name: String = field.name
  //        colWidth(index) = name.length
  //        name
  //    }
  //
  //    def splitLines(value: String): Seq[String] = {
  //      val lines = new ArrayBuffer[String]()
  //      var startIndex = 0
  //      value.zipWithIndex.foreach {
  //        case (c, index) =>
  //          if (c == '\n') {
  //            lines.append(value.substring(startIndex, index))
  //            startIndex = index + 1
  //          }
  //      }
  //      lines.append(value.substring(startIndex))
  //      lines
  //    }
  //
  //    def convertValueToString(value: Any): String =
  //      value match {
  //        case map: Map[_, _] =>
  //          map
  //            .map {
  //              case (key, value) => s"${convertValueToString(key)}:
  //              ${convertValueToString(value)}"
  //            }
  //            .mkString("{", ",", "}")
  //        case ba: Array[Byte] => s"'${DatatypeConverter.printHexBinary(ba)}'"
  //        case bytes: Array[java.lang.Byte] =>
  //          s"'${DatatypeConverter.printHexBinary(bytes.map(_.toByte))}'"
  //        case arr: Array[String] =>
  //          arr.mkString("[", ",", "]")
  //        case arr: Array[_] =>
  //          arr.map(convertValueToString).mkString("[", ",", "]")
  //        case arr: java.sql.Array =>
  //          arr.getArray().asInstanceOf[Array[_]]
  //          .map(convertValueToString).mkString("[", ",", "]")
  //        case _ => value.toString
  //      }
  //
  //    val body: Seq[Seq[String]] = result.flatMap(row => {
  //      // Value may contain multiple lines
  //      val lines: Seq[Seq[String]] = row.toSeq.zipWithIndex.map {
  //        case (value, index) =>
  //          val texts: Seq[String] = if (value != null) {
  //            // if the result contains multiple lines, split result string
  //            splitLines(convertValueToString(value))
  //          } else {
  //            Seq("NULL")
  //          }
  //          texts.foreach(str => {
  //            // update column width
  //            if (colWidth(index) < str.length) {
  //              colWidth(index) = str.length
  //            }
  //            if (colWidth(index) > maxWidth) {
  //              colWidth(index) = maxWidth
  //            }
  //          })
  //          texts
  //      }
  //      // max line number in this row
  //      val lineCount: Int = lines.map(_.size).max
  //      val result = new Array[Seq[String]](lineCount)
  //      (0 until lineCount).foreach(lineNumber => {
  //        val newLine = new Array[String](lines.size)
  //        lines.indices.foreach(colIndex => {
  //          // append empty string if no such line
  //          newLine(colIndex) =
  //            if (lines(colIndex).length > lineNumber) lines(colIndex)(lineNumber) else ""
  //        })
  //        result(lineNumber) = newLine.toSeq
  //      })
  //      result.toSeq
  //    })
  //
  //    // add 2 more spaces in each column
  //    (0 until colCount).foreach(index => colWidth(index) = colWidth(index) + 2)
  //
  //    val totalWidth: Int = colWidth.sum + colCount + 1
  //
  //    val line: String = (0 until totalWidth).map(x => "-").mkString + "\n"
  //
  //    def rowToString(row: Seq[String]): String =
  //      row
  //        .zip(colWidth)
  //        .map {
  //          case (str, size) =>
  //            if (str.length > maxWidth) {
  //              // if truncated, add ... to the end
  //              (str.take(maxWidth - 3) + "...").padTo(size, " ").mkString
  //            } else {
  //              str.padTo(size, " ").mkString
  //            }
  //        }
  //        .mkString("|", "|", "|") + "\n"
  //
  //    line + rowToString(header) + line + body.map(rowToString).mkString + line
  //  }

  /**
   * Creates a view that captures the computation expressed by this DataFrame.
   *
   * For `viewName`, you can include the database and schema name (i.e. specify a fully-qualified
   * name). If no database name or schema name are specified, the view will be created in the
   * current database or schema.
   *
   * `viewName` must be a valid
   * [[https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html Snowflake identifier]].
   *
   * @since 0.1.0
   * @group actions
   * @param viewName
   *   The name of the view to create or replace.
   */
  def createOrReplaceView(viewName: String): Unit = null

  /**
   * Creates a view that captures the computation expressed by this DataFrame.
   *
   * In `multipartIdentifer`, you can include the database and schema name to specify a
   * fully-qualified name. If no database name or schema name are specified, the view will be
   * created in the current database or schema.
   *
   * The view name must be a valid
   * [[https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html Snowflake identifier]].
   *
   * @since 0.5.0
   * @group actions
   * @param multipartIdentifier
   *   A sequence of strings that specifies the database name, schema name, and view name.
   */
  def createOrReplaceView(multipartIdentifier: Seq[String]): Unit = null

  /**
   * Creates a view that captures the computation expressed by this DataFrame.
   *
   * In `multipartIdentifer`, you can include the database and schema name to specify a
   * fully-qualified name. If no database name or schema name are specified, the view will be
   * created in the current database or schema.
   *
   * The view name must be a valid
   * [[https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html Snowflake identifier]].
   *
   * @since 0.5.0
   * @group actions
   * @param multipartIdentifier
   *   A list of strings that specifies the database name, schema name, and view name.
   */
  def createOrReplaceView(multipartIdentifier: java.util.List[String]): Unit = null

  /**
   * Creates a temporary view that returns the same results as this DataFrame.
   *
   * You can use the view in subsequent SQL queries and statements during the current session. The
   * temporary view is only available in the session in which it is created.
   *
   * For `viewName`, you can include the database and schema name (i.e. specify a fully-qualified
   * name). If no database name or schema name are specified, the view will be created in the
   * current database or schema.
   *
   * `viewName` must be a valid
   * [[https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html Snowflake identifier]].
   *
   * @since 0.4.0
   * @group actions
   * @param viewName
   *   The name of the view to create or replace.
   */
  def createOrReplaceTempView(viewName: String): Unit = null

  /**
   * Creates a temporary view that returns the same results as this DataFrame.
   *
   * You can use the view in subsequent SQL queries and statements during the current session. The
   * temporary view is only available in the session in which it is created.
   *
   * In `multipartIdentifer`, you can include the database and schema name to specify a
   * fully-qualified name. If no database name or schema name are specified, the view will be
   * created in the current database or schema.
   *
   * The view name must be a valid
   * [[https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html Snowflake identifier]].
   *
   * @since 0.5.0
   * @group actions
   * @param multipartIdentifier
   *   A sequence of strings that specify the database name, schema name, and view name.
   */
  def createOrReplaceTempView(multipartIdentifier: Seq[String]): Unit = null

  /**
   * Creates a temporary view that returns the same results as this DataFrame.
   *
   * You can use the view in subsequent SQL queries and statements during the current session. The
   * temporary view is only available in the session in which it is created.
   *
   * In `multipartIdentifer`, you can include the database and schema name to specify a
   * fully-qualified name. If no database name or schema name are specified, the view will be
   * created in the current database or schema.
   *
   * The view name must be a valid
   * [[https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html Snowflake identifier]].
   *
   * @since 0.5.0
   * @group actions
   * @param multipartIdentifier
   *   A list of strings that specify the database name, schema name, and view name.
   */
  def createOrReplaceTempView(multipartIdentifier: java.util.List[String]): Unit = null

  /**
   * Executes the query representing this DataFrame and returns the first row of results.
   *
   * @group actions
   * @since 0.2.0
   * @return
   *   The first [[Row]], if the row exists. Otherwise, returns `None`.
   */
  def first(): Option[Row] = null

  /**
   * Executes the query representing this DataFrame and returns the first {@code n} rows of the
   * results.
   *
   * @group actions
   * @since 0.2.0
   * @param n
   *   The number of rows to return.
   * @return
   *   An Array of the first {@code n} [[Row]] objects. If {@code n} is negative or larger than the
   *   number of rows in the results, returns all rows in the results.
   */
  def first(n: Int): Array[Row] = null

  //  /**
  //   * Returns a [[DataFrameNaFunctions]] object that
  //   provides functions for handling missing values
  //   * in the DataFrame.
  //   *
  //   * @group basic
  //   * @since 0.2.0
  //   */
  //  lazy val na: DataFrameNaFunctions = new DataFrameNaFunctions(this)
  //
  //  /**
  //   * Returns a [[DataFrameStatFunctions]] object that provides statistic functions.
  //   *
  //   * @group basic
  //   * @since 0.2.0
  //   */
  //  lazy val stat: DataFrameStatFunctions = new DataFrameStatFunctions(this)

  /**
   * Returns a new DataFrame with a sample of N rows from the underlying DataFrame.
   *
   * NOTE:
   *
   *   - If the row count in the DataFrame is larger than the requested number of rows, the method
   *     returns a DataFrame containing the number of requested rows.
   *   - If the row count in the DataFrame is smaller than the requested number of rows, the method
   *     returns a DataFrame containing all rows.
   *
   * @param num
   *   The number of rows to sample in the range of 0 to 1,000,000.
   * @group transform
   * @since 0.2.0
   * @return
   *   A [[DataFrame]] containing the sample of {@code num} rows.
   */
  def sample(num: Long): DataFrame = null

  /**
   * Returns a new DataFrame that contains a sampling of rows from the current DataFrame.
   *
   * NOTE:
   *
   *   - The number of rows returned may be close to (but not exactly equal to)
   *     {@code (probabilityFraction * totalRowCount)} .
   *   - The Snowflake [[https://docs.snowflake.com/en/sql-reference/constructs/sample.html SAMPLE]]
   *     function supports specifying 'probability' as a percentage number. The range of
   *     'probability' is {@code [0.0, 100.0]} . The conversion formula is
   *     {@code probability = probabilityFraction * 100} .
   *
   * @param probabilityFraction
   *   The fraction of rows to sample. This must be in the range of `0.0` to `1.0`.
   * @group transform
   * @since 0.2.0
   * @return
   *   A [[DataFrame]] containing the sample of rows.
   */
  def sample(probabilityFraction: Double): DataFrame = null

  /**
   * Randomly splits the current DataFrame into separate DataFrames, using the specified weights.
   *
   * NOTE:
   *
   *   - If only one weight is specified, the returned DataFrame array only includes the current
   *     DataFrame.
   *   - If multiple weights are specified, the current DataFrame will be cached before being split.
   *
   * @param weights
   *   Weights to use for splitting the DataFrame. If the weights don't add up to 1, the weights
   *   will be normalized.
   * @group actions
   * @since 0.2.0
   * @return
   *   A list of [[DataFrame]] objects
   */
  def randomSplit(weights: Array[Double]): Array[DataFrame] = null

  /**
   * Flattens (explodes) compound values into multiple rows (similar to the SQL
   * [[https://docs.snowflake.com/en/sql-reference/functions/flatten.html FLATTEN]] function).
   *
   * The `flatten` method adds the following
   * [[https://docs.snowflake.com/en/sql-reference/functions/flatten.html#output columns]] to the
   * returned DataFrame:
   *
   *   - SEQ
   *   - KEY
   *   - PATH
   *   - INDEX
   *   - VALUE
   *   - THIS
   *
   * If {@code this} DataFrame also has columns with the names above, you can disambiguate the
   * columns by using the {@code this("value")} syntax.
   *
   * For example, if the current DataFrame has a column named `value`:
   * {{{
   *   val table1 = session.sql(
   *     "select parse_json(value) as value from values('[1,2]') as T(value)")
   *   val flattened = table1.flatten(table1("value"))
   *   flattened.select(table1("value"), flattened("value").as("newValue")).show()
   * }}}
   *
   * @param input
   *   The expression that will be unseated into rows. The expression must be of data type VARIANT,
   *   OBJECT, or ARRAY.
   * @group transform
   * @return
   *   A [[DataFrame]] containing the flattened values.
   * @since 0.2.0
   */
  def flatten(input: Column): DataFrame = null

  /**
   * Flattens (explodes) compound values into multiple rows (similar to the SQL
   * [[https://docs.snowflake.com/en/sql-reference/functions/flatten.html FLATTEN]] function).
   *
   * The `flatten` method adds the following
   * [[https://docs.snowflake.com/en/sql-reference/functions/flatten.html#output columns]] to the
   * returned DataFrame:
   *
   *   - SEQ
   *   - KEY
   *   - PATH
   *   - INDEX
   *   - VALUE
   *   - THIS
   *
   * If {@code this} DataFrame also has columns with the names above, you can disambiguate the
   * columns by using the {@code this("value")} syntax.
   *
   * For example, if the current DataFrame has a column named `value`:
   * {{{
   *   val table1 = session.sql(
   *     "select parse_json(value) as value from values('[1,2]') as T(value)")
   *   val flattened = table1.flatten(table1("value"), "", outer = false,
   *     recursive = false, "both")
   *   flattened.select(table1("value"), flattened("value").as("newValue")).show()
   * }}}
   *
   * @param input
   *   The expression that will be unseated into rows. The expression must be of data type VARIANT,
   *   OBJECT, or ARRAY.
   * @param path
   *   The path to the element within a VARIANT data structure which needs to be flattened. Can be a
   *   zero-length string (i.e. empty path) if the outermost element is to be flattened.
   * @param outer
   *   If FALSE, any input rows that cannot be expanded, either because they cannot be accessed in
   *   the path or because they have zero fields or entries, are completely omitted from the output.
   *   Otherwise, exactly one row is generated for zero-row expansions (with NULL in the KEY, INDEX,
   *   and VALUE columns).
   * @param recursive
   *   If FALSE, only the element referenced by PATH is expanded. Otherwise, the expansion is
   *   performed for all sub-elements recursively.
   * @param mode
   *   Specifies whether only OBJECT, ARRAY, or BOTH should be flattened.
   * @group transform
   * @return
   *   A [[DataFrame]] containing the flattened values.
   * @since 0.2.0
   */
  def flatten(
      input: Column,
      path: String,
      outer: Boolean,
      recursive: Boolean,
      mode: String): DataFrame = null

}
//
// /**
// * A DataFrame that returns cached data. Repeated invocations of actions on
// * this type of dataframe are guaranteed to produce the same results.
// * It is returned from `cacheResult` functions (e.g. [[DataFrame.cacheResult]]).
// *
// * @since 0.4.0
// */
// class HasCachedResult private[snowpark] (
//                                          override private[snowpark] val session: Session,
//                                          override private[snowpark] val plan: LogicalPlan,
//                                          override private[snowpark] val methodChain: Seq[String])
//  extends DataFrame(session, plan, methodChain) {
//
//  /**
//   * Caches the content of this DataFrame to create a new cached DataFrame.
//   *
//   * All subsequent operations on the returned cached DataFrame are performed on the cached data
//   * and have no effect on the original DataFrame.
//   *
//   * @since 1.5.0
//   * @group actions
//   * @return A [[HasCachedResult]]
//   */
//  override def cacheResult(): HasCachedResult = action("cacheResult") {
//    // cacheResult function of HashCachedResult returns a clone of this
//    // HashCachedResult DataFrame instead of to cache this DataFrame again.
//    new HasCachedResult(session, snowflakePlan.clone, Seq())
//  }
// }
//
// /**
// * Provides APIs to execute DataFrame actions asynchronously.
// *
// * @since 0.11.0
// */
// class DataFrameAsyncActor private[snowpark] (df: DataFrame) {
//
//  /**
//   * Executes [[DataFrame.collect]] asynchronously.
//   *
//   * @return A [[TypedAsyncJob]] object that you can use to check the status of the action
//   *         and get the results.
//   * @since 0.11.0
//   */
//  def collect(): TypedAsyncJob[Array[Row]] = action("collect") {
//    df.session.conn.executeAsync[Array[Row]](df.snowflakePlan)
//  }
//
//  /**
//   * Executes [[DataFrame.toLocalIterator]] asynchronously.
//   *
//   * @return A [[TypedAsyncJob]] object that you can use to check the status of the action
//   *         and get the results.
//   * @since 0.11.0
//   */
//  def toLocalIterator(): TypedAsyncJob[Iterator[Row]] = action("toLocalIterator") {
//    df.session.conn.executeAsync[Iterator[Row]](df.snowflakePlan)
//  }
//
//  /**
//   * Executes [[DataFrame.count]] asynchronously.
//   *
//   * @return A [[TypedAsyncJob]] object that you can use to check the status of the action
//   *         and get the results.
//   * @since 0.11.0
//   */
//  def count(): TypedAsyncJob[Long] = action("count") {
//    df.session.conn.executeAsync[Long](df.agg(("*", "count")).snowflakePlan)
//  }
//
//  @inline protected def action[T](funcName: String)(func: => T): T = {
//    OpenTelemetry.action("DataFrameAsyncActor", funcName, df.methodChainString + ".async")(func)
//  }
