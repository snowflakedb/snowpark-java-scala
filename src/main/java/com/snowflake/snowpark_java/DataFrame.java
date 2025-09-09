package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;
import com.snowflake.snowpark.internal.Logging;
import com.snowflake.snowpark_java.types.InternalUtils;
import com.snowflake.snowpark_java.types.StructType;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * Represents a lazily-evaluated relational dataset that contains a collection of {@code Row}
 * objects with columns defined by a schema (column name and type).
 *
 * <p>A {@code DataFrame} is considered lazy because it encapsulates the computation or query
 * required to produce a relational dataset. The computation is not performed until you call a
 * method that performs an action (e.g. {@code collect}).
 *
 * @since 0.8.0
 */
public class DataFrame extends Logging implements Cloneable {
  private final com.snowflake.snowpark.DataFrame df;

  DataFrame(com.snowflake.snowpark.DataFrame df) {
    this.df = df;
  }

  /**
   * Retrieves the definition of the columns in this DataFrame (the "relational schema" for the
   * DataFrame).
   *
   * @return A StructType object representing the DataFrame's schema
   * @since 0.9.0
   */
  public StructType schema() {
    return InternalUtils.createStructType(df.schema());
  }

  /**
   * Caches the content of this DataFrame to create a new cached DataFrame.
   *
   * <p>All subsequent operations on the returned cached DataFrame are performed on the cached data
   * and have no effect on the original DataFrame.
   *
   * @since 0.12.0
   * @return A HasCachedResult
   */
  public HasCachedResult cacheResult() {
    return new HasCachedResult(df.cacheResult());
  }

  /**
   * Prints the list of queries that will be executed to evaluate this DataFrame. Prints the query
   * execution plan if only one SELECT/DML/DDL statement will be executed.
   *
   * <p>For more information about the query execution plan, see the <a
   * href="https://docs.snowflake.com/en/sql-reference/sql/explain.html">EXPLAIN</a> command.
   *
   * @since 0.12.0
   */
  public void explain() {
    df.explain();
  }

  /**
   * Creates a new DataFrame containing the data in the current DataFrame but in columns with the
   * specified names. The number of column names that you pass in must match the number of columns
   * in the current DataFrame.
   *
   * @since 0.12.0
   * @param colNames A list of column names.
   * @return A DataFrame
   */
  public DataFrame toDF(String... colNames) {
    return new DataFrame(df.toDF(JavaUtils.stringArrayToStringSeq(colNames)));
  }

  /**
   * Returns a DataFrame with an additional column with the specified name (`colName`). The column
   * is computed by using the specified expression (`col`).
   *
   * <p>If a column with the same name already exists in the DataFrame, that column is replaced by
   * the new column.
   *
   * <p>This example adds a new column named `mean_price` that contains the mean of the existing
   * `price` column in the DataFrame.
   *
   * <p>{{{ DataFrame dfWithMeanPriceCol = df.withColumn("mean_price",
   * Functions.mean(df.col("price"))); }}}
   *
   * @since 0.9.0
   * @param colName The name of the column to add or replace.
   * @param col The Column to add or replace.
   * @return A DataFrame
   */
  public DataFrame withColumn(String colName, Column col) {
    return new DataFrame(this.df.withColumn(colName, col.toScalaColumn()));
  }

  /**
   * Returns a DataFrame with additional columns with the specified names (`colNames`). The columns
   * are computed by using the specified expressions (`cols`).
   *
   * <p>If columns with the same names already exist in the DataFrame, those columns are replaced by
   * the new columns.
   *
   * <p>This example adds new columns named `mean_price` and `avg_price` that contain the mean and
   * average of the existing `price` column.
   *
   * <pre><code>
   *     DataFrame dfWithAddedColumns = df.withColumns(
   *       new String[]{"mean_price", "avg_price"},
   *       new Column[]{Functions.mean(df.col("price")),
   *         Functions.avg(df.col("price"))}
   *     );
   * </code></pre>
   *
   * @since 0.12.0
   * @param colNames A list of the names of the columns to add or replace.
   * @param values A list of the Column objects to add or replace.
   * @return A DataFrame
   */
  public DataFrame withColumns(String[] colNames, Column[] values) {
    return new DataFrame(
        df.withColumns(
            JavaUtils.stringArrayToStringSeq(colNames),
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(values))));
  }

  /**
   * Returns a DataFrame with the specified column `col` renamed as `newName`.
   *
   * <p>This example renames the column `A` as `NEW_A` in the DataFrame.
   *
   * <pre>{@code
   * DataFrame df = session.sql("select 1 as A, 2 as B");
   * DateFrame dfRenamed = df.rename("NEW_A", df.col("A"));
   * }</pre>
   *
   * @since 0.12.0
   * @param newName The new name for the column
   * @param col The Column to be renamed
   * @return A DataFrame
   */
  public DataFrame rename(String newName, Column col) {
    return new DataFrame(df.rename(newName, col.toScalaColumn()));
  }

  /**
   * Generates a new DataFrame with the specified Column expressions as output (similar to SELECT in
   * SQL). Only the Columns specified as arguments will be present in the resulting DataFrame.
   *
   * <p>You can use any Column expression.
   *
   * <p>For example:
   *
   * <pre>{@code
   * import com.snowflake.snowpark_java.Functions;
   *
   * DataFrame dfSelected =
   *   df.select(df.col("col1"), Functions.lit("abc"), df.col("col1").plus(df.col("col2")));
   * }</pre>
   *
   * @param columns The arguments of this select function
   * @return The result DataFrame object
   * @since 0.9.0
   */
  public DataFrame select(Column... columns) {
    return new DataFrame(df.select(Column.toScalaColumnArray(columns)));
  }

  /**
   * Returns a new DataFrame with a subset of named columns (similar to SELECT in SQL).
   *
   * <p>For example:
   *
   * <p>{@code DataFrame dfSelected = df.select("col1", "col2", "col3");}
   *
   * @since 0.12.0
   * @param columnNames A list of the names of columns to return.
   * @return A DataFrame
   */
  public DataFrame select(String... columnNames) {
    return new DataFrame(df.select(columnNames));
  }

  /**
   * Returns a new DataFrame that excludes the columns with the specified names from the output.
   *
   * <p>This is functionally equivalent to calling {@code select()} and passing in all columns
   * except the ones to exclude.
   *
   * @since 0.12.0
   * @param columns An array of columns to exclude.
   * @throws com.snowflake.snowpark.SnowparkClientException if the resulting DataFrame contains no
   *     output columns.
   * @return A DataFrame
   */
  public DataFrame drop(Column... columns) {
    return new DataFrame(df.drop(Column.toScalaColumnArray(columns)));
  }

  /**
   * Returns a new DataFrame that excludes the columns with the specified names from the output.
   *
   * <p>This is functionally equivalent to calling {@code select()} and passing in all columns
   * except the ones to exclude.
   *
   * @since 0.12.0
   * @param columnNames An array of the names of columns to exclude.
   * @throws com.snowflake.snowpark.SnowparkClientException if the resulting DataFrame contains no
   *     output columns.
   * @return A DataFrame
   */
  public DataFrame drop(String... columnNames) {
    return new DataFrame(df.drop(columnNames));
  }

  /**
   * Filters rows based on the specified conditional expression (similar to WHERE in SQL).
   *
   * <p>For example:
   *
   * <pre>{@code
   * import com.snowflake.snowpark_java.Functions;
   *
   * DataFrame dfFiltered =
   *   df.filter(df.col("colA").gt(Functions.lit(1)));
   * }</pre>
   *
   * @param condition The filter condition defined as an expression on columns
   * @return A filtered DataFrame
   * @since 0.9.0
   */
  public DataFrame filter(Column condition) {
    return new DataFrame(df.filter(condition.toScalaColumn()));
  }

  /**
   * Filters rows based on the specified conditional expression (similar to WHERE in SQL). This is
   * equivalent to calling filter function.
   *
   * <p>For example:
   *
   * <pre>{@code
   * import com.snowflake.snowpark_java.Functions;
   *
   * DataFrame dfFiltered =
   *   df.where(df.col("colA").gt(Functions.lit(1)));
   * }</pre>
   *
   * @param condition The filter condition defined as an expression on columns
   * @return A filtered DataFrame
   * @since 0.9.0
   */
  public DataFrame where(Column condition) {
    return filter(condition);
  }

  /**
   * Aggregate the data in the DataFrame. Use this method if you don't need to group the data
   * (`groupBy`).
   *
   * <p>For the input value, pass in expressions that apply aggregation functions to columns
   * (functions that are defined in the {@code functions} object).
   *
   * <p>The following example calculates the maximum value of the `num_sales` column and the mean
   * value of the `price` column:
   *
   * <p>For example:
   *
   * <p>{@code df.agg(Functions.max(df.col("num_sales")), Functions.mean(df.col("price")))}
   *
   * @since 0.12.0
   * @param exprs A list of expressions on columns.
   * @return A DataFrame
   */
  public DataFrame agg(Column... exprs) {
    return new DataFrame(df.agg(Column.toScalaColumnArray(exprs)));
  }

  /**
   * Returns a new DataFrame that contains only the rows with distinct values from the current
   * DataFrame.
   *
   * <p>This is equivalent to performing a SELECT DISTINCT in SQL.
   *
   * @since 0.12.0
   * @return A DataFrame
   */
  public DataFrame distinct() {
    return new DataFrame(df.distinct());
  }

  /**
   * Creates a new DataFrame by removing duplicated rows on given subset of columns. If no subset of
   * columns specified, this function is same as {@code distinct()} function. The result is
   * non-deterministic when removing duplicated rows from the subset of columns but not all columns.
   * For example: Supposes we have a DataFrame `df`, which contains three rows (a, b, c): (1, 1, 1),
   * (1, 1, 2), (1, 2, 3) The result of df.dropDuplicates("a", "b") can be either (1, 1, 1), (1, 2,
   * 3) or (1, 1, 2), (1, 2, 3)
   *
   * @since 0.12.0
   * @param colNames A list of column names
   * @return A DataFrame
   */
  public DataFrame dropDuplicates(String... colNames) {
    return new DataFrame(df.dropDuplicates(JavaUtils.stringArrayToStringSeq(colNames)));
  }

  /**
   * Returns a new DataFrame that contains all the rows in the current DataFrame and another
   * DataFrame (`other`), excluding any duplicate rows. Both input DataFrames must contain the same
   * number of columns.
   *
   * @since 0.9.0
   * @param other The other DataFrame that contains the rows to include.
   * @return A DataFrame
   */
  public DataFrame union(DataFrame other) {
    return new DataFrame(this.df.union(other.df));
  }

  /**
   * Returns a new DataFrame that contains all the rows in the current DataFrame and another
   * DataFrame (`other`), including any duplicate rows. Both input DataFrames must contain the same
   * number of columns.
   *
   * <p>For example:
   *
   * <p>{@code DataFrame df1and2 = df1.unionAll(df2);}
   *
   * @since 0.12.0
   * @param other The other DataFrame that contains the rows to include.
   * @return A DataFrame
   */
  public DataFrame unionAll(DataFrame other) {
    return new DataFrame(df.unionAll(other.getScalaDataFrame()));
  }

  /**
   * Returns a new DataFrame that contains all the rows in the current DataFrame and another
   * DataFrame (`other`), excluding any duplicate rows.
   *
   * <p>This method matches the columns in the two DataFrames by their names, not by their
   * positions. The columns in the other DataFrame are rearranged to match the order of columns in
   * the current DataFrame.
   *
   * @since 0.12.0
   * @param other The other DataFrame that contains the rows to include.
   * @return A DataFrame
   */
  public DataFrame unionByName(DataFrame other) {
    return new DataFrame(df.unionByName(other.getScalaDataFrame()));
  }

  /**
   * Returns a new DataFrame that contains all the rows in the current DataFrame and another
   * DataFrame (`other`), including any duplicate rows.
   *
   * <p>This method matches the columns in the two DataFrames by their names, not by their
   * positions. The columns in the other DataFrame are rearranged to match the order of columns in
   * the current DataFrame.
   *
   * @since 0.12.0
   * @param other The other DataFrame that contains the rows to include.
   * @return A DataFrame
   */
  public DataFrame unionAllByName(DataFrame other) {
    return new DataFrame(df.unionAllByName(other.getScalaDataFrame()));
  }

  /**
   * Returns a new DataFrame that contains the intersection of rows from the current DataFrame and
   * another DataFrame (`other`). Duplicate rows are eliminated.
   *
   * @since 0.12.0
   * @param other The other DataFrame that contains the rows to use for the intersection.
   * @return A DataFrame
   */
  public DataFrame intersect(DataFrame other) {
    return new DataFrame(df.intersect(other.getScalaDataFrame()));
  }

  /**
   * Returns a new DataFrame that contains all the rows from the current DataFrame except for the
   * rows that also appear in another DataFrame (`other`). Duplicate rows are eliminated.
   *
   * @since 0.12.0
   * @param other The DataFrame that contains the rows to exclude.
   * @return A DataFrame
   */
  public DataFrame except(DataFrame other) {
    return new DataFrame(df.except(other.getScalaDataFrame()));
  }

  /**
   * Returns a clone of this DataFrame.
   *
   * @since 0.12.0
   * @return A DataFrame
   */
  @Override
  public DataFrame clone() {
    // invoke super.clone to remove compiler warning
    try {
      super.clone();
    } catch (CloneNotSupportedException e) {
      logWarning(e.getMessage());
    }
    return new DataFrame(df.clone());
  }

  /**
   * Performs a default inner join of the current DataFrame and another DataFrame (`right`).
   *
   * <p>Because this method does not specify a join condition, the returned DataFrame is a cartesian
   * product of the two DataFrames.
   *
   * <p>If the current and `right` DataFrames have columns with the same name, and you need to refer
   * to one of these columns in the returned DataFrame, use the {@code col} function on the current
   * or `right` DataFrame to disambiguate references to these columns.
   *
   * @since 0.1.0
   * @param right The other DataFrame to join.
   * @return A DataFrame
   */
  public DataFrame join(DataFrame right) {
    return new DataFrame(df.join(right.getScalaDataFrame()));
  }

  /**
   * Performs a default inner join of the current DataFrame and another DataFrame (`right`) on a
   * column (`usingColumn`).
   *
   * <p>The method assumes that the `usingColumn` column has the same meaning in the left and right
   * DataFrames.
   *
   * <p>For example: {@code left.join(right, "col")}
   *
   * @since 0.12.0
   * @param right The other DataFrame to join.
   * @param usingColumn The name of the column to use for the join.
   * @return A DataFrame
   */
  public DataFrame join(DataFrame right, String usingColumn) {
    return new DataFrame(df.join(right.getScalaDataFrame(), usingColumn));
  }

  /**
   * Performs a default inner join of the current DataFrame and another DataFrame (`right`) on a
   * list of columns (`usingColumns`).
   *
   * <p>The method assumes that the columns in `usingColumns` have the same meaning in the left and
   * right DataFrames.
   *
   * <p>For example: <code>left.join(right, new String[]{"col1", "col2"})</code>
   *
   * @since 0.12.0
   * @param right The other DataFrame to join.
   * @param usingColumns A list of the names of the columns to use for the join.
   * @return A DataFrame
   */
  public DataFrame join(DataFrame right, String[] usingColumns) {
    return new DataFrame(
        df.join(right.getScalaDataFrame(), JavaUtils.stringArrayToStringSeq(usingColumns)));
  }

  /**
   * Performs a join of the specified type (`joinType`) with the current DataFrame and another
   * DataFrame (`right`) on a list of columns (`usingColumns`).
   *
   * <p>The method assumes that the columns in `usingColumns` have the same meaning in the left and
   * right DataFrames.
   *
   * <p>For example:
   *
   * <pre>{@code
   * left.join(right, new String[]{"col"}, "left");
   * left.join(right, new String[]{"col1", "col2}, "outer");
   * }</pre>
   *
   * @since 0.12.0
   * @param right The other DataFrame to join.
   * @param usingColumns A list of the names of the columns to use for the join.
   * @param joinType The type of join (e.g. {@code "right"}, {@code "outer"}, etc.).
   * @return A DataFrame
   */
  public DataFrame join(DataFrame right, String[] usingColumns, String joinType) {
    return new DataFrame(
        df.join(
            right.getScalaDataFrame(), JavaUtils.stringArrayToStringSeq(usingColumns), joinType));
  }

  /**
   * Performs a default inner join of the current DataFrame and another DataFrame (`right`) using
   * the join condition specified in an expression (`joinExpr`).
   *
   * <p>To disambiguate columns with the same name in the left DataFrame and right DataFrame, use
   * the {@code col()} method of each DataFrame. You can use this approach to disambiguate columns
   * in the `joinExpr` parameter and to refer to columns in the returned DataFrame.
   *
   * <p>For example: {@code df1.join(df2, df1.col("col1").equal_to(df2.col("col2")))}
   *
   * <p>If you need to join a DataFrame with itself, keep in mind that there is no way to
   * distinguish between columns on the left and right sides in a join expression. For example:
   *
   * <p>{@code df.join(df, df.col("a").equal_to(df.col("b")))} As a workaround, you can either
   * construct the left and right DataFrames separately, or you can call a {@code join(DataFrame,
   * String[])} method that allows you to pass in 'usingColumns' parameter.
   *
   * @since 0.12.0
   * @param right The other DataFrame to join.
   * @param joinExpr Expression that specifies the join condition.
   * @return A DataFrame
   */
  public DataFrame join(DataFrame right, Column joinExpr) {
    return new DataFrame(df.join(right.getScalaDataFrame(), joinExpr.toScalaColumn()));
  }

  /**
   * Performs a join of the specified type (`joinType`) with the current DataFrame and another
   * DataFrame (`right`) using the join condition specified in an expression (`joinExpr`).
   *
   * <p>To disambiguate columns with the same name in the left DataFrame and right DataFrame, use
   * the {@code col()} method of each DataFrame. You can use this approach to disambiguate columns
   * in the `joinExpr` parameter and to refer to columns in the returned DataFrame.
   *
   * <p>For example: {@code df1.join(df2, df1.col("col1").equal_to(df2.col("col2")))}
   *
   * <p>If you need to join a DataFrame with itself, keep in mind that there is no way to
   * distinguish between columns on the left and right sides in a join expression. For example:
   *
   * <p>{@code df.join(df, df.col("a").equal_to(df.col("b")))} As a workaround, you can either
   * construct the left and right DataFrames separately, or you can call a {@code join(DataFrame,
   * String[])} method that allows you to pass in 'usingColumns' parameter.
   *
   * @since 0.12.0
   * @param right The other DataFrame to join.
   * @param joinExpr Expression that specifies the join condition.
   * @param joinType The type of join (e.g. {@code "right"}, {@code "outer"}, etc.).
   * @return A DataFrame
   */
  public DataFrame join(DataFrame right, Column joinExpr, String joinType) {
    return new DataFrame(df.join(right.getScalaDataFrame(), joinExpr.toScalaColumn(), joinType));
  }

  /**
   * Performs a cross join, which returns the cartesian product of the current DataFrame and another
   * DataFrame (`right`).
   *
   * <p>If the current and `right` DataFrames have columns with the same name, and you need to refer
   * to one of these columns in the returned DataFrame, use the {@code col} function on the current
   * or `right` DataFrame to disambiguate references to these columns.
   *
   * @since 0.12.0
   * @param right The other DataFrame to join.
   * @return A DataFrame
   */
  public DataFrame crossJoin(DataFrame right) {
    return new DataFrame(df.crossJoin(right.getScalaDataFrame()));
  }

  /**
   * Performs a natural join (a default inner join) of the current DataFrame and another DataFrame
   * (`right`).
   *
   * @since 0.12.0
   * @param right The other DataFrame to join.
   * @return A DataFrame
   */
  public DataFrame naturalJoin(DataFrame right) {
    return new DataFrame(df.naturalJoin(right.getScalaDataFrame()));
  }

  /**
   * Performs a natural join of the specified type (`joinType`) with the current DataFrame and
   * another DataFrame (`right`).
   *
   * @since 0.12.0
   * @param right The other DataFrame to join.
   * @param joinType The type of join (e.g. {@code "right"}, {@code "outer"}, etc.).
   * @return A DataFrame
   */
  public DataFrame naturalJoin(DataFrame right, String joinType) {
    return new DataFrame(df.naturalJoin(right.getScalaDataFrame(), joinType));
  }

  /**
   * Sorts a DataFrame by the specified expressions (similar to ORDER BY in SQL).
   *
   * <p>For example:
   *
   * <pre>{@code
   * DataFrame dfSorted = df.sort(df.col("colA"), df.col("colB").desc);
   * }</pre>
   *
   * @param sortExprs A list of Column expressions for sorting the DataFrame
   * @return The sorted DataFrame
   * @since 0.9.0
   */
  public DataFrame sort(Column... sortExprs) {
    return new DataFrame(df.sort(Column.toScalaColumnArray(sortExprs)));
  }

  /**
   * Sorts a DataFrame by the specified column names in ascending order (similar to ORDER BY in
   * SQL).
   *
   * <p>Example:
   *
   * <pre>{@code
   * DataFrame df = session.createDataFrame(
   *   new Row[] {
   *     Row.create("Alice", 30, "Manager"),
   *     Row.create("Charlie", 25, "Designer"),
   *     Row.create("Bob", 25, "Engineer"),
   *   },
   *   StructType.create(
   *     new StructField("name", DataTypes.StringType),
   *     new StructField("age", DataTypes.IntegerType),
   *     new StructField("role", DataTypes.StringType)
   *   )
   * );
   *
   * df.sort("role").show();
   * ------------------------------
   * |"NAME"   |"AGE"  |"ROLE"    |
   * ------------------------------
   * |Charlie  |25     |Designer  |
   * |Bob      |25     |Engineer  |
   * |Alice    |30     |Manager   |
   * ------------------------------
   *
   * df.sort("age", "name").show();
   * ------------------------------
   * |"NAME"   |"AGE"  |"ROLE"    |
   * ------------------------------
   * |Bob      |25     |Engineer  |
   * |Charlie  |25     |Designer  |
   * |Alice    |30     |Manager   |
   * ------------------------------
   * }</pre>
   *
   * @param first The first column name for sorting the DataFrame.
   * @param remaining Additional column names for sorting the DataFrame.
   * @return A {@code DataFrame} with rows sorted according to the specified column names.
   * @since 1.17.0
   */
  public DataFrame sort(String first, String... remaining) {
    return new DataFrame(this.df.sort(first, JavaUtils.stringArrayToStringSeq(remaining)));
  }

  /**
   * Returns a new DataFrame that contains at most `n` rows from the current DataFrame (similar to
   * LIMIT in SQL).
   *
   * <p>Note that this is a transformation method and not an action method.
   *
   * @since 0.12.0
   * @param n Number of rows to return.
   * @return A DataFrame
   */
  public DataFrame limit(int n) {
    return new DataFrame(df.limit(n));
  }

  /**
   * Groups rows by the columns specified by expressions (similar to GROUP BY in SQL).
   *
   * @since 0.9.0
   * @param cols An array of expressions on columns.
   * @return A RelationalGroupedDataFrame that you can use to perform aggregations on each group of
   *     data.
   */
  public RelationalGroupedDataFrame groupBy(Column... cols) {
    return new RelationalGroupedDataFrame(this.df.groupBy(Column.toScalaColumnArray(cols)));
  }

  /**
   * Groups rows by the columns specified by name (similar to GROUP BY in SQL).
   *
   * <p>This method returns a RelationalGroupedDataFrame that you can use to perform aggregations on
   * each group of data.
   *
   * @since 1.1.0
   * @param colNames A list of the names of columns to group by.
   * @return A RelationalGroupedDataFrame that you can use to perform aggregations on each group of
   *     data.
   */
  public RelationalGroupedDataFrame groupBy(String... colNames) {
    return new RelationalGroupedDataFrame(this.df.groupBy(colNames));
  }

  /**
   * Performs an SQL <a
   * href="https://docs.snowflake.com/en/sql-reference/constructs/group-by-rollup.html">GROUP BY
   * ROLLUP</a> on the DataFrame.
   *
   * @since 1.1.0
   * @param cols A list of expressions on columns.
   * @return A RelationalGroupedDataFrame that you can use to perform aggregations on each group of
   *     data.
   */
  public RelationalGroupedDataFrame rollup(Column... cols) {
    return new RelationalGroupedDataFrame(this.df.rollup(Column.toScalaColumnArray(cols)));
  }

  /**
   * Performs an SQL <a
   * href="https://docs.snowflake.com/en/sql-reference/constructs/group-by-rollup.html">GROUP BY
   * ROLLUP</a> on the DataFrame.
   *
   * @since 1.1.0
   * @param colNames A list of column names.
   * @return A RelationalGroupedDataFrame that you can use to perform aggregations on each group of
   *     data.
   */
  public RelationalGroupedDataFrame rollup(String... colNames) {
    return new RelationalGroupedDataFrame(this.df.rollup(colNames));
  }

  /**
   * Performs an SQL <a
   * href="https://docs.snowflake.com/en/sql-reference/constructs/group-by-rollup.html">GROUP BY
   * CUBE</a> on the DataFrame.
   *
   * @since 0.9.0
   * @param cols A list of expressions for columns to use.
   * @return A RelationalGroupedDataFrame
   */
  public RelationalGroupedDataFrame cube(Column... cols) {
    return new RelationalGroupedDataFrame(this.df.cube(Column.toScalaColumnArray(cols)));
  }

  /**
   * Performs an SQL <a
   * href="https://docs.snowflake.com/en/sql-reference/constructs/group-by-rollup.html">GROUP BY
   * CUBE</a> on the DataFrame.
   *
   * @since 1.1.0
   * @param colNames A list of column names.
   * @return A RelationalGroupedDataFrame
   */
  public RelationalGroupedDataFrame cube(String... colNames) {
    return new RelationalGroupedDataFrame(this.df.cube(JavaUtils.stringArrayToStringSeq(colNames)));
  }

  /**
   * Performs an SQL <a
   * href="https://docs.snowflake.com/en/sql-reference/constructs/group-by-rollup.html">GROUP BY
   * GROUPING SETS</a> on the DataFrame.
   *
   * <p>GROUP BY GROUPING SETS is an extension of the GROUP BY clause that allows computing multiple
   * group-by clauses in a single statement. The group set is a set of dimension columns.
   *
   * <p>GROUP BY GROUPING SETS is equivalent to the UNION of two or more GROUP BY operations in the
   * same result set:
   *
   * <p>{@code df.groupByGroupingSets(GroupingSets.create(Set.of(df.col("a"))))} is equivalent to
   * {@code df.groupBy("a")}
   *
   * <p>and
   *
   * <p>{@code df.groupByGroupingSets(GroupingSets.create(Set.of(df.col("a")),
   * Set.of(df.col("b"))))} is equivalent to {@code df.groupBy("a") 'union' df.groupBy("b")}
   *
   * @param sets A list of GroupingSets objects.
   * @since 1.1.0
   * @return A RelationalGroupedDataFrame that you can use to perform aggregations on each group of
   *     data.
   */
  public RelationalGroupedDataFrame groupByGroupingSets(GroupingSets... sets) {
    com.snowflake.snowpark.GroupingSets[] arr =
        new com.snowflake.snowpark.GroupingSets[sets.length];
    for (int i = 0; i < sets.length; i++) {
      arr[i] = sets[i].getScalaGroupingSets();
    }
    return new RelationalGroupedDataFrame(
        this.df.groupByGroupingSets(JavaUtils.groupingSetArrayToSeq(arr)));
  }

  /**
   * Rotates this DataFrame by turning the unique values from one column in the input expression
   * into multiple columns and aggregating results where required on any remaining column values.
   *
   * <p>Only one aggregate is supported with pivot.
   *
   * <p>For example:
   *
   * <pre>
   * DataFrame dfPivoted = df.pivot(df.col("col1"), new int[]{1, 2, 3})
   *   .agg(sum(df.col("col2")));
   * </pre>
   *
   * @since 1.2.0
   * @param pivotColumn The name of the column to use.
   * @param values An array of values in the column.
   * @return A RelationalGroupedDataFrame
   */
  public RelationalGroupedDataFrame pivot(Column pivotColumn, Object[] values) {
    return new RelationalGroupedDataFrame(
        this.df.pivot(pivotColumn.toScalaColumn(), JavaUtils.objectArrayToSeq(values)));
  }

  /**
   * Rotates this DataFrame by turning the unique values from one column in the input expression
   * into multiple columns and aggregating results where required on any remaining column values.
   *
   * <p>Only one aggregate is supported with pivot.
   *
   * <p>For example:
   *
   * <pre>
   * DataFrame dfPivoted = df.pivot("col1", new int[]{1, 2, 3})
   *   .agg(sum(df.col("col2")));
   * </pre>
   *
   * @since 1.2.0
   * @param pivotColumn The name of the column to use.
   * @param values An array of values in the column.
   * @return A RelationalGroupedDataFrame
   */
  public RelationalGroupedDataFrame pivot(String pivotColumn, Object[] values) {
    return new RelationalGroupedDataFrame(
        this.df.pivot(pivotColumn, JavaUtils.objectArrayToSeq(values)));
  }

  /**
   * Executes the query representing this DataFrame and returns the number of rows in the result
   * (similar to the COUNT function in SQL). This is an action function.
   *
   * @since 0.8.0
   * @return The number of rows.
   */
  public long count() {
    return df.count();
  }

  /**
   * Retrieves a reference to a column in this DataFrame.
   *
   * @param colName The name of the column
   * @return The target column
   * @since 0.9.0
   */
  public Column col(String colName) {
    return new Column(df.col(colName));
  }

  /**
   * Returns the current DataFrame aliased as the input alias name.
   *
   * <p>For example:
   *
   * <p>{{{ val df2 = df.alias("A") df2.select(df2.col("A.num")) }}}
   *
   * @since 1.10.0
   * @param alias The alias name of the dataframe
   * @return a [[DataFrame]]
   */
  public DataFrame alias(String alias) {
    return new DataFrame(this.df.alias(alias));
  }

  /**
   * Executes the query representing this DataFrame and returns the result as an array of Row
   * objects.
   *
   * @return The result array
   * @since 0.9.0
   */
  public Row[] collect() {
    com.snowflake.snowpark.Row[] rows = df.collect();
    Row[] result = new Row[rows.length];
    for (int i = 0; i < result.length; i++) {
      result[i] = new Row(rows[i]);
    }
    return result;
  }

  /**
   * Executes the query representing this DataFrame and returns an iterator of Row objects that you
   * can use to retrieve the results.
   *
   * <p>Unlike the {@code collect} method, this method does not load all data into memory at once.
   *
   * @since 0.12.0
   * @return An Iterator of Row
   */
  public Iterator<Row> toLocalIterator() {
    return toJavaIterator(df.toLocalIterator());
  }

  /**
   * Evaluates this DataFrame and prints out the first ten rows.
   *
   * @since 0.9.0
   */
  public void show() {
    df.show();
  }

  /**
   * Evaluates this DataFrame and prints out the first ten rows with configurable column truncation.
   *
   * @since 1.17.0
   * @param truncate Whether to truncate long column values. If true, column values longer than
   *                 50 characters will be truncated with "..." If false, full column values
   *                 will be displayed regardless of length.
   */
  public void show(boolean truncate) {
        df.show(truncate);
    }

  /**
   * Evaluates this DataFrame and prints out the first `''n''` rows.
   *
   * @since 0.12.0
   * @param n The number of rows to print out.
   */
  public void show(int n) {
    df.show(n);
  }

  /**
   * Evaluates this DataFrame and prints out the first n rows with configurable column truncation.
   *
   * @since 1.17.0
   * @param n The number of rows to print out.
   * @param truncate Whether to truncate long column values. If true, column values longer than
   *                 50 characters will be truncated with "..." If false, full column values
   *                 will be displayed regardless of length.
   */
  public void show(int n, boolean truncate) {
        df.show(n, truncate);
    }

  /**
   * Evaluates this DataFrame and prints out the first `''n''` rows with the specified maximum
   * number of characters per column.
   *
   * @since 0.12.0
   * @param n The number of rows to print out.
   * @param maxWidth The maximum number of characters to print out for each column. If the number of
   *     characters exceeds the maximum, the method prints out an ellipsis (...) at the end of the
   *     column.
   */
  public void show(int n, int maxWidth) {
    df.show(n, maxWidth);
  }

  /**
   * Creates a view that captures the computation expressed by this DataFrame.
   *
   * <p>For `viewName`, you can include the database and schema name (i.e. specify a fully-qualified
   * name). If no database name or schema name are specified, the view will be created in the
   * current database or schema.
   *
   * <p>`viewName` must be a valid <a
   * href="https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html">Snowflake
   * identifier</a>
   *
   * @since 0.12.0
   * @param viewName The name of the view to create or replace.
   */
  public void createOrReplaceView(String viewName) {
    df.createOrReplaceView(viewName);
  }

  /**
   * Creates a view that captures the computation expressed by this DataFrame.
   *
   * <p>In `multipartIdentifer`, you can include the database and schema name to specify a
   * fully-qualified name. If no database name or schema name are specified, the view will be
   * created in the current database or schema.
   *
   * <p>The view name must be a valid <a
   * href="https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html">Snowflake
   * identifier</a>
   *
   * @since 0.12.0
   * @param multipartIdentifier A sequence of strings that specifies the database name, schema name,
   *     and view name.
   */
  public void createOrReplaceView(String[] multipartIdentifier) {
    df.createOrReplaceView(JavaUtils.stringArrayToStringSeq(multipartIdentifier));
  }

  /**
   * Creates a temporary view that returns the same results as this DataFrame.
   *
   * <p>You can use the view in subsequent SQL queries and statements during the current session.
   * The temporary view is only available in the session in which it is created.
   *
   * <p>For `viewName`, you can include the database and schema name (i.e. specify a fully-qualified
   * name). If no database name or schema name are specified, the view will be created in the
   * current database or schema.
   *
   * <p>`viewName` must be a valid <a
   * href="https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html">Snowflake
   * identifier</a>
   *
   * @since 0.12.0
   * @param viewName The name of the view to create or replace.
   */
  public void createOrReplaceTempView(String viewName) {
    df.createOrReplaceTempView(viewName);
  }

  /**
   * Creates a temporary view that returns the same results as this DataFrame.
   *
   * <p>You can use the view in subsequent SQL queries and statements during the current session.
   * The temporary view is only available in the session in which it is created.
   *
   * <p>In `multipartIdentifer`, you can include the database and schema name to specify a
   * fully-qualified name. If no database name or schema name are specified, the view will be
   * created in the current database or schema.
   *
   * <p>The view name must be a valid <a
   * href="https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html">Snowflake
   * identifier</a>
   *
   * @since 0.12.0
   * @param multipartIdentifier A sequence of strings that specify the database name, schema name,
   *     and view name.
   */
  public void createOrReplaceTempView(String[] multipartIdentifier) {
    df.createOrReplaceTempView(JavaUtils.stringArrayToStringSeq(multipartIdentifier));
  }

  /**
   * Executes the query representing this DataFrame and returns the first row of results.
   *
   * @since 0.12.0
   * @return An Optional Row.
   */
  public Optional<Row> first() {
    scala.Option<com.snowflake.snowpark.Row> result = df.first();
    if (result.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(new Row(result.get()));
    }
  }

  /**
   * Executes the query representing this DataFrame and returns the first {@code n} rows of the
   * results.
   *
   * @since 0.12.0
   * @param n The number of rows to return.
   * @return An Array of the first {@code n} Row objects. If {@code n} is negative or larger than
   *     the number of rows in the results, returns all rows in the results.
   */
  public Row[] first(int n) {
    com.snowflake.snowpark.Row[] result = df.first(n);
    Row[] javaRows = new Row[n];
    for (int i = 0; i < result.length; i++) {
      javaRows[i] = new Row(result[i]);
    }
    return javaRows;
  }

  /**
   * Checks whether the {@code DataFrame} contains any rows.
   *
   * <p><b>Example:</b>
   *
   * <pre>{@code
   * DataFrame df = session.sql("SELECT * FROM VALUES (1), (2), (3) AS T(a)");
   * boolean isEmpty = df.isEmpty(); // returns false
   *
   * DataFrame emptyDf = session.sql("SELECT * FROM (SELECT 1) WHERE 1 = 0");
   * boolean isEmptyDf = emptyDf.isEmpty(); // returns true
   * }</pre>
   *
   * @return {@code true} if the {@code DataFrame} contains no rows; {@code false} otherwise.
   * @since 1.17.0
   */
  public Boolean isEmpty() {
    return this.df.isEmpty();
  }

  /**
   * Returns a new DataFrame with a sample of N rows from the underlying DataFrame.
   *
   * <p>NOTE:
   *
   * <p>- If the row count in the DataFrame is larger than the requested number of rows, the method
   * returns a DataFrame containing the number of requested rows. - If the row count in the
   * DataFrame is smaller than the requested number of rows, the method returns a DataFrame
   * containing all rows.
   *
   * @param num The number of rows to sample in the range of 0 to 1,000,000.
   * @since 0.12.0
   * @return A DataFrame containing the sample of {@code num} rows.
   */
  public DataFrame sample(long num) {
    return new DataFrame(df.sample(num));
  }

  /**
   * Returns a new DataFrame that contains a sampling of rows from the current DataFrame.
   *
   * <p>NOTE:
   *
   * <p>- The number of rows returned may be close to (but not exactly equal to) {@code
   * (probabilityFraction * totalRowCount)}. - The Snowflake <a
   * href="https://docs.snowflake.com/en/sql-reference/constructs/sample.html">SAMPLE</a> supports
   * specifying 'probability' as a percentage number. The range of 'probability' is {@code [0.0,
   * 100.0]}. The conversion formula is {@code probability = probabilityFraction * 100}.
   *
   * @param probabilityFraction The fraction of rows to sample. This must be in the range of `0.0`
   *     to `1.0`.
   * @since 0.12.0
   * @return A DataFrame containing the sample of rows.
   */
  public DataFrame sample(double probabilityFraction) {
    return new DataFrame(df.sample(probabilityFraction));
  }

  /**
   * Randomly splits the current DataFrame into separate DataFrames, using the specified weights.
   *
   * <p>NOTE:
   *
   * <p>- If only one weight is specified, the returned DataFrame array only includes the current
   * DataFrame. - If multiple weights are specified, the current DataFrame will be cached before
   * being split.
   *
   * @param weights Weights to use for splitting the DataFrame. If the weights don't add up to 1,
   *     the weights will be normalized.
   * @since 0.12.0
   * @return A list of DataFrame objects
   */
  public DataFrame[] randomSplit(double[] weights) {
    com.snowflake.snowpark.DataFrame[] result = df.randomSplit(weights);
    DataFrame[] javaDF = new DataFrame[result.length];
    for (int i = 0; i < javaDF.length; i++) {
      javaDF[i] = new DataFrame(result[i]);
    }
    return javaDF;
  }

  /**
   * Flattens (explodes) compound values into multiple rows (similar to the SQL <a
   * href="https://docs.snowflake.com/en/sql-reference/functions/flatten.html">FLATTEN</a>
   *
   * <p>The `flatten` method adds the following <a
   * href="https://docs.snowflake.com/en/sql-reference/functions/flatten.html#output">columns</a> to
   * the returned DataFrame:
   *
   * <p>- SEQ - KEY - PATH - INDEX - VALUE - THIS
   *
   * <p>If {@code this} DataFrame also has columns with the names above, you can disambiguate the
   * columns by using the {@code this("value")} syntax.
   *
   * <p>For example, if the current DataFrame has a column named `value`:
   *
   * <pre>{@code
   * DataFrame df = session.sql("select parse_json(value) as value from values('[1,2]') as T(value)");
   * DataFrame flattened = df.flatten(df.col("value"));
   * flattened.select(df.col("value"), flattened("value").as("newValue")).show();
   * }</pre>
   *
   * @param input The expression that will be unseated into rows. The expression must be of data
   *     type VARIANT, OBJECT, or ARRAY.
   * @return A DataFrame containing the flattened values.
   * @since 0.12.0
   */
  public DataFrame flatten(Column input) {
    return new DataFrame(df.flatten(input.toScalaColumn()));
  }

  /**
   * Flattens (explodes) compound values into multiple rows (similar to the SQL <a
   * href="https://docs.snowflake.com/en/sql-reference/functions/flatten.html">FLATTEN</a>
   *
   * <p>The `flatten` method adds the following <a
   * href="https://docs.snowflake.com/en/sql-reference/functions/flatten.html#output">columns</a> to
   * the returned DataFrame:
   *
   * <p>- SEQ - KEY - PATH - INDEX - VALUE - THIS
   *
   * <p>If {@code this} DataFrame also has columns with the names above, you can disambiguate the
   * columns by using the {@code this("value")} syntax.
   *
   * <p>For example, if the current DataFrame has a column named `value`:
   *
   * <pre>{@code
   * DataFrame df = session.sql("select parse_json(value) as value from values('[1,2]') as T(value)");
   * DataFrame flattened = df.flatten(df.col("value"), "", false, false, "both");
   * flattened.select(df.col("value"), flattened("value").as("newValue")).show();
   * }</pre>
   *
   * @param input The expression that will be unseated into rows. The expression must be of data
   *     type VARIANT, OBJECT, or ARRAY.
   * @param path The path to the element within a VARIANT data structure which needs to be
   *     flattened. Can be a zero-length string (i.e. empty path) if the outermost element is to be
   *     flattened.
   * @param outer If FALSE, any input rows that cannot be expanded, either because they cannot be
   *     accessed in the path or because they have zero fields or entries, are completely omitted
   *     from the output. Otherwise, exactly one row is generated for zero-row expansions (with NULL
   *     in the KEY, INDEX, and VALUE columns).
   * @param recursive If FALSE, only the element referenced by PATH is expanded. Otherwise, the
   *     expansion is performed for all sub-elements recursively.
   * @param mode Specifies whether only OBJECT, ARRAY, or BOTH should be flattened.
   * @return A DataFrame containing the flattened values.
   * @since 0.12.0
   */
  public DataFrame flatten(
      Column input, String path, boolean outer, boolean recursive, String mode) {
    return new DataFrame(df.flatten(input.toScalaColumn(), path, outer, recursive, mode));
  }

  /**
   * Returns a DataFrameWriter object that you can use to write the data in the DataFrame to any
   * supported destination. The default SaveMode for the returned DataFrameWriter is {@code
   * SaveMode.Append}.
   *
   * <p>Example:
   *
   * <pre>{@code
   * df.write().saveAsTable("table1");
   * }</pre>
   *
   * @since 1.1.0
   * @return A DataFrameWriter
   */
  public DataFrameWriter write() {
    return new DataFrameWriter(df.write(), getScalaDataFrame().session());
  }

  /**
   * Returns a {@code DataFrameNaFunctions} object that provides functions for handling missing
   * values in the DataFrame.
   *
   * @since 1.1.0
   * @return The DataFrameNaFunctions
   */
  public DataFrameNaFunctions na() {
    return new DataFrameNaFunctions(this.df.na());
  }

  /**
   * Returns a DataFrameStatFunctions object that provides statistic functions.
   *
   * @since 1.1.0
   * @return The DataFrameStatFunctions
   */
  public DataFrameStatFunctions stat() {
    return new DataFrameStatFunctions(this.df.stat());
  }

  /**
   * Returns a DataFrameAsyncActor object that can be used to execute DataFrame actions
   * asynchronously.
   *
   * @since 1.2.0
   * @return A DataFrameAsyncActor object
   */
  public DataFrameAsyncActor async() {
    return new DataFrameAsyncActor(this);
  }

  /**
   * Joins the current DataFrame with the output of the specified table function `func`.
   *
   * <p>To pass arguments to the table function, use the `args` arguments of this method. In the
   * table function arguments, you can include references to columns in this DataFrame.
   *
   * <p>For example:
   *
   * <pre>{@code
   * // The following example uses the split_to_table function to split
   * // column 'a' in this DataFrame on the character ','.
   * // Each row in the current DataFrame will produce N rows in the resulting DataFrame,
   * // where N is the number of tokens in the column 'a'.
   *
   * df.join(TableFunctions.split_to_table(), df.col("a"), Functions.lit(","))
   * }</pre>
   *
   * @since 1.2.0
   * @param func TableFunction object, which can be one of the values in the TableFunctions class or
   *     an object that you create from the TableFunction class.
   * @param args The functions arguments
   * @return The result DataFrame
   */
  public DataFrame join(TableFunction func, Column... args) {
    return new DataFrame(
        this.df.join(
            func.getScalaTableFunction(),
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(args))));
  }

  /**
   * Joins the current DataFrame with the output of the specified user-defined table function (UDTF)
   * `func`.
   *
   * <p>To pass arguments to the table function, use the `args` argument of this method. In the
   * table function arguments, you can include references to columns in this DataFrame.
   *
   * <p>To specify a PARTITION BY or ORDER BY clause, use the `partitionBy` and `orderBy` arguments.
   *
   * <p>For example
   *
   * <pre>{@code
   * // The following example passes the values in the column `col1` to the
   * // user-defined tabular function (UDTF) `udtf`, partitioning the
   * // data by `col2` and sorting the data by `col1`. The example returns
   * // a new DataFrame that joins the contents of the current DataFrame with
   * // the output of the UDTF.
   * df.join(new TableFunction("udtf"),
   *     new Column[] {df.col("col1")},
   *     new Column[] {df.col("col2")},
   *     new Column[] {df.col("col1")});
   * }</pre>
   *
   * @since 1.7.0
   * @param func An object that represents a user-defined table function (UDTF).
   * @param args An array of arguments to pass to the specified table function.
   * @param partitionBy An array of columns partitioned by.
   * @param orderBy An array of columns ordered by.
   * @return The result DataFrame
   */
  public DataFrame join(TableFunction func, Column[] args, Column[] partitionBy, Column[] orderBy) {
    return new DataFrame(
        this.df.join(
            func.getScalaTableFunction(),
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(args)),
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(partitionBy)),
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(orderBy))));
  }

  /**
   * Joins the current DataFrame with the output of the specified table function `func` that takes
   * named parameters (e.g. `flatten`).
   *
   * <p>To pass arguments to the table function, use the `args` argument of this method. Pass in a
   * `Map` of parameter names and values. In these values, you can include references to columns in
   * this DataFrame.
   *
   * <p>For example:
   *
   * <pre>{@code
   * Map<String, Column> args = new HashMap<>();
   * args.put("input", Functions.parse_json(df.col("a")));
   * df.join(new TableFunction("flatten"), args);
   * }</pre>
   *
   * @since 1.2.0
   * @param func TableFunction object, which can be one of the values in the TableFunctions class or
   *     an object that you create from the TableFunction class.
   * @param args Map of arguments to pass to the specified table function. Some functions, like
   *     `flatten`, have named parameters. Use this map to specify the parameter names and their
   *     corresponding values.
   * @return The result DataFrame
   */
  public DataFrame join(TableFunction func, Map<String, Column> args) {
    Map<String, com.snowflake.snowpark.Column> scalaArgs = new HashMap<>();
    for (Map.Entry<String, Column> entry : args.entrySet()) {
      scalaArgs.put(entry.getKey(), entry.getValue().toScalaColumn());
    }
    return new DataFrame(
        this.df.join(
            func.getScalaTableFunction(), JavaUtils.javaStringColumnMapToScala(scalaArgs)));
  }

  /**
   * Joins the current DataFrame with the output of the specified user-defined table function (UDTF)
   * `func`.
   *
   * <p>To pass arguments to the table function, use the `args` argument of this method. Pass in a
   * `Map` of parameter names and values. In these values, you can include references to columns in
   * this DataFrame.
   *
   * <p>To specify a PARTITION BY or ORDER BY clause, use the `partitionBy` and `orderBy` arguments.
   *
   * <p>For example:
   *
   * <pre>{@code
   * // The following example passes the values in the column `col1` to the
   * // user-defined tabular function (UDTF) `udtf`, partitioning the
   * // data by `col2` and sorting the data by `col1`. The example returns
   * // a new DataFrame that joins the contents of the current DataFrame with
   * // the output of the UDTF.
   * Map<String, Column> args = new HashMap<>();
   * args.put("arg1", df.col("col1"));
   * df.join(
   *   args,
   *   new Column[] {df.col("col2")},
   *   new Column[] {df.col("col1")}
   * )
   * }</pre>
   *
   * @since 1.7.0
   * @param func An object that represents a user-defined table function (UDTF).
   * @param args Map of arguments to pass to the specified table function. Some functions, like
   *     `flatten`, have named parameters. Use this map to specify the parameter names and their
   *     corresponding values.
   * @param partitionBy An array of columns partitioned by.
   * @param orderBy An array of columns ordered by.
   * @return The result DataFrame
   */
  public DataFrame join(
      TableFunction func, Map<String, Column> args, Column[] partitionBy, Column[] orderBy) {
    Map<String, com.snowflake.snowpark.Column> scalaArgs = new HashMap<>();
    for (Map.Entry<String, Column> entry : args.entrySet()) {
      scalaArgs.put(entry.getKey(), entry.getValue().toScalaColumn());
    }
    return new DataFrame(
        this.df.join(
            func.getScalaTableFunction(),
            JavaUtils.javaStringColumnMapToScala(scalaArgs),
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(partitionBy)),
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(orderBy))));
  }

  /**
   * Joins the current DataFrame with the output of the specified table function `func`.
   *
   * <p>Pre-defined table functions can be found in `TableFunctions` class.
   *
   * <p>For example:
   *
   * <pre>{@code
   * df.join(TableFunctions.flatten(
   *   Functions.parse_json(df.col("col")),
   *   "path", true, true, "both"
   * ));
   * }</pre>
   *
   * <p>Or load any Snowflake builtin table function via TableFunction Class.
   *
   * <pre>{@code
   * Map<String, Column> args = new HashMap<>();
   * args.put("input", Functions.parse_json(df.col("a")));
   * df.join(new TableFunction("flatten").call(args));
   * }</pre>
   *
   * @since 1.10.0
   * @param func Column object, which can be one of the values in the TableFunctions class or an
   *     object that you create from the `new TableFunction("name").call()`.
   * @return The result DataFrame
   */
  public DataFrame join(Column func) {
    return new DataFrame(this.df.join(func.toScalaColumn()));
  }

  /**
   * Joins the current DataFrame with the output of the specified table function `func`.
   *
   * <p>To specify a PARTITION BY or ORDER BY clause, use the `partitionBy` and `orderBy` arguments.
   *
   * <p>Pre-defined table functions can be found in `TableFunctions` class.
   *
   * <p>For example:
   *
   * <pre>{@code
   * df.join(TableFunctions.flatten(
   *     Functions.parse_json(df.col("col1")),
   *     "path", true, true, "both"
   *   ),
   *   new Column[] {df.col("col2")},
   *   new Column[] {df.col("col1")}
   * );
   * }</pre>
   *
   * <p>Or load any Snowflake builtin table function via TableFunction Class.
   *
   * <pre>{@code
   * Map<String, Column> args = new HashMap<>();
   * args.put("input", Functions.parse_json(df.col("col1")));
   * df.join(new TableFunction("flatten").call(args),
   * new Column[] {df.col("col2")},
   * new Column[] {df.col("col1")});
   * }</pre>
   *
   * @since 1.10.0
   * @param func Column object, which can be one of the values in the TableFunctions class or an
   *     object that you create from the `new TableFunction("name").call()`.
   * @param partitionBy An array of columns partitioned by.
   * @param orderBy An array of columns ordered by.
   * @return The result DataFrame
   */
  public DataFrame join(Column func, Column[] partitionBy, Column[] orderBy) {
    return new DataFrame(
        this.df.join(
            func.toScalaColumn(),
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(partitionBy)),
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(orderBy))));
  }

  com.snowflake.snowpark.DataFrame getScalaDataFrame() {
    return this.df;
  }

  static Iterator<Row> toJavaIterator(scala.collection.Iterator<com.snowflake.snowpark.Row> input) {
    return new Iterator<Row>() {
      @Override
      public boolean hasNext() {
        return input.hasNext();
      }

      @Override
      public Row next() {
        if (!hasNext()) {
          // user error, Java standard
          throw new NoSuchElementException();
        }
        return new Row(input.next());
      }
    };
  }
}
