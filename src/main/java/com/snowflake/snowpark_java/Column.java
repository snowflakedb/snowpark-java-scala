package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaDataTypeUtils;
import com.snowflake.snowpark.internal.JavaUtils;
import com.snowflake.snowpark_java.types.DataType;
import java.util.Optional;
import scala.Option;

/**
 * Represents a column or an expression in a DataFrame.
 *
 * @since 0.9.0
 */
public class Column {
  private final com.snowflake.snowpark.Column scalaColumn;

  Column(com.snowflake.snowpark.Column scalaColumn) {
    this.scalaColumn = scalaColumn;
  }

  /**
   * Retrieves the specified element (field) in a column that contains <a
   * href="https://docs.snowflake.com/en/user-guide/semistructured-concepts.html">semi-structured
   * data</a>.
   *
   * <p>The method applies case-sensitive matching to the names of the specified elements.
   *
   * <p>This is equivalent to using <a
   * href="https://docs.snowflake.com/en/user-guide/querying-semistructured.html#bracket-notation">
   * bracket notation in SQL</a> (`column['element']`).
   *
   * <p>If the column is an OBJECT value, this function extracts the VARIANT value of the element
   * with the specified name from the OBJECT value. If the element is not found, the method returns
   * NULL. You must not specify an empty string for the element name.
   *
   * <p>If the column is a VARIANT value, this function first checks if the VARIANT value contains
   * an OBJECT value. If the VARIANT value does not contain an OBJECT value, the method returns
   * NULL. Otherwise, the method works as described above.
   *
   * <p>For example:
   *
   * <pre>{@code
   * df.select(df.col("src").subField("salesperson").subField("emails").subField(0))
   * }</pre>
   *
   * @param field The field name of the subfield to be extracted. You cannot specify a path.
   * @return The sub-field of this column
   * @since 0.9.0
   */
  public Column subField(String field) {
    return new Column(scalaColumn.apply(field));
  }

  /**
   * Retrieves the element (field) at the specified index in a column that contains <a
   * href="https://docs.snowflake.com/en/user-guide/semistructured-concepts.html">semi-structured
   * data</a>.
   *
   * <p>The method applies case-sensitive matching to the names of the specified elements.
   *
   * <p>This is equivalent to using <a
   * href="https://docs.snowflake.com/en/user-guide/querying-semistructured.html#bracket-notation">
   * bracket notation in SQL</a> (`column['element']`).
   *
   * <p>If the column is an ARRAY value, this function extracts the VARIANT value of the ARRAY
   * element at the specified index. If the index points outside of the array boundaries or if an
   * element does not exists at the specified index (e.g. if the array is sparsely populated), the
   * method returns NULL.
   *
   * <p>If the column is a VARIANT value, this functions first checks if the VARIANT value contains
   * an ARRAY value. If the VARIANT value does not contain an ARRAY value, the method returns NULL.
   * Otherwise, the method works as described above.
   *
   * <pre>{@code
   * df.select(df.col("src").subField(1).subField(0))
   * }</pre>
   *
   * @param index The index of the subfield to be extracted
   * @return The sub-field of this column
   * @since 0.9.0
   */
  public Column subField(int index) {
    return new Column(scalaColumn.apply(index));
  }

  /**
   * Retrieves the column name (if the column has a name).
   *
   * @return An Optional String
   * @since 0.9.0
   */
  public Optional<String> getName() {
    Option<String> name = this.scalaColumn.getName();
    if (name.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(name.get());
  }

  /**
   * Retrieves s new renamed Column.
   *
   * @param alias The new column name
   * @return A new Column object
   * @since 0.9.0
   */
  public Column as(String alias) {
    return new Column(this.scalaColumn.as(alias));
  }

  /**
   * Retrieves s new renamed Column.
   *
   * @param alias The new column name
   * @return A new Column object
   * @since 0.9.0
   */
  public Column alias(String alias) {
    return new Column(this.scalaColumn.alias(alias));
  }

  /**
   * Unary minus.
   *
   * @return The result column object
   * @since 0.9.0
   */
  public Column unary_minus() {
    return new Column(this.scalaColumn.unary_$minus());
  }

  /**
   * Unary not.
   *
   * @return The result column object
   * @since 0.9.0
   */
  public Column unary_not() {
    return new Column(this.scalaColumn.unary_$bang());
  }

  /**
   * Equal to.
   *
   * @param other The column being compared
   * @return The result column object
   * @since 0.9.0
   */
  public Column equal_to(Column other) {
    return new Column(this.scalaColumn.equal_to(other.scalaColumn));
  }

  /**
   * Not equal to.
   *
   * @param other The column being compared
   * @return The result column object
   * @since 0.9.0
   */
  public Column not_equal(Column other) {
    return new Column(this.scalaColumn.not_equal(other.scalaColumn));
  }

  /**
   * Greater than.
   *
   * @param other The column being compared
   * @return The result column object
   * @since 0.9.0
   */
  public Column gt(Column other) {
    return new Column(this.scalaColumn.gt(other.scalaColumn));
  }

  /**
   * Less than.
   *
   * @param other The column being compared
   * @return The result column object
   * @since 0.9.0
   */
  public Column lt(Column other) {
    return new Column(this.scalaColumn.lt(other.scalaColumn));
  }

  /**
   * Less than or equal to.
   *
   * @param other The column being compared
   * @return The result column object
   * @since 0.9.0
   */
  public Column leq(Column other) {
    return new Column(this.scalaColumn.leq(other.scalaColumn));
  }

  /**
   * Greater than or equal to.
   *
   * @param other The column being compared
   * @return The result column object
   * @since 0.9.0
   */
  public Column geq(Column other) {
    return new Column(this.scalaColumn.geq(other.scalaColumn));
  }

  /**
   * Equal to. This function can be used to compare against a null value.
   *
   * @param other The column being compared
   * @return The result column object
   * @since 0.9.0
   */
  public Column equal_null(Column other) {
    return new Column(this.scalaColumn.equal_null(other.scalaColumn));
  }

  /**
   * Is NaN.
   *
   * @return The result column object
   * @since 0.9.0
   */
  public Column equal_nan() {
    return new Column(this.scalaColumn.equal_nan());
  }

  /**
   * Is null.
   *
   * @return The result column object
   * @since 0.9.0
   */
  public Column is_null() {
    return new Column(this.scalaColumn.is_null());
  }

  /**
   * Is not null.
   *
   * @return The result column object
   * @since 0.9.0
   */
  public Column is_not_null() {
    return new Column(this.scalaColumn.is_not_null());
  }

  /**
   * Or.
   *
   * @param other The other column
   * @return The result column object
   * @since 0.9.0
   */
  public Column or(Column other) {
    return new Column(this.scalaColumn.or(other.scalaColumn));
  }

  /**
   * And.
   *
   * @param other The other column
   * @return The result column object
   * @since 0.9.0
   */
  public Column and(Column other) {
    return new Column(this.scalaColumn.and(other.scalaColumn));
  }

  /**
   * Between lower bound (including) and upper bound (including).
   *
   * @param lowerBound the lower bound
   * @param upperBound the upper bound
   * @return The result column object
   * @since 0.9.0
   */
  public Column between(Column lowerBound, Column upperBound) {
    return new Column(this.scalaColumn.between(lowerBound.scalaColumn, upperBound.scalaColumn));
  }

  /**
   * Plus
   *
   * @param other The column being added
   * @return The result column object
   * @since 0.9.0
   */
  public Column plus(Column other) {
    return new Column(this.scalaColumn.plus(other.scalaColumn));
  }

  /**
   * Minus
   *
   * @param other The column being subtracted
   * @return The result column object
   * @since 0.9.0
   */
  public Column minus(Column other) {
    return new Column(this.scalaColumn.minus(other.scalaColumn));
  }

  /**
   * Multiply
   *
   * @param other The column being multiplied
   * @return The result column object
   * @since 0.9.0
   */
  public Column multiply(Column other) {
    return new Column(this.scalaColumn.multiply(other.scalaColumn));
  }

  /**
   * Divide
   *
   * @param other The column being divided
   * @return The result column object
   * @since 0.9.0
   */
  public Column divide(Column other) {
    return new Column(this.scalaColumn.divide(other.scalaColumn));
  }

  /**
   * Remainder
   *
   * @param other The column being calculated
   * @return The result column object
   * @since 0.9.0
   */
  public Column mod(Column other) {
    return new Column(this.scalaColumn.mod(other.scalaColumn));
  }

  /**
   * Casts the values in the Column to the specified data type.
   *
   * @param to The target data type
   * @return The result column object
   * @since 0.9.0
   */
  public Column cast(DataType to) {
    return new Column(this.scalaColumn.cast(JavaDataTypeUtils.javaTypeToScalaType(to)));
  }

  /**
   * Sorts this column in descending order.
   *
   * @return The result column object
   * @since 0.9.0
   */
  public Column desc() {
    return new Column(this.scalaColumn.desc());
  }

  /**
   * Sorts this column in descending order, null values sorted before non-null values.
   *
   * @return The result column object
   * @since 0.9.0
   */
  public Column desc_nulls_first() {
    return new Column(this.scalaColumn.desc_nulls_first());
  }

  /**
   * Sorts this column in descending order, null values sorted after non-null values.
   *
   * @return The result column object
   * @since 0.9.0
   */
  public Column desc_nulls_last() {
    return new Column(this.scalaColumn.desc_nulls_last());
  }

  /**
   * Sorts this column in ascending order.
   *
   * @return The result column object
   * @since 0.9.0
   */
  public Column asc() {
    return new Column(this.scalaColumn.asc());
  }

  /**
   * Sorts this column in ascending order, null values sorted before non-null values.
   *
   * @return The result column object
   * @since 0.9.0
   */
  public Column asc_nulls_first() {
    return new Column(this.scalaColumn.asc_nulls_first());
  }

  /**
   * Sorts this column in ascending order, null values sorted after non-null values.
   *
   * @return The result column object
   * @since 0.9.0
   */
  public Column asc_nulls_last() {
    return new Column(this.scalaColumn.asc_nulls_last());
  }

  /**
   * Bitwise or.
   *
   * @param other The column being calculated
   * @return The result column object
   * @since 0.9.0
   */
  public Column bitor(Column other) {
    return new Column(this.scalaColumn.bitor(other.scalaColumn));
  }

  /**
   * Bitwise and.
   *
   * @param other The column being calculated
   * @return The result column object
   * @since 0.9.0
   */
  public Column bitand(Column other) {
    return new Column(this.scalaColumn.bitand(other.scalaColumn));
  }

  /**
   * Bitwise xor.
   *
   * @param other The column being calculated
   * @return The result column object
   * @since 0.9.0
   */
  public Column bitxor(Column other) {
    return new Column(this.scalaColumn.bitxor(other.scalaColumn));
  }

  /**
   * Allows case-sensitive matching of strings based on comparison with a pattern.
   *
   * <p>For details, see the Snowflake documentation on <a
   * href="https://docs.snowflake.com/en/sql-reference/functions-regexp.html#label-regexp-general-usage-notes">regular
   * expressions</a>
   *
   * @param pattern A regular expression
   * @return The result column object
   * @since 0.9.0
   */
  public Column like(Column pattern) {
    return new Column(this.scalaColumn.like(pattern.scalaColumn));
  }

  /**
   * Returns true if this column matches the specified regular expression.
   *
   * <p>For details, see the Snowflake documentation on <a
   * href="https://docs.snowflake.com/en/sql-reference/functions-regexp.html#label-regexp-general-usage-notes">regular
   * expressions</a>
   *
   * @param pattern A regular expression
   * @return The result column object
   * @since 0.9.0
   */
  public Column regexp(Column pattern) {
    return new Column(this.scalaColumn.regexp(pattern.scalaColumn));
  }

  /**
   * Returns a copy of the original Column with the specified 'collateSpec` property, rather than
   * the original collation specification property.
   *
   * <p>For details, see the Snowflake documentation on <a
   * href="https://docs.snowflake.com/en/sql-reference/collation.html#label-collation-specification">collation
   * specifications</a>
   *
   * @since 0.9.0
   * @param collateSpec The collation specification
   * @return The result column object
   */
  public Column collate(String collateSpec) {
    return new Column(this.scalaColumn.collate(collateSpec));
  }

  /**
   * Returns a windows frame, based on the specified WindowSpec.
   *
   * @see com.snowflake.snowpark_java.WindowSpec WindowSpec
   * @since 0.9.0
   * @return The result column object
   */
  public Column over() {
    return new Column(this.scalaColumn.over());
  }

  /**
   * Returns a windows frame, based on the specified WindowSpec.
   *
   * @since 0.1.0
   * @param windowSpec The window frame specification
   * @return The result column object
   */
  public Column over(WindowSpec windowSpec) {
    return new Column(this.scalaColumn.over(windowSpec.toScalaWindowSpec()));
  }

  /**
   * Returns a Column expression that adds a WITHIN GROUP clause to sort the rows by the specified
   * sequence of columns.
   *
   * <p>This method is supported on Column expressions returned by some aggregate functions,
   * including {@code functions.array_agg}, LISTAGG(), PERCENTILE_CONT(), and PERCENTILE_DISC().
   *
   * <p>For example:
   *
   * <pre>{@code
   * df.groupBy(df.col("col1")).agg(Functions.listagg(df.col("col2"), ",")
   *      .withinGroup(df.col("col2").asc()))
   * }</pre>
   *
   * @since 1.1.0
   * @param cols A list of Columns
   * @return The result Column
   */
  public Column withinGroup(Column... cols) {
    return new Column(
        this.scalaColumn.withinGroup(JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(cols))));
  }

  /**
   * Returns a conditional expression that you can pass to the filter or where method to perform the
   * equivalent of a WHERE ... IN query with a specified list of values.
   *
   * <p>The expression evaluates to true if the value in the column is one of the values in a
   * specified sequence.
   *
   * <p>For example, the following code returns a DataFrame that contains the rows where the column
   * "a" contains the value 1, 2, or 3. This is equivalent to SELECT * FROM table WHERE a IN (1, 2,
   * 3).
   *
   * <pre>{@code
   * df.filter(df.col("a").in(1, 2, 3))
   * }</pre>
   *
   * @param values The value list
   * @return The result Column
   * @since 1.2.0
   */
  public Column in(Object... values) {
    return new Column(this.scalaColumn.in(JavaUtils.objectArrayToSeq(values)));
  }

  /**
   * Returns a conditional expression that you can pass to the filter or where method to perform a
   * WHERE ... IN query with a specified subquery.
   *
   * <p>The expression evaluates to true if the value in the column is one of the values in the
   * column of the same name in a specified DataFrame.
   *
   * <p>For example, the following code returns a DataFrame that contains the rows where the column
   * "a" of `df2` contains one of the values from column "a" in `df1`. This is equivalent to SELECT
   * * FROM table2 WHERE a IN (SELECT a FROM table1).
   *
   * <pre>{@code
   * DataFrame df1 = session.table(table1);
   * DataFrame df2 = session.table(table2);
   * df2.filter(df1.col("a").in(df1));
   * }</pre>
   *
   * @param df A DataFrame contains target values
   * @return The return column.
   * @since 1.2.0
   */
  public Column in(DataFrame df) {
    return new Column(this.scalaColumn.in(df.getScalaDataFrame()));
  }

  /**
   * Retrieves a string representation of the expression corresponding to this Column instance.
   *
   * @return A String representing this expression
   * @since 0.9.0
   */
  @Override
  public String toString() {
    return this.scalaColumn.toString();
  }

  com.snowflake.snowpark.Column toScalaColumn() {
    return this.scalaColumn;
  }

  static com.snowflake.snowpark.Column[] toScalaColumnArray(Column[] arr) {
    com.snowflake.snowpark.Column[] result = new com.snowflake.snowpark.Column[arr.length];
    for (int i = 0; i < arr.length; i++) {
      result[i] = arr[i].scalaColumn;
    }
    return result;
  }
}
