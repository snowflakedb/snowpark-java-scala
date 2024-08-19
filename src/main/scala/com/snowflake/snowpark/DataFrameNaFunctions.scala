package com.snowflake.snowpark

import com.snowflake.snowpark.internal.{ErrorMessage, Logging}
import com.snowflake.snowpark.types._
import com.snowflake.snowpark.functions.{lit, when}
import com.snowflake.snowpark.internal.analyzer.quoteName

/** Provides functions for handling missing values in a DataFrame.
  *
  * @since 0.2.0
  */
final class DataFrameNaFunctions private[snowpark] (df: DataFrame) extends Logging {

  /** Returns a new DataFrame that excludes all rows containing fewer than {@code minNonNullsPerRow}
    * non-null and non-NaN values in the specified columns {@code cols} .
    *
    *   - If {@code minNonNullsPerRow} is greater than the number of the specified columns, the
    *     method returns an empty DataFrame.
    *   - If {@code minNonNullsPerRow} is less than 1, the method returns the original DataFrame.
    *   - If {@code cols} is empty, the method returns the original DataFrame.
    *
    * @param minNonNullsPerRow
    *   The minimum number of non-null and non-NaN values that should be in the specified columns in
    *   order for the row to be included.
    * @param cols
    *   A sequence of the names of columns to check for null and NaN values.
    * @return
    *   A [[DataFrame]]
    * @throws SnowparkClientException
    *   if cols contains any unrecognized column name
    * @since 0.2.0
    */
  def drop(minNonNullsPerRow: Int, cols: Seq[String]): DataFrame = transformation("drop") {
    // translate to
    // select * from table where
    // iff(floatCol = 'NaN' or floatCol is null, 0, 1)
    // + iff(nonFloatCol is null, 0, 1) >= minNonNullsPerRow

    // name in schema is not always quoted
    val schemaNameToIsFloat = df.output
      .map(field =>
        internal.analyzer
          .quoteName(field.name) -> (field.dataType == FloatType || field.dataType == DoubleType)
      )
      .toMap

    // split cols into two groups, float or non float.
    // for float values, we also need to verify if it is NaN.
    val (floatCols, nonFloatCols) = cols
      .map(col => {
        val normalized = internal.analyzer.quoteName(col)
        val isFloat = schemaNameToIsFloat.get(normalized) match {
          case Some(f) => f
          case None =>
            throw ErrorMessage
              .DF_CANNOT_RESOLVE_COLUMN_NAME(col, schemaNameToIsFloat.keySet)
        }
        (normalized, isFloat)
      })
      .partition(_._2)

    if (cols.isEmpty || minNonNullsPerRow < 1) {
      df
    } else if (minNonNullsPerRow > df.schema.size) {
      df.limit(0)
    } else {
      // iff(floatCol = 'NaN' or floatCol is null, 0, 1)
      val floatColsCounter = floatCols
        .map(x => df.col(x._1))
        .map(col => functions.callBuiltin("iff", col === "NaN" or col.is_null, 0, 1))

      // iff(nonFloatCol is null, 0, 1)
      val nonFloatColsCounter = nonFloatCols
        .map(x => df.col(x._1))
        .map(col => functions.callBuiltin("iff", col.is_null, 0, 1))

      // add all count result together
      val counter = (floatColsCounter ++ nonFloatColsCounter).reduce(_ + _)

      df.where(counter >= minNonNullsPerRow)
    }
  }

  /** Returns a new DataFrame that replaces all null and NaN values in the specified columns with
    * the values provided.
    *
    * {@code valueMap} describes which columns will be replaced and what the replacement values are.
    *
    *   - It only supports Long, Int, short, byte, String, Boolean, float, and Double values.
    *   - If the type of the given value doesn't match the column type (e.g. a Long value for a
    *     StringType column), the replacement in this column will be skipped.
    *
    * @param valueMap
    *   A Map that associates the names of columns with the values that should be used to replace
    *   null and NaN values in those columns.
    * @return
    *   A [[DataFrame]]
    * @throws SnowparkClientException
    *   if valueMap contains unrecognized columns
    *
    * @since 0.2.0
    */
  def fill(valueMap: Map[String, Any]): DataFrame = transformation("fill") {
    // translate to
    // select col, iff(floatCol is null or floatCol == 'NaN', replacement, floatCol),
    // iff(nonFloatCol is null, replacement, nonFloatCol) from table

    // don't use map because we want to keep the column name order
    val columnToDataType: Seq[(String, Any)] =
      df.output.map(field => (internal.analyzer.quoteName(field.name), field.dataType))
    val columnNameSet = columnToDataType.map(_._1).toSet
    val normalizedMap = valueMap.map { case (str, value) =>
      val normalized = internal.analyzer.quoteName(str)
      if (!columnNameSet.contains(normalized)) {
        throw ErrorMessage.DF_CANNOT_RESOLVE_COLUMN_NAME(str, columnNameSet)
      }
      normalized -> value
    }

    val columns: Seq[Column] = columnToDataType.map { case (colName, dataType) =>
      val column = df.col(colName)
      if (normalizedMap.contains(colName)) {
        (dataType, normalizedMap(colName)) match {
          case (LongType, number)
              if number.isInstanceOf[Long] || number.isInstanceOf[Int] || number
                .isInstanceOf[Short] || number.isInstanceOf[Byte] =>
            functions.callBuiltin("iff", column.is_null, number, column).as(colName)
          case (StringType, str: String) =>
            functions.callBuiltin("iff", column.is_null, str, column).as(colName)
          case (BooleanType, bool: Boolean) =>
            functions.callBuiltin("iff", column.is_null, bool, column).as(colName)
          case (DoubleType, number) if number.isInstanceOf[Double] || number.isInstanceOf[Float] =>
            functions
              .callBuiltin("iff", column.is_null or column === "NaN", number, column)
              .as(colName)
          case _ =>
            logWarning(
              s"Input value type of fill function doesn't match the target column data type, " +
                s"this replacement was skipped. Column Name: $colName " +
                s"Type: $dataType " +
                s"Input Value: ${normalizedMap(colName)} " +
                s"Type: ${normalizedMap(colName).getClass.getName}"
            )
            column
        }
      } else {
        column
      }
    }

    df.select(columns)
  }

  /** Returns a new DataFrame that replaces values in a specified column.
    *
    * Use the {@code replacement} parameter to specify a Map that associates the values to replace
    * with new values. To replace a null value, use None as the key in the Map.
    *
    * For example, suppose that you pass `col1` for {@code colName} and
    * {@code Map(2 -> 3, None -> 2, 4 -> null)} for {@code replacement} . In `col1`, this function
    * replaces:
    *
    *   - `2` with `3`
    *   - null with `2`
    *   - `4` with null
    *
    * @param colName
    *   The name of the column in which the values should be replaced.
    * @param replacement
    *   A Map that associates the original values with the replacement values.
    * @throws SnowparkClientException
    *   if colName is an unrecognized column name
    * @since 0.2.0
    */
  def replace(colName: String, replacement: Map[Any, Any]): DataFrame =
    transformation("replace") {
      // verify name
      val column = df.col(colName)

      if (replacement.isEmpty) {
        df
      } else {
        val columns = df.output.map { field =>
          if (quoteName(field.name) == quoteName(colName)) {
            val conditionReplacement = replacement.toSeq.map { case (original, replace) =>
              val cond = if (original == None || original == null) {
                column.is_null
              } else {
                column === lit(original)
              }
              val replacement = if (replace == None) {
                lit(null)
              } else {
                lit(replace)
              }
              (cond, replacement)
            }
            var caseWhen = when(conditionReplacement.head._1, conditionReplacement.head._2)
            conditionReplacement.tail.foreach { case (cond, replace) =>
              caseWhen = caseWhen.when(cond, replace)
            }
            caseWhen.otherwise(column).cast(field.dataType).as(colName)
          } else {
            df.col(field.name)
          }
        }
        df.select(columns)
      }
    }

  @inline protected def transformation(funcName: String)(func: => DataFrame): DataFrame =
    DataFrame.buildMethodChain(this.df.methodChain :+ "na", funcName)(func)
}
