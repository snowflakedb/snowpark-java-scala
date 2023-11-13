package com.snowflake.snowpark

import com.snowflake.snowpark.internal.ErrorMessage
import com.snowflake.snowpark.internal.ParameterUtils.{
  MAX_REQUEST_TIMEOUT_IN_SECONDS,
  MIN_REQUEST_TIMEOUT_IN_SECONDS,
  SnowparkRequestTimeoutInSeconds
}
import org.scalatest.FunSuite

class ErrorMessageSuite extends FunSuite {

  test("INTERNAL_TEST_MESSAGE") {
    val ex = ErrorMessage.INTERNAL_TEST_MESSAGE("my message: '%d $'")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0010")))
    assert(
      ex.message.startsWith("Error Code: 0010, Error message: " +
        "internal test message: my message: '%d $'"))
  }

  test("DF_CANNOT_DROP_COLUMN_NAME") {
    val ex = ErrorMessage.DF_CANNOT_DROP_COLUMN_NAME("col")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0100")))
    assert(
      ex.message.startsWith(
        "Error Code: 0100, Error message: " +
          "Unable to drop the column col. You must specify " +
          "the column by name (e.g. df.drop(col(\"a\")))."))
  }

  test("DF_SORT_NEED_AT_LEAST_ONE_EXPR") {
    val ex = ErrorMessage.DF_SORT_NEED_AT_LEAST_ONE_EXPR()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0101")))
    assert(
      ex.message.startsWith("Error Code: 0101, Error message: " +
        "For sort(), you must specify at least one sort expression."))
  }

  test("DF_CANNOT_DROP_ALL_COLUMNS") {
    val ex = ErrorMessage.DF_CANNOT_DROP_ALL_COLUMNS()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0102")))
    assert(
      ex.message.startsWith("Error Code: 0102, Error message: " +
        s"Cannot drop all columns"))
  }

  test("DF_CANNOT_RESOLVE_COLUMN_NAME_AMONG") {
    val ex = ErrorMessage.DF_CANNOT_RESOLVE_COLUMN_NAME_AMONG("c1", "a, b, c")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0103")))
    assert(
      ex.message.startsWith(
        "Error Code: 0103, Error message: " +
          "Cannot combine the DataFrames by column names. " +
          """The column "c1" is not a column in the other DataFrame (a, b, c)."""))
  }

  test("DF_SELF_JOIN_NOT_SUPPORTED") {
    val ex = ErrorMessage.DF_SELF_JOIN_NOT_SUPPORTED()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0104")))
    assert(
      ex.message.startsWith(
        "Error Code: 0104, Error message: " +
          "You cannot join a DataFrame with itself because the column references cannot " +
          "be resolved correctly. Instead, call clone() to create a copy of the DataFrame," +
          " and join the DataFrame with this copy."))
  }

  test("DF_RANDOM_SPLIT_WEIGHT_INVALID") {
    val ex = ErrorMessage.DF_RANDOM_SPLIT_WEIGHT_INVALID()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0105")))
    assert(
      ex.message.startsWith("Error Code: 0105, Error message: " +
        "The specified weights for randomSplit() must not be negative numbers."))
  }

  test("DF_RANDOM_SPLIT_WEIGHT_ARRAY_EMPTY") {
    val ex = ErrorMessage.DF_RANDOM_SPLIT_WEIGHT_ARRAY_EMPTY()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0106")))
    assert(
      ex.message.startsWith("Error Code: 0106, Error message: " +
        "You cannot pass an empty array of weights to randomSplit()."))
  }

  test("DF_FLATTEN_UNSUPPORTED_INPUT_MODE") {
    val ex = ErrorMessage.DF_FLATTEN_UNSUPPORTED_INPUT_MODE("String")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0107")))
    assert(
      ex.message.startsWith(
        "Error Code: 0107, Error message: " +
          "Unsupported input mode String. For the mode parameter in flatten(), " +
          "you must specify OBJECT, ARRAY, or BOTH."))
  }

  test("DF_CANNOT_RESOLVE_COLUMN_NAME") {
    val ex = ErrorMessage.DF_CANNOT_RESOLVE_COLUMN_NAME("col1", Seq("c1", "c3"))
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0108")))
    assert(
      ex.message.startsWith("Error Code: 0108, Error message: " +
        "The DataFrame does not contain the column named 'col1' and the valid names are c1, c3."))
  }

  test("DF_MUST_PROVIDE_SCHEMA_FOR_READING_FILE") {
    val ex = ErrorMessage.DF_MUST_PROVIDE_SCHEMA_FOR_READING_FILE()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0109")))
    assert(
      ex.message.startsWith("Error Code: 0109, Error message: " +
        "You must call DataFrameReader.schema() and specify the schema for the file."))
  }

  test("DF_CROSS_TAB_COUNT_TOO_LARGE") {
    val ex = ErrorMessage.DF_CROSS_TAB_COUNT_TOO_LARGE(1, 2)
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0110")))
    assert(
      ex.message.startsWith(
        "Error Code: 0110, Error message: " +
          "The number of distinct values in the second input column (1) " +
          "exceeds the maximum number of distinct values allowed (2)."))
  }

  test("DF_DATAFRAME_IS_NOT_QUALIFIED_FOR_SCALAR_QUERY") {
    val ex = ErrorMessage.DF_DATAFRAME_IS_NOT_QUALIFIED_FOR_SCALAR_QUERY(2, "c1, c2")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0111")))
    assert(
      ex.message.startsWith(
        "Error Code: 0111, Error message: " +
          "The DataFrame passed in to this function must have only one output column. " +
          "This DataFrame has 2 output columns: c1, c2"))
  }

  test("DF_PIVOT_ONLY_SUPPORT_ONE_AGG_EXPR") {
    val ex = ErrorMessage.DF_PIVOT_ONLY_SUPPORT_ONE_AGG_EXPR()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0112")))
    assert(
      ex.message.startsWith(
        "Error Code: 0112, Error message: " +
          "You can apply only one aggregate expression to a RelationalGroupedDataFrame " +
          "returned by the pivot() method."))
  }

  test("DF_FUNCTION_ARGS_CANNOT_BE_EMPTY") {
    val ex = ErrorMessage.DF_FUNCTION_ARGS_CANNOT_BE_EMPTY("myFunc")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0113")))
    assert(
      ex.message.startsWith("Error Code: 0113, Error message: " +
        "You must pass a Seq of one or more Columns to function: myFunc"))
  }

  test("DF_WINDOW_BOUNDARY_START_INVALID") {
    val ex = ErrorMessage.DF_WINDOW_BOUNDARY_START_INVALID(123)
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0114")))
    assert(
      ex.message.startsWith("Error Code: 0114, Error message: " +
        "The starting point for the window frame is not a valid integer: 123."))
  }

  test("DF_WINDOW_BOUNDARY_END_INVALID") {
    val ex = ErrorMessage.DF_WINDOW_BOUNDARY_END_INVALID(123)
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0115")))
    assert(
      ex.message.startsWith("Error Code: 0115, Error message: " +
        "The ending point for the window frame is not a valid integer: 123."))
  }

  test("DF_JOIN_INVALID_JOIN_TYPE") {
    val ex = ErrorMessage.DF_JOIN_INVALID_JOIN_TYPE("inner", "left, right")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0116")))
    assert(
      ex.message.startsWith("Error Code: 0116, Error message: " +
        "Unsupported join type 'inner'. Supported join types include: left, right."))
  }

  test("DF_JOIN_INVALID_NATURAL_JOIN_TYPE") {
    val ex = ErrorMessage.DF_JOIN_INVALID_NATURAL_JOIN_TYPE("leftanti")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0117")))
    assert(
      ex.message.startsWith("Error Code: 0117, Error message: " +
        "Unsupported natural join type 'leftanti'."))
  }

  test("DF_JOIN_INVALID_USING_JOIN_TYPE") {
    val ex = ErrorMessage.DF_JOIN_INVALID_USING_JOIN_TYPE("leftanti")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0118")))
    assert(
      ex.message.startsWith("Error Code: 0118, Error message: " +
        "Unsupported using join type 'leftanti'."))
  }

  test("DF_RANGE_STEP_CANNOT_BE_ZERO") {
    val ex = ErrorMessage.DF_RANGE_STEP_CANNOT_BE_ZERO()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0119")))
    assert(
      ex.message.startsWith("Error Code: 0119, Error message: " +
        "The step for range() cannot be 0."))
  }

  test("DF_CANNOT_RENAME_COLUMN_BECAUSE_NOT_EXIST") {
    val ex = ErrorMessage.DF_CANNOT_RENAME_COLUMN_BECAUSE_NOT_EXIST("oldName", "newName")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0120")))
    assert(
      ex.message.startsWith(
        "Error Code: 0120, Error message: " +
          "Unable to rename the column oldName as newName because" +
          " this DataFrame doesn't have a column named oldName."))
  }

  test("DF_CANNOT_RENAME_COLUMN_BECAUSE_MULTIPLE_EXIST") {
    val ex = ErrorMessage.DF_CANNOT_RENAME_COLUMN_BECAUSE_MULTIPLE_EXIST("oldName", "newName", 3)
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0121")))
    assert(
      ex.message.startsWith(
        "Error Code: 0121, Error message: " +
          "Unable to rename the column oldName as newName because" +
          " this DataFrame has 3 columns named oldName."))
  }

  test("DF_COPY_INTO_CANNOT_CREATE_TABLE") {
    val ex = ErrorMessage.DF_COPY_INTO_CANNOT_CREATE_TABLE("table_123")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0122")))
    assert(
      ex.message.startsWith(
        "Error Code: 0122, Error message: " +
          "Cannot create the target table table_123 because Snowpark cannot determine" +
          " the column names to use. You should create the table before calling copyInto()."))
  }

  test("DF_WITH_COLUMNS_INPUT_NAMES_NOT_MATCH_VALUES") {
    val ex = ErrorMessage.DF_WITH_COLUMNS_INPUT_NAMES_NOT_MATCH_VALUES(10, 5)
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0123")))
    assert(
      ex.message.startsWith("Error Code: 0123, Error message: " +
        "The number of column names (10) does not match the number of values (5)."))
  }

  test("DF_WITH_COLUMNS_INPUT_NAMES_CONTAINS_DUPLICATES") {
    val ex = ErrorMessage.DF_WITH_COLUMNS_INPUT_NAMES_CONTAINS_DUPLICATES
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0124")))
    assert(
      ex.message.startsWith("Error Code: 0124, Error message: " +
        "The same column name is used multiple times in the colNames parameter."))
  }

  test("DF_COLUMN_LIST_OF_GENERATOR_CANNOT_BE_EMPTY") {
    val ex = ErrorMessage.DF_COLUMN_LIST_OF_GENERATOR_CANNOT_BE_EMPTY()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0125")))
    assert(
      ex.message.startsWith("Error Code: 0125, Error message: " +
        "The column list of generator function can not be empty."))
  }

  test("DF_WRITER_INVALID_OPTION_NAME") {
    val ex = ErrorMessage.DF_WRITER_INVALID_OPTION_NAME("myOption", "table")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0126")))
    assert(
      ex.message.startsWith("Error Code: 0126, Error message: " +
        "DataFrameWriter doesn't support option 'myOption' when writing to a table."))
  }

  test("DF_WRITER_INVALID_OPTION_VALUE") {
    val ex = ErrorMessage.DF_WRITER_INVALID_OPTION_VALUE("myOption", "myValue", "table")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0127")))
    assert(
      ex.message.startsWith(
        "Error Code: 0127, Error message: " +
          "DataFrameWriter doesn't support to set option 'myOption' as 'myValue'" +
          " when writing to a table."))
  }

  test("DF_WRITER_INVALID_OPTION_NAME_FOR_MODE") {
    val ex =
      ErrorMessage.DF_WRITER_INVALID_OPTION_NAME_IN_MODE(
        "myOption",
        "myValue",
        "Overwrite",
        "table")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0128")))
    assert(ex.message.startsWith("Error Code: 0128, Error message: " +
      "DataFrameWriter doesn't support to set option 'myOption' as 'myValue' in 'Overwrite' mode" +
      " when writing to a table."))
  }

  test("DF_WRITER_INVALID_MODE") {
    val ex = ErrorMessage.DF_WRITER_INVALID_MODE("Append", "file")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0129")))
    assert(
      ex.message.startsWith("Error Code: 0129, Error message: " +
        "DataFrameWriter doesn't support mode 'Append' when writing to a file."))
  }

  test("DF_JOIN_WITH_WRONG_ARGUMENT") {
    val ex = ErrorMessage.DF_JOIN_WITH_WRONG_ARGUMENT()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0130")))
    assert(
      ex.message.startsWith(
        "Error Code: 0130, Error message: " +
          "Unsupported join operations, Dataframes can join with other Dataframes" +
          " or TableFunctions only"))
  }

  test("UDF_INCORRECT_ARGS_NUMBER") {
    val ex = ErrorMessage.UDF_INCORRECT_ARGS_NUMBER(1, 2)
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0200")))
    assert(
      ex.message.startsWith("Error Code: 0200, Error message: " +
        "Incorrect number of arguments passed to the UDF: Expected: 1, Found: 2"))
  }

  test("UDF_FOUND_UNREGISTERED_UDF") {
    val ex = ErrorMessage.UDF_FOUND_UNREGISTERED_UDF()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0201")))
    assert(
      ex.message.startsWith("Error Code: 0201, Error message: " +
        "Attempted to call an unregistered UDF. You must register the UDF before calling it."))
  }

  test("UDF_CANNOT_DETECT_UDF_FUNCION_CLASS") {
    val ex = ErrorMessage.UDF_CANNOT_DETECT_UDF_FUNCION_CLASS()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0202")))
    assert(
      ex.message.startsWith(
        "Error Code: 0202, Error message: " +
          "Unable to detect the location of the enclosing class of the UDF. " +
          "Call session.addDependency, and pass in the path to the directory or JAR file " +
          "containing the compiled class file."))
  }

  test("UDF_NEED_SCALA_2_12_OR_LAMBDAFYMETHOD") {
    val ex = ErrorMessage.UDF_NEED_SCALA_2_12_OR_LAMBDAFYMETHOD()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0203")))
    assert(
      ex.message.startsWith(
        "Error Code: 0203, Error message: " +
          "Unable to clean the closure. You must use a supported version of Scala (2.12+) or " +
          "specify the Scala compiler flag -Dlambdafymethod."))
  }

  test("UDF_RETURN_STATEMENT_IN_CLOSURE_EXCEPTION") {
    val ex = ErrorMessage.UDF_RETURN_STATEMENT_IN_CLOSURE_EXCEPTION()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0204")))
    assert(
      ex.message.startsWith("Error Code: 0204, Error message: " +
        "You cannot include a return statement in a closure."))
  }

  test("UDF_CANNOT_FIND_JAVA_COMPILER") {
    val ex = ErrorMessage.UDF_CANNOT_FIND_JAVA_COMPILER()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0205")))
    assert(
      ex.message.startsWith("Error Code: 0205, Error message: " +
        "Cannot find the JDK. For your development environment, set the JAVA_HOME environment " +
        "variable to the directory where you installed a supported version of the JDK."))
  }

  test("UDF_ERROR_IN_COMPILING_CODE") {
    val ex = ErrorMessage.UDF_ERROR_IN_COMPILING_CODE("'v1' is not defined")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0206")))
    assert(
      ex.message.startsWith("Error Code: 0206, Error message: " +
        "Error compiling your UDF code: 'v1' is not defined"))
  }

  test("UDF_NO_DEFAULT_SESSION_FOUND") {
    val ex = ErrorMessage.UDF_NO_DEFAULT_SESSION_FOUND()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0207")))
    assert(
      ex.message.startsWith(
        "Error Code: 0207, Error message: " +
          "No default Session found. Use <session>.udf.registerTemporary()" +
          " to explicitly refer to a session."))
  }

  test("UDF_INVALID_UDTF_COLUMN_NAME") {
    val ex = ErrorMessage.UDF_INVALID_UDTF_COLUMN_NAME("invalid name")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0208")))
    assert(
      ex.message.startsWith(
        "Error Code: 0208, Error message: " +
          "You cannot use 'invalid name' as an UDTF output schema name " +
          "which needs to be a valid Java identifier."))
  }

  test("UDF_CANNOT_INFER_MAP_TYPES") {
    val ex = ErrorMessage.UDF_CANNOT_INFER_MAP_TYPES()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0209")))
    assert(
      ex.message.startsWith(
        "Error Code: 0209, Error message: " +
          "Cannot determine the input types because the process() method passes in" +
          " Map arguments. In your JavaUDTF class, implement the inputSchema() method to" +
          " describe the input types."))
  }

  test("UDF_CANNOT_INFER_MULTIPLE_PROCESS") {
    val ex = ErrorMessage.UDF_CANNOT_INFER_MULTIPLE_PROCESS(3)
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0210")))
    assert(
      ex.message.startsWith("Error Code: 0210, Error message: " +
        "Cannot determine the input types because the process() method has multiple signatures" +
        " with 3 arguments. In your JavaUDTF class, implement the inputSchema() method to" +
        " describe the input types."))
  }

  test("UDF_INCORRECT_SPROC_ARGS_NUMBER") {
    val ex = ErrorMessage.UDF_INCORRECT_SPROC_ARGS_NUMBER(1, 2)
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0211")))
    assert(
      ex.message.startsWith("Error Code: 0211, Error message: " +
        "Incorrect number of arguments passed to the SProc: Expected: 1, Found: 2"))
  }

  test("UDF_CANNOT_ACCEPT_MANY_DF_COLS") {
    val ex = ErrorMessage.UDF_CANNOT_ACCEPT_MANY_DF_COLS()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0212")))
    assert(
      ex.message.startsWith("Error Code: 0212, Error message: " +
        "Session.tableFunction does not support columns from more than one dataframe as input." +
        " Join these dataframes before using the function"))
  }

  test("UDF_UNEXPECTED_COLUMN_ORDER") {
    val ex = ErrorMessage.UDF_UNEXPECTED_COLUMN_ORDER()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0213")))
    assert(
      ex.message.startsWith(
        "Error Code: 0213, Error message: " +
          "Dataframe resulting from table function has an unexpected column order." +
          " Source DataFrame columns did not come first."))
  }

  test("PLAN_LAST_QUERY_RETURN_RESULTSET") {
    val ex = ErrorMessage.PLAN_LAST_QUERY_RETURN_RESULTSET()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0300")))
    assert(
      ex.message.startsWith(
        "Error Code: 0300, Error message: " +
          "Internal error: the execution for the last query in the snowflake plan " +
          "doesn't return a ResultSet."))
  }

  test("PLAN_ANALYZER_INVALID_NAME") {
    val ex = ErrorMessage.PLAN_ANALYZER_INVALID_IDENTIFIER("wrong_identifier")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0301")))
    assert(
      ex.message.startsWith("Error Code: 0301, Error message: " +
        "Invalid identifier wrong_identifier"))
  }

  test("PLAN_ANALYZER_UNSUPPORTED_VIEW_TYPE") {
    val ex = ErrorMessage.PLAN_ANALYZER_UNSUPPORTED_VIEW_TYPE("wrong view type")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0302")))
    assert(
      ex.message.startsWith(
        "Error Code: 0302, Error message: " +
          "Internal Error: Only PersistedView and LocalTempView are supported. " +
          "view type: wrong view type"))
  }

  test("PLAN_SAMPLING_NEED_ONE_PARAMETER") {
    val ex = ErrorMessage.PLAN_SAMPLING_NEED_ONE_PARAMETER()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0303")))
    assert(
      ex.message.startsWith("Error Code: 0303, Error message: " +
        "You must specify either the fraction of rows or the number of rows to sample."))
  }

  test("PLAN_JOIN_NEED_USING_CLAUSE_OR_JOIN_CONDITION") {
    val ex = ErrorMessage.PLAN_JOIN_NEED_USING_CLAUSE_OR_JOIN_CONDITION()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0304")))
    assert(
      ex.message.startsWith(
        "Error Code: 0304, Error message: " +
          "For the join, you must specify either the conditions for the join or " +
          "the list of columns to use for the join (not both)."))
  }

  test("PLAN_LEFT_SEMI_JOIN_NOT_SUPPORT_USING_CLAUSE") {
    val ex = ErrorMessage.PLAN_LEFT_SEMI_JOIN_NOT_SUPPORT_USING_CLAUSE()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0305")))
    assert(
      ex.message.startsWith("Error Code: 0305, Error message: " +
        "Internal error: Unexpected Using clause in left semi join"))
  }

  test("PLAN_LEFT_ANTI_JOIN_NOT_SUPPORT_USING_CLAUSE") {
    val ex = ErrorMessage.PLAN_LEFT_ANTI_JOIN_NOT_SUPPORT_USING_CLAUSE()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0306")))
    assert(
      ex.message.startsWith("Error Code: 0306, Error message: " +
        "Internal error: Unexpected Using clause in left anti join"))
  }

  test("PLAN_UNSUPPORTED_FILE_OPERATION_TYPE") {
    val ex = ErrorMessage.PLAN_UNSUPPORTED_FILE_OPERATION_TYPE()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0307")))
    assert(
      ex.message.startsWith("Error Code: 0307, Error message: " +
        "Internal error: Unsupported file operation type"))
  }

  test("PLAN_JDBC_REPORT_UNEXPECTED_ALIAS") {
    val ex = ErrorMessage.PLAN_JDBC_REPORT_UNEXPECTED_ALIAS()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0308")))
    assert(
      ex.message.startsWith(
        "Error Code: 0308, Error message: " +
          "You can only define aliases for the root Columns in a DataFrame returned by " +
          "select() and agg(). You cannot use aliases for Columns in expressions."))
  }

  test("PLAN_JDBC_REPORT_INVALID_ID") {
    val ex = ErrorMessage.PLAN_JDBC_REPORT_INVALID_ID("id1")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0309")))
    assert(
      ex.message.startsWith("Error Code: 0309, Error message: " +
        """The column specified in df("id1") is not present in the output of the DataFrame."""))
  }

  test("PLAN_JDBC_REPORT_JOIN_AMBIGUOUS") {
    val ex = ErrorMessage.PLAN_JDBC_REPORT_JOIN_AMBIGUOUS("a", "b")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0310")))
    assert(
      ex.message.startsWith(
        "Error Code: 0310, Error message: " +
          "The reference to the column 'a' is ambiguous. " +
          "The column is present in both DataFrames used in the join. " +
          "To identify the DataFrame that you want to use in the reference, " +
          """use the syntax <df>("b") in join conditions and in select() calls """ +
          "on the result of the join."))
  }

  test("PLAN_COPY_DONT_SUPPORT_SKIP_LOADED_FILES") {
    val ex = ErrorMessage.PLAN_COPY_DONT_SUPPORT_SKIP_LOADED_FILES("false")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0311")))
    assert(
      ex.message.startsWith("Error Code: 0311, Error message: " +
        "The COPY option 'FORCE = false' is not supported by the Snowpark library. " +
        "The Snowflake library loads all files, even if the files have been loaded previously " +
        "and have not changed since they were loaded."))
  }

  test("PLAN_CANNOT_CREATE_LITERAL") {
    val ex = ErrorMessage.PLAN_CANNOT_CREATE_LITERAL("MyClass", "myValue")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0312")))
    assert(
      ex.message.startsWith("Error Code: 0312, Error message: " +
        "Cannot create a Literal for MyClass(myValue)"))
  }

  test("PLAN_UNSUPPORTED_FILE_FORMAT_TYPE") {
    val ex = ErrorMessage.PLAN_UNSUPPORTED_FILE_FORMAT_TYPE("unknown_type")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0313")))
    assert(
      ex.message.startsWith("Error Code: 0313, Error message: " +
        "Internal error: unsupported file format type: 'unknown_type'."))
  }

  test("PLAN_IN_EXPRESSION_UNSUPPORTED_VALUE") {
    val ex = ErrorMessage.PLAN_IN_EXPRESSION_UNSUPPORTED_VALUE("column_A")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0314")))
    assert(
      ex.message.startsWith(
        "Error Code: 0314, Error message: " +
          "'column_A' is not supported for the values parameter of the function in()." +
          " You must either specify a sequence of literals or a DataFrame that" +
          " represents a subquery."))
  }

  test("PLAN_IN_EXPRESSION_INVALID_VALUE_COUNT") {
    val ex = ErrorMessage.PLAN_IN_EXPRESSION_INVALID_VALUE_COUNT(1, 2)
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0315")))
    assert(
      ex.message.startsWith("Error Code: 0315, Error message: " +
        "For the in() function, the number of values 1 does not match the number of columns 2."))
  }

  test("PLAN_COPY_INVALID_COLUMN_NAME_SIZE") {
    val ex = ErrorMessage.PLAN_COPY_INVALID_COLUMN_NAME_SIZE(1, 2)
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0316")))
    assert(
      ex.message.startsWith(
        "Error Code: 0316, Error message: " +
          "Number of column names provided to copy into does not match the number of " +
          "transformations provided. Number of column names: 1, number of transformations: 2."))
  }

  test("PLAN_CANNOT_EXECUTE_IN_ASYNC_MODE") {
    val ex = ErrorMessage.PLAN_CANNOT_EXECUTE_IN_ASYNC_MODE("Plan(q1, q2)")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0317")))
    assert(
      ex.message.startsWith("Error Code: 0317, Error message: " +
        "Cannot execute the following plan asynchronously:\nPlan(q1, q2)"))
  }

  test("PLAN_QUERY_IS_STILL_RUNNING") {
    val ex = ErrorMessage.PLAN_QUERY_IS_STILL_RUNNING("qid_123", "RUNNING", 100)
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0318")))
    assert(
      ex.message.startsWith("Error Code: 0318, Error message: " +
        "The query with the ID qid_123 is still running and has the current status RUNNING." +
        " The function call has been running for 100 seconds, which exceeds the maximum number" +
        " of seconds to wait for the results. Use the `maxWaitTimeInSeconds` argument" +
        " to increase the number of seconds to wait."))
  }

  test("PLAN_CANNOT_SUPPORT_TYPE_FOR_ASYNC_JOB") {
    val ex = ErrorMessage.PLAN_CANNOT_SUPPORT_TYPE_FOR_ASYNC_JOB("MyType")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0319")))
    assert(
      ex.message.startsWith("Error Code: 0319, Error message: " +
        "Internal Error: Unsupported type 'MyType' for TypedAsyncJob."))
  }

  test("PLAN_CANNOT_GET_ASYNC_JOB_RESULT") {
    val ex = ErrorMessage.PLAN_CANNOT_GET_ASYNC_JOB_RESULT("MyType", "myFunc")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0320")))
    assert(
      ex.message.startsWith(
        "Error Code: 0320, Error message: " +
          "Internal Error: Cannot retrieve the value for the type 'MyType'" +
          " in the function 'myFunc'."))
  }

  test("PLAN_MERGE_RETURN_WRONG_ROWS") {
    val ex = ErrorMessage.PLAN_MERGE_RETURN_WRONG_ROWS(1, 2)
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0321")))
    assert(
      ex.message.startsWith("Error Code: 0321, Error message: " +
        "Internal error: Merge statement should return 1 row but returned 2 rows"))
  }

  test("MISC_CANNOT_CAST_VALUE") {
    val ex = ErrorMessage.MISC_CANNOT_CAST_VALUE("MyClass", "value123", "Int")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0400")))
    assert(
      ex.message.startsWith("Error Code: 0400, Error message: " +
        "Cannot cast MyClass(value123) to Int."))
  }

  test("MISC_CANNOT_FIND_CURRENT_DB_OR_SCHEMA") {
    val ex = ErrorMessage.MISC_CANNOT_FIND_CURRENT_DB_OR_SCHEMA("DB", "DB", "DB")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0401")))
    assert(
      ex.message.startsWith(
        "Error Code: 0401, Error message: " +
          "The DB is not set for the current session. To set this, either run " +
          "session.sql(\"USE DB\").collect() or set the DB connection property in " +
          "the Map or properties file that you specify when creating a session."))
  }

  test("MISC_QUERY_IS_CANCELLED") {
    val ex = ErrorMessage.MISC_QUERY_IS_CANCELLED()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0402")))
    assert(
      ex.message.startsWith("Error Code: 0402, Error message: " +
        "The query has been cancelled by the user."))
  }

  test("MISC_INVALID_CLIENT_VERSION") {
    val ex = ErrorMessage.MISC_INVALID_CLIENT_VERSION("0.6.x")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0403")))
    assert(
      ex.message.startsWith("Error Code: 0403, Error message: " +
        "Invalid client version string 0.6.x"))
  }

  test("MISC_INVALID_CLOSURE_CLEANER_PARAMETER") {
    val ex = ErrorMessage.MISC_INVALID_CLOSURE_CLEANER_PARAMETER("my_parameter")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0404")))
    assert(
      ex.message.startsWith("Error Code: 0404, Error message: " +
        "The parameter my_parameter must be 'always', 'never', or 'repl_only'."))
  }

  test("MISC_INVALID_CONNECTION_STRING") {
    val ex = ErrorMessage.MISC_INVALID_CONNECTION_STRING("invalid_connection_string")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0405")))
    assert(
      ex.message.startsWith("Error Code: 0405, Error message: " +
        "Invalid connection string invalid_connection_string"))
  }

  test("MISC_MULTIPLE_VALUES_RETURNED_FOR_PARAMETER") {
    val ex = ErrorMessage.MISC_MULTIPLE_VALUES_RETURNED_FOR_PARAMETER("myParameter")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0406")))
    assert(
      ex.message.startsWith("Error Code: 0406, Error message: " +
        "The server returned multiple values for the parameter myParameter."))
  }

  test("MISC_NO_VALUES_RETURNED_FOR_PARAMETER") {
    val ex = ErrorMessage.MISC_NO_VALUES_RETURNED_FOR_PARAMETER("myParameter")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0407")))
    assert(
      ex.message.startsWith("Error Code: 0407, Error message: " +
        "The server returned no value for the parameter myParameter."))
  }

  test("MISC_SESSION_EXPIRED") {
    val ex = ErrorMessage.MISC_SESSION_EXPIRED("session expired!")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0408")))
    assert(
      ex.message.startsWith("Error Code: 0408, Error message: " +
        "Your Snowpark session has expired. You must recreate your session.\nsession expired!"))
  }

  test("MISC_NESTED_OPTION_TYPE_IS_NOT_SUPPORTED") {
    val ex = ErrorMessage.MISC_NESTED_OPTION_TYPE_IS_NOT_SUPPORTED()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0409")))
    assert(
      ex.message.startsWith("Error Code: 0409, Error message: " +
        "You cannot use a nested Option type (e.g. Option[Option[Int]])."))
  }

  test("MISC_CANNOT_INFER_SCHEMA_FROM_TYPE") {
    val ex = ErrorMessage.MISC_CANNOT_INFER_SCHEMA_FROM_TYPE("MyClass")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0410")))
    assert(
      ex.message.startsWith("Error Code: 0410, Error message: " +
        "Could not infer schema from data of type: MyClass"))
  }

  test("MISC_SCALA_VERSION_NOT_SUPPORTED") {
    val ex = ErrorMessage.MISC_SCALA_VERSION_NOT_SUPPORTED("2.12.6", "2.12", "2.12.9")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0411")))
    assert(
      ex.message.startsWith(
        "Error Code: 0411, Error message: " +
          "Scala version 2.12.6 detected. Snowpark only supports Scala version 2.12 with " +
          "the minor version 2.12.9 and higher."))
  }

  test("MISC_INVALID_OBJECT_NAME") {
    val ex = ErrorMessage.MISC_INVALID_OBJECT_NAME("objName")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0412")))
    assert(
      ex.message.startsWith("Error Code: 0412, Error message: " +
        "The object name 'objName' is invalid."))
  }

  test("MISC_SP_ACTIVE_SESSION_RESET") {
    val ex = ErrorMessage.MISC_SP_ACTIVE_SESSION_RESET()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0413")))
    assert(
      ex.message.startsWith("Error Code: 0413, Error message: " +
        "Unexpected stored procedure active session reset."))
  }

  test("MISC_SESSION_HAS_BEEN_CLOSED") {
    val ex = ErrorMessage.MISC_SESSION_HAS_BEEN_CLOSED()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0414")))
    assert(
      ex.message.startsWith("Error Code: 0414, Error message: " +
        "Cannot perform this operation because the session has been closed."))
  }

  test("MISC_FAILED_CLOSE_SESSION") {
    val ex = ErrorMessage.MISC_FAILED_CLOSE_SESSION("this error message")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0415")))
    assert(
      ex.message.startsWith("Error Code: 0415, Error message: " +
        "Failed to close this session. The error is: this error message"))
  }

  test("MISC_CANNOT_CLOSE_STORED_PROC_SESSION") {
    val ex = ErrorMessage.MISC_CANNOT_CLOSE_STORED_PROC_SESSION()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0416")))
    assert(
      ex.message.startsWith("Error Code: 0416, Error message: " +
        "Cannot close this session because it is used by stored procedure."))
  }

  test("MISC_INVALID_INT_PARAMETER") {
    val ex = ErrorMessage.MISC_INVALID_INT_PARAMETER(
      "abc",
      SnowparkRequestTimeoutInSeconds,
      MIN_REQUEST_TIMEOUT_IN_SECONDS,
      MAX_REQUEST_TIMEOUT_IN_SECONDS)
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0418")))
    assert(
      ex.message.startsWith(
        "Error Code: 0418, Error message: " +
          "Invalid value abc for parameter snowpark_request_timeout_in_seconds. " +
          "Please input an integer value that is between 0 and 604800."))
  }

  test("MISC_REQUEST_TIMEOUT") {
    val ex = ErrorMessage.MISC_REQUEST_TIMEOUT("UDF jar uploading", 10)
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0419")))
    assert(
      ex.message.startsWith("Error Code: 0419, Error message: " +
        "UDF jar uploading exceeds the maximum allowed time: 10 second(s)."))
  }

  test("MISC_INVALID_RSA_PRIVATE_KEY") {
    val ex = ErrorMessage.MISC_INVALID_RSA_PRIVATE_KEY("test message")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0420")))
    assert(
      ex.message.startsWith("Error Code: 0420, Error message: Invalid RSA private key." +
        " The error is: test message"))
  }

  test("MISC_INVALID_STAGE_LOCATION") {
    val ex = ErrorMessage.MISC_INVALID_STAGE_LOCATION("stage", "test message")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0421")))
    assert(
      ex.message.startsWith(
        "Error Code: 0421, Error message: Invalid stage location: stage. Reason: test message."))
  }

  test("MISC_NO_SERVER_VALUE_NO_DEFAULT_FOR_PARAMETER") {
    val ex = ErrorMessage.MISC_NO_SERVER_VALUE_NO_DEFAULT_FOR_PARAMETER("someParameter")
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0422")))
    assert(
      ex.message.startsWith(
        "Error Code: 0422, Error message: Internal error: Server fetching is disabled" +
          " for the parameter someParameter and there is no default value for it."))
  }

  test("MISC_INVALID_TABLE_FUNCTION_INPUT") {
    val ex = ErrorMessage.MISC_INVALID_TABLE_FUNCTION_INPUT()
    assert(ex.telemetryMessage.equals(ErrorMessage.getMessage("0423")))
    assert(
      ex.message.startsWith(
        "Error Code: 0423, Error message: Invalid input argument, " +
          "Session.tableFunction only supports table function arguments"))
  }
}
