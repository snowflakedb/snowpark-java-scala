package com.snowflake.snowpark.internal

import com.snowflake.snowpark.SnowparkClientException

/*
 * Steps to define new error message:
 * 1. Define a new message in `allMessages` which is a map from error code to error message
 *    For example, ("0100" -> "Could not drop column %s. Can only drop columns by name")
 *    a) The error codes are partitioned
 *    b) Error message is of type String
 * 2. Define a function to create SnowparkClientException, for example,
 *    def DF_CANNOT_DROP_COLUMN_NAME(colName: String): SnowparkClientException =
 *         createException("0100", colName)
 * 3. Add one test case for the message in ErrorMessageSuite, for example,
 *    test("DF_CANNOT_DROP_COLUMN_NAME")
 * 4. Use the function when throwing the Exception, for example,
 *    throw ErrorMessage.DF_CANNOT_DROP_COLUMN_NAME("col1")
 *
 * Error codes partitioning:
 *   "00NN": Reserve for some special usage
 *   "01NN": DataFrame error code
 *   "02NN": UDF error code
 *   "03NN": Plan analysis and execution error code
 *   "04NN": miscellaneous error code
 *
 *   N can be any digits from 0 to 9.
 */
private[snowpark] object ErrorMessage {
  @inline final val ERROR_CODE_UNKNOWN: String = "0000"

  // The map of error_code -> error_message
  // scalastyle:off
  private val allMessages = Map(
    "0010" -> "internal test message: %s.",
    // Begin to define DataFrame related messages
    "0100" -> "Unable to drop the column %s. You must specify the column by name (e.g. df.drop(col(\"a\"))).",
    "0101" -> "For sort(), you must specify at least one sort expression.",
    "0102" -> "Cannot drop all columns",
    "0103" -> "Cannot combine the DataFrames by column names. The column \"%s\" is not a column in the other DataFrame (%s).",
    "0104" -> """You cannot join a DataFrame with itself because the column references cannot be resolved correctly.
              | Instead, call clone() to create a copy of the DataFrame, and join the DataFrame with this copy.
              |""".stripMargin.replaceAll("\n", ""),
    "0105" -> "The specified weights for randomSplit() must not be negative numbers.",
    "0106" -> "You cannot pass an empty array of weights to randomSplit().",
    "0107" -> "Unsupported input mode %s. For the mode parameter in flatten(), you must specify OBJECT, ARRAY, or BOTH.",
    "0108" -> "The DataFrame does not contain the column named '%s' and the valid names are %s.",
    "0109" -> "You must call DataFrameReader.schema() and specify the schema for the file.",
    "0110" -> "The number of distinct values in the second input column (%d) exceeds the maximum number of distinct values allowed (%d).",
    "0111" -> "The DataFrame passed in to this function must have only one output column. This DataFrame has %d output columns: %s",
    "0112" -> "You can apply only one aggregate expression to a RelationalGroupedDataFrame returned by the pivot() method.",
    "0113" -> "You must pass a Seq of one or more Columns to function: %s",
    "0114" -> "The starting point for the window frame is not a valid integer: %d.",
    "0115" -> "The ending point for the window frame is not a valid integer: %d.",
    "0116" -> "Unsupported join type '%s'. Supported join types include: %s.",
    "0117" -> "Unsupported natural join type '%s'.",
    "0118" -> "Unsupported using join type '%s'.",
    "0119" -> "The step for range() cannot be 0.",
    "0120" -> "Unable to rename the column %s as %s because this DataFrame doesn't have a column named %s.",
    "0121" -> "Unable to rename the column %s as %s because this DataFrame has %d columns named %s.",
    "0122" -> """Cannot create the target table %s because Snowpark cannot determine the column names to use.
                | You should create the table before calling copyInto().
                |""".stripMargin.replaceAll("\n", ""),
    "0123" -> "The number of column names (%d) does not match the number of values (%d).",
    "0124" -> "The same column name is used multiple times in the colNames parameter.",
    "0125" -> "The column list of generator function can not be empty.",
    "0126" -> "DataFrameWriter doesn't support option '%s' when writing to a %s.",
    "0127" -> "DataFrameWriter doesn't support to set option '%s' as '%s' when writing to a %s.",
    "0128" -> "DataFrameWriter doesn't support to set option '%s' as '%s' in '%s' mode when writing to a %s.",
    "0129" -> "DataFrameWriter doesn't support mode '%s' when writing to a %s.",
    "0130" -> "Unsupported join operations, Dataframes can join with other Dataframes or TableFunctions only",
    "0131" -> "At most one table function can be called inside select() function",
    "0132" -> "Duplicated dataframe alias defined: %s",
    // Begin to define UDF related messages
    "0200" -> "Incorrect number of arguments passed to the UDF: Expected: %d, Found: %d",
    "0201" -> "Attempted to call an unregistered UDF. You must register the UDF before calling it.",
    "0202" -> """Unable to detect the location of the enclosing class of the UDF. Call session.addDependency,
               | and pass in the path to the directory or JAR file containing the compiled class file.
               |""".stripMargin.replaceAll("\n", ""),
    "0203" -> "Unable to clean the closure. You must use a supported version of Scala (2.12+) or specify the Scala compiler flag -Dlambdafymethod.",
    "0204" -> "You cannot include a return statement in a closure.",
    "0205" -> "Cannot find the JDK. For your development environment, set the JAVA_HOME environment variable to the directory where you installed a supported version of the JDK.",
    "0206" -> "Error compiling your UDF code: %s",
    "0207" -> "No default Session found. Use <session>.udf.registerTemporary() to explicitly refer to a session.",
    "0208" -> "You cannot use '%s' as an UDTF output schema name which needs to be a valid Java identifier.",
    "0209" -> "Cannot determine the input types because the process() method passes in Map arguments. In your JavaUDTF class, implement the inputSchema() method to describe the input types.",
    "0210" -> "Cannot determine the input types because the process() method has multiple signatures with %d arguments. In your JavaUDTF class, implement the inputSchema() method to describe the input types.",
    "0211" -> "Incorrect number of arguments passed to the SProc: Expected: %d, Found: %d",
    "0212" -> "Session.tableFunction does not support columns from more than one dataframe as input. Join these dataframes before using the function",
    "0213" -> "Dataframe resulting from table function has an unexpected column order. Source DataFrame columns did not come first.",
    // Begin to define Snowflake plan analysis & execution related messages
    "0300" -> "Internal error: the execution for the last query in the snowflake plan doesn't return a ResultSet.",
    "0301" -> "Invalid identifier %s",
    "0302" -> "Internal Error: Only PersistedView and LocalTempView are supported. view type: %s",
    "0303" -> "You must specify either the fraction of rows or the number of rows to sample.",
    "0304" -> "For the join, you must specify either the conditions for the join or the list of columns to use for the join (not both).",
    "0305" -> "Internal error: Unexpected Using clause in left semi join",
    "0306" -> "Internal error: Unexpected Using clause in left anti join",
    "0307" -> "Internal error: Unsupported file operation type",
    "0308" -> "You can only define aliases for the root Columns in a DataFrame returned by select() and agg(). You cannot use aliases for Columns in expressions.",
    "0309" -> """The column specified in df("%s") is not present in the output of the DataFrame.""",
    "0310" -> """The reference to the column '%s' is ambiguous.
                | The column is present in both DataFrames used in the join.
                | To identify the DataFrame that you want to use in the reference,
                | use the syntax <df>("%s") in join conditions and in select() calls
                | on the result of the join.
                |""".stripMargin.replaceAll("\n", ""),
    "0311" -> """The COPY option 'FORCE = %s' is not supported by the Snowpark library.
              | The Snowflake library loads all files, even if the files have been loaded previously
              | and have not changed since they were loaded.
              |""".stripMargin.replaceAll("\n", ""),
    "0312" -> "Cannot create a Literal for %s(%s)",
    "0313" -> "Internal error: unsupported file format type: '%s'.",
    "0314" -> """'%s' is not supported for the values parameter of the function in().
              | You must either specify a sequence of literals or a DataFrame that represents a subquery.
              |""".stripMargin.replaceAll("\n", ""),
    "0315" -> "For the in() function, the number of values %d does not match the number of columns %d.",
    "0316" ->
      """Number of column names provided to copy into does not match the number of transformations
        | provided. Number of column names: %d, number of transformations: %d.
        |""".stripMargin.replaceAll("\n", ""),
    "0317" -> "Cannot execute the following plan asynchronously:\n%s",
    "0318" -> """The query with the ID %s is still running and has the current status %s.
                | The function call has been running for %d seconds, which exceeds
                | the maximum number of seconds to wait for the results. Use the
                | `maxWaitTimeInSeconds` argument to increase the number of seconds to wait.
                |""".stripMargin.replaceAll("\n", ""),
    "0319" -> "Internal Error: Unsupported type '%s' for TypedAsyncJob.",
    "0320" -> "Internal Error: Cannot retrieve the value for the type '%s' in the function '%s'.",
    "0321" -> "Internal error: Merge statement should return %d row but returned %d rows",
    // Begin to define miscellaneous messages
    "0400" -> "Cannot cast %s(%s) to %s.",
    "0401" -> """The %s is not set for the current session. To set this, either run
                | session.sql("USE %s").collect() or set the %s connection property in the Map or properties file
                | that you specify when creating a session.
                |""".stripMargin.replaceAll("\n", ""),
    "0402" -> "The query has been cancelled by the user.",
    "0403" -> "Invalid client version string %s",
    "0404" -> "The parameter %s must be 'always', 'never', or 'repl_only'.",
    "0405" -> "Invalid connection string %s",
    "0406" -> "The server returned multiple values for the parameter %s.",
    "0407" -> "The server returned no value for the parameter %s.",
    "0408" -> "Your Snowpark session has expired. You must recreate your session.\n%s",
    "0409" -> "You cannot use a nested Option type (e.g. Option[Option[Int]]).",
    "0410" -> "Could not infer schema from data of type: %s",
    "0411" -> "Scala version %s detected. Snowpark only supports Scala version %s with the minor version %s and higher.",
    "0412" -> "The object name '%s' is invalid.",
    "0413" -> "Unexpected stored procedure active session reset.",
    "0414" -> "Cannot perform this operation because the session has been closed.",
    "0415" -> "Failed to close this session. The error is: %s",
    "0416" -> "Cannot close this session because it is used by stored procedure.",
    "0417" -> "Unsupported Geography output format: %s. Please set session parameter GEOGRAPHY_OUTPUT_FORMAT to GeoJSON.",
    "0418" -> "Invalid value %s for parameter %s. Please input an integer value that is between %d and %d.",
    "0419" -> "%s exceeds the maximum allowed time: %d second(s).",
    "0420" -> "Invalid RSA private key. The error is: %s",
    "0421" -> "Invalid stage location: %s. Reason: %s.",
    "0422" -> "Internal error: Server fetching is disabled for the parameter %s and there is no default value for it.",
    "0423" -> "Invalid input argument, Session.tableFunction only supports table function arguments",
    "0424" ->
      """Invalid input argument type, the input argument type of Explode function should be either Map or Array types.
        |The input argument type: %s
        |""".stripMargin,
    "0425" -> "Unsupported Geometry output format: %s. Please set session parameter GEOMETRY_OUTPUT_FORMAT to GeoJSON.",
    "0426" -> "The given query tag must be a valid JSON string. Ensure it's correctly formatted as JSON.",
    "0427" -> "The query tag of the current session must be a valid JSON string. Current query tag: %s",
    "0428" -> "Failed to serialize the query tag into a JSON string."
  )
  // scalastyle:on

  /*
   * 0NN: Reserve for some special usage
   */
  def INTERNAL_TEST_MESSAGE(message: String): SnowparkClientException =
    createException("0010", message)

  /*
   * 1NN: DataFrame error code
   */
  def DF_CANNOT_DROP_COLUMN_NAME(colName: String): SnowparkClientException =
    createException("0100", colName)
  def DF_SORT_NEED_AT_LEAST_ONE_EXPR(): SnowparkClientException = createException("0101")
  def DF_CANNOT_DROP_ALL_COLUMNS(): SnowparkClientException = createException("0102")
  def DF_CANNOT_RESOLVE_COLUMN_NAME_AMONG(
      colName: String,
      allColumns: String
  ): SnowparkClientException =
    createException("0103", colName, allColumns)
  def DF_SELF_JOIN_NOT_SUPPORTED(): SnowparkClientException = createException("0104")
  def DF_RANDOM_SPLIT_WEIGHT_INVALID(): SnowparkClientException = createException("0105")
  def DF_RANDOM_SPLIT_WEIGHT_ARRAY_EMPTY(): SnowparkClientException = createException("0106")
  def DF_FLATTEN_UNSUPPORTED_INPUT_MODE(mode: String): SnowparkClientException =
    createException("0107", mode)
  def DF_CANNOT_RESOLVE_COLUMN_NAME(
      colName: String,
      names: Traversable[String]
  ): SnowparkClientException =
    createException("0108", colName, names.mkString(", "))

  def DF_MUST_PROVIDE_SCHEMA_FOR_READING_FILE(): SnowparkClientException =
    createException("0109")
  def DF_CROSS_TAB_COUNT_TOO_LARGE(count: Long, maxCount: Long): SnowparkClientException =
    createException("0110", count, maxCount)
  def DF_DATAFRAME_IS_NOT_QUALIFIED_FOR_SCALAR_QUERY(
      count: Long,
      columns: String
  ): SnowparkClientException =
    createException("0111", count, columns)
  def DF_PIVOT_ONLY_SUPPORT_ONE_AGG_EXPR(): SnowparkClientException =
    createException("0112")
  def DF_FUNCTION_ARGS_CANNOT_BE_EMPTY(funcName: String): SnowparkClientException =
    createException("0113", funcName)
  def DF_WINDOW_BOUNDARY_START_INVALID(startValue: Long): SnowparkClientException =
    createException("0114", startValue)
  def DF_WINDOW_BOUNDARY_END_INVALID(endValue: Long): SnowparkClientException =
    createException("0115", endValue)
  def DF_JOIN_INVALID_JOIN_TYPE(type1: String, types: String): SnowparkClientException =
    createException("0116", type1, types)
  def DF_JOIN_INVALID_NATURAL_JOIN_TYPE(joinType: String): SnowparkClientException =
    createException("0117", joinType)
  def DF_JOIN_INVALID_USING_JOIN_TYPE(joinType: String): SnowparkClientException =
    createException("0118", joinType)
  def DF_RANGE_STEP_CANNOT_BE_ZERO(): SnowparkClientException =
    createException("0119")
  def DF_CANNOT_RENAME_COLUMN_BECAUSE_NOT_EXIST(
      oldName: String,
      newName: String
  ): SnowparkClientException =
    createException("0120", oldName, newName, oldName)
  def DF_CANNOT_RENAME_COLUMN_BECAUSE_MULTIPLE_EXIST(
      oldName: String,
      newName: String,
      times: Int
  ): SnowparkClientException =
    createException("0121", oldName, newName, times, oldName)
  def DF_COPY_INTO_CANNOT_CREATE_TABLE(name: String): SnowparkClientException =
    createException("0122", name)
  def DF_WITH_COLUMNS_INPUT_NAMES_NOT_MATCH_VALUES(
      nameSize: Int,
      valueSize: Int
  ): SnowparkClientException =
    createException("0123", nameSize, valueSize)
  def DF_WITH_COLUMNS_INPUT_NAMES_CONTAINS_DUPLICATES: SnowparkClientException =
    createException("0124")
  def DF_COLUMN_LIST_OF_GENERATOR_CANNOT_BE_EMPTY(): SnowparkClientException =
    createException("0125")
  def DF_WRITER_INVALID_OPTION_NAME(name: String, target: String): SnowparkClientException =
    createException("0126", name, target)
  def DF_WRITER_INVALID_OPTION_VALUE(
      name: String,
      value: String,
      target: String
  ): SnowparkClientException =
    createException("0127", name, value, target)
  def DF_WRITER_INVALID_OPTION_NAME_IN_MODE(
      name: String,
      value: String,
      mode: String,
      target: String
  ): SnowparkClientException =
    createException("0128", name, value, mode, target)
  def DF_WRITER_INVALID_MODE(mode: String, target: String): SnowparkClientException =
    createException("0129", mode, target)

  def DF_JOIN_WITH_WRONG_ARGUMENT(): SnowparkClientException =
    createException("0130")

  def DF_MORE_THAN_ONE_TF_IN_SELECT(): SnowparkClientException =
    createException("0131")

  def DF_ALIAS_DUPLICATES(duplicatedAlias: scala.collection.Set[String]): SnowparkClientException =
    createException("0132", duplicatedAlias.mkString(", "))

  /*
   * 2NN: UDF error code
   */
  def UDF_INCORRECT_ARGS_NUMBER(expected: Int, actual: Int): SnowparkClientException =
    createException("0200", expected, actual)
  def UDF_FOUND_UNREGISTERED_UDF(): SnowparkClientException = createException("0201")
  def UDF_CANNOT_DETECT_UDF_FUNCION_CLASS(): SnowparkClientException = createException("0202")
  def UDF_NEED_SCALA_2_12_OR_LAMBDAFYMETHOD(): SnowparkClientException = createException("0203")
  def UDF_RETURN_STATEMENT_IN_CLOSURE_EXCEPTION(): SnowparkClientException =
    createException("0204")
  def UDF_CANNOT_FIND_JAVA_COMPILER(): SnowparkClientException = createException("0205")
  def UDF_ERROR_IN_COMPILING_CODE(error: String): SnowparkClientException =
    createException("0206", error)
  def UDF_NO_DEFAULT_SESSION_FOUND(): SnowparkClientException = createException("0207")
  def UDF_INVALID_UDTF_COLUMN_NAME(name: String): SnowparkClientException =
    createException("0208", name)
  def UDF_CANNOT_INFER_MAP_TYPES(): SnowparkClientException = createException("0209")
  def UDF_CANNOT_INFER_MULTIPLE_PROCESS(count: Int): SnowparkClientException =
    createException("0210", count)
  def UDF_INCORRECT_SPROC_ARGS_NUMBER(expected: Int, actual: Int): SnowparkClientException =
    createException("0211", expected, actual)
  def UDF_CANNOT_ACCEPT_MANY_DF_COLS(): SnowparkClientException = createException("0212")
  def UDF_UNEXPECTED_COLUMN_ORDER(): SnowparkClientException = createException("0213")
  /*
   * 3NN: Plan analysis and execution error code
   */
  def PLAN_LAST_QUERY_RETURN_RESULTSET(): SnowparkClientException = createException("0300")
  def PLAN_ANALYZER_INVALID_IDENTIFIER(name: String): SnowparkClientException =
    createException("0301", name)
  def PLAN_ANALYZER_UNSUPPORTED_VIEW_TYPE(typeName: String): SnowparkClientException =
    createException("0302", typeName)
  def PLAN_SAMPLING_NEED_ONE_PARAMETER(): SnowparkClientException = createException("0303")
  def PLAN_JOIN_NEED_USING_CLAUSE_OR_JOIN_CONDITION(): SnowparkClientException =
    createException("0304")
  def PLAN_LEFT_SEMI_JOIN_NOT_SUPPORT_USING_CLAUSE(): SnowparkClientException =
    createException("0305")
  def PLAN_LEFT_ANTI_JOIN_NOT_SUPPORT_USING_CLAUSE(): SnowparkClientException =
    createException("0306")
  def PLAN_UNSUPPORTED_FILE_OPERATION_TYPE(): SnowparkClientException =
    createException("0307")
  def PLAN_JDBC_REPORT_UNEXPECTED_ALIAS(): SnowparkClientException =
    createException("0308")
  def PLAN_JDBC_REPORT_INVALID_ID(name: String): SnowparkClientException =
    createException("0309", name)
  def PLAN_JDBC_REPORT_JOIN_AMBIGUOUS(c1: String, c2: String): SnowparkClientException =
    createException("0310", c1, c2)
  def PLAN_COPY_DONT_SUPPORT_SKIP_LOADED_FILES(value: String): SnowparkClientException =
    createException("0311", value)
  def PLAN_CANNOT_CREATE_LITERAL(cls: String, value: String): SnowparkClientException =
    createException("0312", cls, value)
  def PLAN_UNSUPPORTED_FILE_FORMAT_TYPE(format: String): SnowparkClientException =
    createException("0313", format)
  def PLAN_IN_EXPRESSION_UNSUPPORTED_VALUE(value: String): SnowparkClientException =
    createException("0314", value)
  def PLAN_IN_EXPRESSION_INVALID_VALUE_COUNT(actual: Int, expected: Int): SnowparkClientException =
    createException("0315", actual, expected)
  def PLAN_COPY_INVALID_COLUMN_NAME_SIZE(actual: Int, expected: Int): SnowparkClientException =
    createException("0316", actual, expected)
  def PLAN_CANNOT_EXECUTE_IN_ASYNC_MODE(plan: String): SnowparkClientException =
    createException("0317", plan)
  def PLAN_QUERY_IS_STILL_RUNNING(
      queryID: String,
      status: String,
      waitTime: Long
  ): SnowparkClientException =
    createException("0318", queryID, status, waitTime)
  def PLAN_CANNOT_SUPPORT_TYPE_FOR_ASYNC_JOB(typeName: String): SnowparkClientException =
    createException("0319", typeName)
  def PLAN_CANNOT_GET_ASYNC_JOB_RESULT(
      typeName: String,
      funcName: String
  ): SnowparkClientException =
    createException("0320", typeName, funcName)
  def PLAN_MERGE_RETURN_WRONG_ROWS(expected: Int, actual: Int): SnowparkClientException =
    createException("0321", expected, actual)

  /*
   * 4NN: miscellaneous error code
   */
  def MISC_CANNOT_CAST_VALUE(
      sourceType: String,
      value: String,
      targetType: String
  ): SnowparkClientException =
    createException("0400", sourceType, value, targetType)
  def MISC_CANNOT_FIND_CURRENT_DB_OR_SCHEMA(
      v1: String,
      v2: String,
      v3: String
  ): SnowparkClientException =
    createException("0401", v1, v2, v3)
  def MISC_QUERY_IS_CANCELLED(): SnowparkClientException = createException("0402")
  def MISC_INVALID_CLIENT_VERSION(version: String): SnowparkClientException =
    createException("0403", version)
  def MISC_INVALID_CLOSURE_CLEANER_PARAMETER(version: String): SnowparkClientException =
    createException("0404", version)
  def MISC_INVALID_CONNECTION_STRING(connectionString: String): SnowparkClientException =
    createException("0405", connectionString)
  def MISC_MULTIPLE_VALUES_RETURNED_FOR_PARAMETER(parameterName: String): SnowparkClientException =
    createException("0406", parameterName)
  def MISC_NO_VALUES_RETURNED_FOR_PARAMETER(parameterName: String): SnowparkClientException =
    createException("0407", parameterName)
  def MISC_SESSION_EXPIRED(errorMessage: String): SnowparkClientException =
    createException("0408", errorMessage)
  def MISC_NESTED_OPTION_TYPE_IS_NOT_SUPPORTED(): SnowparkClientException =
    createException("0409")
  def MISC_CANNOT_INFER_SCHEMA_FROM_TYPE(typeName: String): SnowparkClientException =
    createException("0410", typeName)
  def MISC_SCALA_VERSION_NOT_SUPPORTED(
      currentVersion: String,
      expectedVersion: String,
      minorVersion: String
  ): SnowparkClientException =
    createException("0411", currentVersion, expectedVersion, minorVersion)
  def MISC_INVALID_OBJECT_NAME(typeName: String): SnowparkClientException =
    createException("0412", typeName)
  def MISC_SP_ACTIVE_SESSION_RESET(): SnowparkClientException =
    createException("0413")
  def MISC_SESSION_HAS_BEEN_CLOSED(): SnowparkClientException =
    createException("0414")
  def MISC_FAILED_CLOSE_SESSION(message: String): SnowparkClientException =
    createException("0415", message)
  def MISC_CANNOT_CLOSE_STORED_PROC_SESSION(): SnowparkClientException =
    createException("0416")
  def MISC_UNSUPPORTED_GEOGRAPHY_FORMAT(typeName: String): SnowparkClientException =
    createException("0417", typeName)
  def MISC_INVALID_INT_PARAMETER(
      value: String,
      parameter: String,
      min: Long,
      max: Long
  ): SnowparkClientException =
    createException("0418", value, parameter, min, max)
  def MISC_REQUEST_TIMEOUT(eventName: String, maxTime: Long): SnowparkClientException =
    createException("0419", eventName, maxTime)
  def MISC_INVALID_RSA_PRIVATE_KEY(message: String): SnowparkClientException =
    createException("0420", message)
  def MISC_INVALID_STAGE_LOCATION(stageLocation: String, reason: String): SnowparkClientException =
    createException("0421", stageLocation, reason)
  def MISC_NO_SERVER_VALUE_NO_DEFAULT_FOR_PARAMETER(
      parameterName: String
  ): SnowparkClientException =
    createException("0422", parameterName)

  def MISC_INVALID_TABLE_FUNCTION_INPUT(): SnowparkClientException =
    createException("0423")

  def MISC_INVALID_EXPLODE_ARGUMENT_TYPE(argumentType: String): SnowparkClientException =
    createException("0424", argumentType)

  def MISC_UNSUPPORTED_GEOMETRY_FORMAT(typeName: String): SnowparkClientException =
    createException("0425", typeName)

  def MISC_INVALID_INPUT_QUERY_TAG(): SnowparkClientException =
    createException("0426")

  def MISC_INVALID_CURRENT_QUERY_TAG(currentQueryTag: String): SnowparkClientException =
    createException("0427", currentQueryTag)

  def MISC_FAILED_TO_SERIALIZE_QUERY_TAG(): SnowparkClientException =
    createException("0428")

  /** Create Snowpark client Exception.
    *
    * @param errorCode
    *   error code for the message
    * @param args
    *   parameters for the Exception
    * @return
    *   Snowpark client Exception
    */
  private def createException(errorCode: String, args: Any*): SnowparkClientException = {
    val message = allMessages(errorCode)
    new SnowparkClientException(
      s"Error Code: $errorCode, Error message: ${message.format(args: _*)}",
      errorCode,
      message
    )
  }

  private[snowpark] def getMessage(errorCode: String) = allMessages(errorCode)

}
