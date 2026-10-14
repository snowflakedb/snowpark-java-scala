package com.snowflake.snowpark.internal

import java.io.{Closeable, InputStream}
import java.sql.{PreparedStatement, ResultSet, ResultSetMetaData, SQLException, Statement}
import java.time.LocalDateTime
import com.snowflake.snowpark.{
  MergeBuilder,
  MergeTypedAsyncJob,
  Row,
  SnowparkClientException,
  TypedAsyncJob
}
import com.snowflake.snowpark.internal.ParameterUtils.{
  ClosureCleanerMode,
  DEFAULT_MAX_FILE_DOWNLOAD_RETRY_COUNT,
  DEFAULT_MAX_FILE_UPLOAD_RETRY_COUNT,
  DEFAULT_REQUEST_TIMEOUT_IN_SECONDS,
  DEFAULT_SNOWPARK_USE_SCOPED_TEMP_OBJECTS,
  MAX_REQUEST_TIMEOUT_IN_SECONDS,
  MIN_REQUEST_TIMEOUT_IN_SECONDS,
  SnowparkMaxFileDownloadRetryCount,
  SnowparkMaxFileUploadRetryCount,
  SnowparkRequestTimeoutInSeconds,
  Url
}
import com.snowflake.snowpark.internal.Utils.PackageNameDelimiter
import com.snowflake.snowpark.internal.analyzer.{Attribute, Query, SnowflakePlan}
import com.snowflake.snowpark.internal.jdbc.{
  FieldInfo,
  JdbcBridge,
  JdbcExceptions,
  QueryState,
  StructuredResultReader
}
import com.snowflake.snowpark.types._
import net.snowflake.client.jdbc.internal.apache.arrow.vector.util.{
  JsonStringArrayList,
  JsonStringHashMap
}

import java.util
import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag
import scala.collection.JavaConverters._

private[snowpark] case class QueryResult(
    rows: Option[Array[Row]],
    iterator: Option[Iterator[Row]],
    attributes: Seq[Attribute],
    queryId: String)

private[snowpark] trait CloseableIterator[+A] extends Iterator[A] with Closeable

private[snowpark] object ServerConnection {

  def apply(options: Map[String, String], isScalaAPI: Boolean): ServerConnection = {
    new ServerConnection(options, isScalaAPI, None)
  }

  def convertResultMetaToAttribute(meta: ResultSetMetaData, jdbc: JdbcBridge): Seq[Attribute] =
    (1 to meta.getColumnCount).map(index => {
      val fieldMetadata = jdbc.columnFields(meta, index)
      val columnName = analyzer.quoteNameWithoutUpperCasing(meta.getColumnLabel(index))
      val dataType = meta.getColumnType(index)
      val fieldSize = meta.getPrecision(index)
      val fieldScale = meta.getScale(index)
      val isSigned = meta.isSigned(index)
      val nullable = meta.isNullable(index) != ResultSetMetaData.columnNoNulls
      // This field is useful for snowflake types that are not JDBC types like
      // variant, object and array
      val columnTypeName = meta.getColumnTypeName(index)
      val columnType =
        getDataType(dataType, columnTypeName, fieldSize, fieldScale, isSigned, fieldMetadata)

      Attribute(columnName, columnType, nullable)
    })

  private[snowpark] def getDataType(
      sqlType: Int,
      columnTypeName: String,
      precision: Int,
      scale: Int,
      signed: Boolean,
      field: List[FieldInfo] = List.empty): DataType = {
    columnTypeName match {
      case "ARRAY" =>
        if (field.isEmpty) {
          ArrayType(StringType)
        } else {
          StructuredArrayType(
            getDataType(
              field.head.sqlType,
              field.head.typeName,
              field.head.precision,
              field.head.scale,
              signed = true, // no sign info in the fields
              field.head.fields),
            field.head.nullable)
        }
      case "VARIANT" => VariantType
      case "OBJECT" =>
        if (field.isEmpty) {
          MapType(StringType, StringType)
        } else if (field.size == 2 && field.head.name.isEmpty) {
          // Map
          StructuredMapType(
            getDataType(
              field.head.sqlType,
              field.head.typeName,
              field.head.precision,
              field.head.scale,
              signed = true,
              field.head.fields),
            getDataType(
              field(1).sqlType,
              field(1).typeName,
              field(1).precision,
              field(1).scale,
              signed = true,
              field(1).fields),
            field(1).nullable)
        } else {
          // object
          StructType(
            field.map(f =>
              StructField(
                f.name,
                getDataType(f.sqlType, f.typeName, f.precision, f.scale, signed = true, f.fields),
                f.nullable)))
        }
      case "GEOGRAPHY" => GeographyType
      case "GEOMETRY" => GeometryType
      case "INTERVAL_DAY_TIME" => DayTimeIntervalType
      case "INTERVAL_YEAR_MONTH" => YearMonthIntervalType
      case _ => getTypeFromJDBCType(sqlType, precision, scale, signed)
    }
  }

  private def getTypeFromJDBCType(
      sqlType: Int,
      precision: Int,
      scale: Int,
      signed: Boolean): DataType = {
    val answer = sqlType match {
      case java.sql.Types.BIGINT =>
        if (signed) {
          LongType
        } else {
          DecimalType(20, 0)
        }
      case java.sql.Types.BOOLEAN => BooleanType
      case java.sql.Types.DECIMAL if precision != 0 || scale != 0 =>
        if (precision > DecimalType.MAX_PRECISION) {
          DecimalType(DecimalType.MAX_PRECISION, scale + (precision - DecimalType.MAX_SCALE))
        } else {
          DecimalType(precision, scale)
        }
      case java.sql.Types.DOUBLE => DoubleType
      case java.sql.Types.TIME => TimeType
      case java.sql.Types.DATE => DateType
      case java.sql.Types.TIMESTAMP | java.sql.Types.TIMESTAMP_WITH_TIMEZONE => TimestampType
      case java.sql.Types.VARCHAR => StringType
      case java.sql.Types.BINARY => BinaryType
      // The following three types are likely never reached, but keep them just in case
      case java.sql.Types.DECIMAL => DecimalType(38, 18)
      case java.sql.Types.CHAR => StringType
      case java.sql.Types.INTEGER =>
        if (signed) {
          IntegerType
        } else {
          LongType
        }
      case _ => null
    }

    if (answer == null) throw new SQLException("Unsupported type " + sqlType)
    answer
  }

  private[snowpark] def connectionString(lowerCaseParameters: Map[String, String]): String = {
    require(lowerCaseParameters.contains(Url), s"missing required parameter $Url")

    var url: String = lowerCaseParameters(Url).trim

    // append :443 if no port number specified
    val urlWithPort: String = "^.+:\\d+$"
    if (!url.matches(urlWithPort)) {
      url = s"$url:443"
    }

    s"""jdbc:snowflake://$url"""
  }
}
/*
 * A JDBC connection is created automatically with the options Map,
 *  or, alternatively, a JDBC Connection can be passed in jdbcConn.
 *  Snowflake-specific operations go through `jdbc`, which matches the JDBC driver major version
 *  of the connection.
 */
private[snowpark] class ServerConnection(
    options: Map[String, String],
    val isScalaAPI: Boolean,
    private val jdbcConn: Option[java.sql.Connection])
    extends Logging {

  val isStoredProc = jdbcConn.isDefined
  // convert all parameter keys to lower case, and only use lower case keys internally.
  private val lowerCaseParameters: Map[String, String] = options.map {
    // scalastyle:off
    case (key, value) => key.toLowerCase -> value
    // scalastyle:on
  }

  val connection: java.sql.Connection = jdbcConn.getOrElse {
    val connURL = ServerConnection.connectionString(lowerCaseParameters)
    val connParam = ParameterUtils.jdbcConfig(lowerCaseParameters, isScalaAPI)
    JdbcBridge.createClientConnection(connURL, connParam)
  }

  private[snowpark] val jdbc: JdbcBridge = JdbcBridge.forConnection(connection)

  private[snowpark] def close(): Unit =
    if (connection != null) {
      connection.close()
    }

  private var query_tag: Option[String] = None

  private val QUERY_TAG_NAME = "QUERY_TAG"

  private[snowpark] def getQueryTag(): Option[String] = query_tag

  private[snowpark] def setQueryTag(queryTag: String): Unit = {
    query_tag = Some(queryTag)
    runQuery(s"alter session set $QUERY_TAG_NAME = '$queryTag'")
  }

  private[snowpark] def unsetQueryTag(): Unit = {
    query_tag = None
    runQuery(s"alter session unset $QUERY_TAG_NAME")
  }

  // Returns true if query_tag is set in by setQueryTag() or it was already
  // set as snowflake parameter at session/user or account level.
  private[snowpark] def queryTagSetInSession(): Boolean = {
    query_tag.isDefined || queryTagIsSet
  }

  private[snowpark] def getStatementParameters(
      isDDLOnTempObject: Boolean = false,
      statementParameters: Map[String, Any] = Map.empty): Map[String, Any] = {
    Map.empty[String, Any] ++
      // Only set queryTag if in client mode and if it is not already set
      (if (isStoredProc || queryTagSetInSession()) Map()
       else Map(QUERY_TAG_NAME -> query_tag.getOrElse(Utils.getUserCodeMeta()))) ++
      // Use SNOWPARK_SKIP_TXN_COMMIT_IN_DDL to avoid the DDL command to commit the open transaction
      (if (isDDLOnTempObject) Map("SNOWPARK_SKIP_TXN_COMMIT_IN_DDL" -> true) else Map()) ++
      statementParameters
  }

  private[snowpark] def listServerPackages(): Set[String] =
    runQueryGetResult(
      "select distinct package_name, version from information_schema.packages " +
        s"where language = 'java'",
      true,
      false,
      getStatementParameters(isDDLOnTempObject = false, Map.empty),
      Seq.empty).rows.get
      .map(r => r.getString(0).toLowerCase() + PackageNameDelimiter + r.getString(1).toLowerCase())
      .toSet

  private[snowflake] def setBindingParameters(
      statement: PreparedStatement,
      params: Seq[Any]): Unit =
    params.zipWithIndex.foreach { case (p, i) =>
      statement.setObject(i + 1, p)
    }

  private[snowflake] def setStatementParameters(
      statement: Statement,
      parameters: Map[String, Any]): Unit =
    parameters.foreach { entry =>
      jdbc.setStatementParameter(statement, entry._1, entry._2)
    }

  private[snowpark] def isClosed(): Boolean = connection.isClosed

  lazy private[snowpark] val telemetry: Telemetry = withValidConnection { new Telemetry(this) }

  def getJDBCSessionID: String = withValidConnection {
    jdbc.sessionId
  }

  private[snowpark] def getStringDatum(query: String): String = withValidConnection {
    val rows = runQueryGetRows(query)
    if (rows.nonEmpty) {
      rows(0).getString(0)
    } else {
      null
    }
  }

  private[snowpark] def resultSetToRows(statement: Statement): Array[Row] = withValidConnection {
    val iterator = resultSetToIterator(statement)._1
    val buff = mutable.ArrayBuilder.make[Row]
    while (iterator.hasNext) {
      buff += iterator.next()
    }
    buff.result()
  }

  private[snowpark] def resultSetToIterator(
      statement: Statement): (CloseableIterator[Row], StructType) =
    withValidConnection {
      val data = statement.getResultSet

      val schema = ServerConnection.convertResultMetaToAttribute(data.getMetaData, jdbc)
      val schemaOption = Some(StructType.fromAttributes(schema))

      lazy val geographyOutputFormat = getParameterValue(ParameterUtils.GeographyOutputFormat)
      lazy val geometryOutputFormat = getParameterValue(ParameterUtils.GeometryOutputFormat)

      val iterator = new CloseableIterator[Row] {
        private var _currentRow: Row = _
        private var _hasNext: Boolean = _
        // init
        readNext()

        private def readNext(): Unit = {
          _hasNext = data.next()
          _currentRow = if (_hasNext) {
            Row.fromSeqWithSchema(
              schema.zipWithIndex.map { case (attribute, index) =>
                val resultIndex: Int = index + 1
                val resultSetExt = SnowflakeResultSetExt(data, jdbc)
                if (resultSetExt.isNull(resultIndex)) {
                  null
                } else {
                  attribute.dataType match {
                    case VariantType => data.getString(resultIndex)
                    case _: StructuredArrayType | _: StructuredMapType | _: StructType =>
                      resultSetExt.getObject(resultIndex)
                    case ArrayType(StringType) => data.getString(resultIndex)
                    case MapType(StringType, StringType) => data.getString(resultIndex)
                    case StringType => data.getString(resultIndex)
                    case _: DecimalType => data.getBigDecimal(resultIndex)
                    case DoubleType => data.getDouble(resultIndex)
                    case FloatType => data.getFloat(resultIndex)
                    case BooleanType => data.getBoolean(resultIndex)
                    case BinaryType => data.getBytes(resultIndex)
                    case DateType => data.getDate(resultIndex)
                    case TimeType => data.getTime(resultIndex)
                    case ByteType => data.getByte(resultIndex)
                    case IntegerType => data.getInt(resultIndex)
                    case LongType => data.getLong(resultIndex)
                    case TimestampType => data.getTimestamp(resultIndex)
                    case ShortType => data.getShort(resultIndex)
                    case DayTimeIntervalType =>
                      data.getObject(resultIndex, classOf[java.time.Duration])
                    case YearMonthIntervalType =>
                      data.getObject(resultIndex, classOf[java.time.Period])
                    case GeographyType =>
                      geographyOutputFormat match {
                        case "GeoJSON" => Geography.fromGeoJSON(data.getString(resultIndex))
                        case _ =>
                          throw ErrorMessage.MISC_UNSUPPORTED_GEOGRAPHY_FORMAT(
                            geographyOutputFormat)
                      }
                    case GeometryType =>
                      geometryOutputFormat match {
                        case "GeoJSON" => Geometry.fromGeoJSON(data.getString(resultIndex))
                        case _ =>
                          throw ErrorMessage.MISC_UNSUPPORTED_GEOMETRY_FORMAT(geometryOutputFormat)
                      }
                    case _ =>
                      // ArrayType, StructType, MapType
                      throw new UnsupportedOperationException(
                        s"Unsupported type: ${attribute.dataType}")
                  }
                }
              },
              schemaOption)
          } else {
            // After all rows are consumed, close the statement to release resource
            close()
            null
          }
        }

        override def hasNext: Boolean = _hasNext
        override def next(): Row = {
          val result = _currentRow
          readNext()
          result
        }

        /**
         * Close the underlying data source.
         */
        override def close(): Unit = {
          _hasNext = false
          statement.close()
        }
      }
      (iterator, StructType.fromAttributes(schema))
    }

  def uploadStream(
      stageName: String,
      destPrefix: String,
      inputStream: InputStream,
      destFileName: String,
      compressData: Boolean): Unit = withValidConnection {
    jdbc.uploadStream(stageName, destPrefix, inputStream, destFileName, compressData)
  }

  def downloadStream(stageName: String, sourceFileName: String, decompress: Boolean): InputStream =
    withValidConnection {
      jdbc.downloadStream(stageName, sourceFileName, decompress)
    }

  // Run the query and return the queryID when the caller doesn't need the ResultSet
  def runQuery(
      query: String,
      isDDLOnTempObject: Boolean = false,
      statementParameters: Map[String, Any] = Map.empty,
      params: Seq[Any] = Seq.empty): String =
    runQueryGetResult(
      query,
      returnRows = false,
      returnIterator = false,
      getStatementParameters(isDDLOnTempObject, statementParameters),
      params).queryId

  // Run the query and return the queryID when the caller doesn't need the ResultSet
  def runQueryGetRows(
      query: String,
      statementParameters: Map[String, Any] = Map.empty): Array[Row] =
    runQueryGetResult(
      query,
      returnRows = true,
      returnIterator = false,
      getStatementParameters(isDDLOnTempObject = false, statementParameters),
      Seq.empty).rows.get

  // Run the query to get query result.
  // 1. If the caller needs to get Iterator[Row], the internal JDBC ResultSet and Statement
  //    will NOT be closed.
  // 2. If the caller needs to get Array[Row], the internal JDBC ResultSet and Statement
  //    will be closed.
  // 3. If the caller does't need Array[Row] and Iterator[Row], the internal JDBC ResultSet and
  //    Statement will be closed.
  private[snowpark] def runQueryGetResult(
      query: String,
      returnRows: Boolean,
      returnIterator: Boolean,
      statementParameters: Map[String, Any],
      params: Seq[Any]): QueryResult =
    withValidConnection {
      var statement: PreparedStatement = null
      try {
        statement = connection.prepareStatement(query)
        setBindingParameters(statement, params)
        setStatementParameters(statement, statementParameters)
        val rs = statement.executeQuery()
        val queryID = jdbc.resultSetQueryId(rs)
        logInfo(s"Execute query [queryID: $queryID] $query")
        val meta = ServerConnection.convertResultMetaToAttribute(rs.getMetaData, jdbc)
        if (returnIterator) {
          QueryResult(None, Some(resultSetToIterator(statement)._1), meta, queryID)
        } else if (returnRows) {
          QueryResult(Some(resultSetToRows(statement)), None, meta, queryID)
        } else {
          QueryResult(None, None, meta, queryID)
        }
      } catch {
        case e: Exception =>
          logError(s"failed to execute query:\n$query")
          if (statement != null) {
            statement.close()
          }
          throw e
      } finally {
        // Don't close the statement if the return is Iterator
        if (statement != null && !returnIterator) {
          statement.close()
        }
      }
    }

  private[snowpark] def runBatchInsert(
      query: String,
      attributes: Seq[Attribute],
      rows: Seq[Row],
      statementParameters: Map[String, Any]): String =
    withValidConnection {
      lazy val bigDecimalRoundContext = new java.math.MathContext(DecimalType.MAX_PRECISION)
      val types: Seq[DataType] = attributes.map(_.dataType)

      var preparedStatement: PreparedStatement = null
      try {
        preparedStatement = connection.prepareStatement(query)
        setStatementParameters(preparedStatement, statementParameters)
        rows.foreach { row =>
          types.zipWithIndex.foreach {
            case (StringType, index) =>
              if (row.isNullAt(index)) {
                preparedStatement.setNull(index + 1, java.sql.Types.VARCHAR)
              } else {
                preparedStatement.setString(index + 1, row.getString(index))
              }
            case (_: DecimalType, index) =>
              if (row.isNullAt(index)) {
                preparedStatement.setNull(index + 1, java.sql.Types.DECIMAL)
              } else {
                preparedStatement
                  .setBigDecimal(index + 1, row.getDecimal(index).round(bigDecimalRoundContext))
              }
            case (DoubleType, index) =>
              if (row.isNullAt(index)) {
                preparedStatement.setNull(index + 1, java.sql.Types.DOUBLE)
              } else {
                preparedStatement.setDouble(index + 1, row.getDouble(index))
              }
            case (FloatType, index) =>
              if (row.isNullAt(index)) {
                preparedStatement.setNull(index + 1, java.sql.Types.FLOAT)
              } else {
                preparedStatement.setFloat(index + 1, row.getFloat(index))
              }
            case (BooleanType, index) =>
              if (row.isNullAt(index)) {
                preparedStatement.setNull(index + 1, java.sql.Types.BOOLEAN)
              } else {
                preparedStatement.setBoolean(index + 1, row.getBoolean(index))
              }
            case (BinaryType, index) =>
              if (row.isNullAt(index)) {
                preparedStatement.setNull(index + 1, java.sql.Types.BINARY)
              } else {
                preparedStatement.setBytes(index + 1, row.getBinary(index))
              }
            case (ByteType, index) =>
              if (row.isNullAt(index)) {
                preparedStatement.setNull(index + 1, java.sql.Types.TINYINT)
              } else {
                preparedStatement.setByte(index + 1, row.getByte(index))
              }
            case (IntegerType, index) =>
              if (row.isNullAt(index)) {
                preparedStatement.setNull(index + 1, java.sql.Types.INTEGER)
              } else {
                preparedStatement.setInt(index + 1, row.getInt(index))
              }
            case (LongType, index) =>
              if (row.isNullAt(index)) {
                preparedStatement.setNull(index + 1, java.sql.Types.BIGINT)
              } else {
                preparedStatement.setLong(index + 1, row.getLong(index))
              }
            case (ShortType, index) =>
              if (row.isNullAt(index)) {
                preparedStatement.setNull(index + 1, java.sql.Types.SMALLINT)
              } else {
                preparedStatement.setShort(index + 1, row.getShort(index))
              }
            case (dataType, index) =>
              // ArrayType, StructType, MapType
              throw new UnsupportedOperationException(
                s"Unsupported type: $dataType at $index for Batch Insert")
          }
          preparedStatement.addBatch()
        }
        preparedStatement.executeBatch()
        val queryId = jdbc.statementQueryId(preparedStatement)
        logInfo(s"Execute query [queryID: $queryId] $query")
        queryId
      } finally {
        if (preparedStatement != null) {
          preparedStatement.close()
        }
      }
    }

  def getResultAttributes(query: String): Seq[Attribute] = withValidConnection {
    /* PUT/GET statement will be directly executed in Prepare statement,
    which is a wrong behavior. It will be fixed in SNOW-197267.
    For now, we temporarily fix it by checking the query being executed.
    if it starts with PUT and GET, we will skip the prepare, and return a
    empty schema. and then the Dataframe uses a hard code schema for PUT and GET.
    todo: remove this temporary solution once the JDBC issue fixed.
    Known issue: since we skip the prepare, we don't verify any query
    starts with PUT and GET in schema analyzation. In other words, if an invalid
    query starts with PUT/GET, session.sql(put/get).schema can always returns the
    schema without any error reported.
     */

    // scalastyle:off
    val lowercase = query.trim.toLowerCase
    // scalastyle:on
    if (lowercase.startsWith("put") || lowercase.startsWith("get")) {
      Seq.empty
    } else {
      var statement: PreparedStatement = null
      try {
        statement = connection.prepareStatement(query)
        ServerConnection.convertResultMetaToAttribute(statement.getMetaData, jdbc)
      } catch {
        case e: Exception =>
          logError(s"Failed to analyze schema of query:\n$query")
          throw e
      } finally {
        if (statement != null) {
          statement.close()
        }
      }
    }
  }

  def getDefaultDatabase: Option[String] = withValidConnection {
    lowerCaseParameters.get("db").map(analyzer.quoteName)
  }

  def getDefaultSchema: Option[String] = withValidConnection {
    lowerCaseParameters.get("schema").map(analyzer.quoteName)
  }

  // functions to get database name and schema name
  // Use analyzer.quoteNameWithoutUpperCasing() instead of analyzer.quoteName()
  // to quote the name because JDBC may return the name without quotation,
  // the letter's case needs to be respected.
  def getCurrentDatabase: Option[String] = withValidConnection {
    val databaseName = if (Utils.isStringEmpty(jdbc.database)) {
      val currentDatabaseName = getStringDatum("SELECT CURRENT_DATABASE()")
      if (Utils.isStringEmpty(currentDatabaseName)) {
        throw ErrorMessage.MISC_CANNOT_FIND_CURRENT_DB_OR_SCHEMA("DB", "DB", "DB")
      }
      currentDatabaseName
    } else {
      jdbc.database
    }

    Option(databaseName).map(analyzer.quoteNameWithoutUpperCasing)
  }

  def getCurrentSchema: Option[String] = withValidConnection {
    val schemaName = if (Utils.isStringEmpty(jdbc.schema)) {
      val currentSchema = getStringDatum("SELECT CURRENT_SCHEMA()")
      if (Utils.isStringEmpty(currentSchema)) {
        throw ErrorMessage.MISC_CANNOT_FIND_CURRENT_DB_OR_SCHEMA("SCHEMA", "SCHEMA", "SCHEMA")
      }
      currentSchema
    } else {
      jdbc.schema
    }
    Option(schemaName).map(analyzer.quoteNameWithoutUpperCasing)
  }

  lazy val isLazyAnalysis: Boolean = if (isStoredProc) {
    // Lazy analysis should always be true for stored procs
    true
  } else {
    ParameterUtils.parseBoolean(getParameterValue(ParameterUtils.SnowparkLazyAnalysis))
  }

  def useScopedTempObjects: Boolean =
    ParameterUtils.parseBoolean(
      getParameterValue(
        ParameterUtils.SnowparkUseScopedTempObjects,
        skipActiveRead = false,
        Some(DEFAULT_SNOWPARK_USE_SCOPED_TEMP_OBJECTS)))

  lazy val hideInternalAlias: Boolean =
    ParameterUtils.parseBoolean(
      getParameterValue(
        ParameterUtils.SnowparkHideInternalAlias,
        skipActiveRead = false,
        Some(ParameterUtils.DEFAULT_SNOWPARK_HIDE_INTERNAL_ALIAS)))

  lazy val queryTagIsSet: Boolean = {
    try {
      getParameterValue(QUERY_TAG_NAME).nonEmpty
    } catch {
      // Any error in reading QUERY_TAG session param should result
      // in snowpark skipping the logic to set QUERY_TAG
      case _: SnowparkClientException => true
    }
  }

  // By default enable closure cleaner, but leave this option to disable it.
  lazy val closureCleanerMode: ClosureCleanerMode.Value = ParameterUtils.parseClosureCleanerParam(
    lowerCaseParameters.getOrElse(ParameterUtils.SnowparkEnableClosureCleaner, "repl_only"))

  lazy val requestTimeoutInSeconds: Int = {
    val timeout = readRequestTimeoutSecond
    // Timeout should be greater than 0 and less than 7 days
    if (timeout <= MIN_REQUEST_TIMEOUT_IN_SECONDS
      || timeout >= MAX_REQUEST_TIMEOUT_IN_SECONDS) {
      throw ErrorMessage.MISC_INVALID_INT_PARAMETER(
        timeout.toString,
        SnowparkRequestTimeoutInSeconds,
        MIN_REQUEST_TIMEOUT_IN_SECONDS,
        MAX_REQUEST_TIMEOUT_IN_SECONDS)
    }
    timeout
  }

  lazy val maxFileUploadRetryCount: Int = {
    val maxRetryCount =
      lowerCaseParameters
        .get(SnowparkMaxFileUploadRetryCount)
        .getOrElse(DEFAULT_MAX_FILE_UPLOAD_RETRY_COUNT)
    try {
      maxRetryCount.trim.toInt
    } catch {
      case _: NumberFormatException =>
        throw ErrorMessage.MISC_INVALID_INT_PARAMETER(
          maxRetryCount,
          SnowparkMaxFileUploadRetryCount,
          0,
          Int.MaxValue)
    }
  }

  lazy val maxFileDownloadRetryCount: Int = {
    val maxRetryCount =
      lowerCaseParameters
        .get(SnowparkMaxFileDownloadRetryCount)
        .getOrElse(DEFAULT_MAX_FILE_DOWNLOAD_RETRY_COUNT)
    try {
      maxRetryCount.trim.toInt
    } catch {
      case _: NumberFormatException =>
        throw ErrorMessage.MISC_INVALID_INT_PARAMETER(
          maxRetryCount,
          SnowparkMaxFileDownloadRetryCount,
          0,
          Int.MaxValue)
    }
  }

  private def readRequestTimeoutSecond: Int = {
    val timeoutInput =
      lowerCaseParameters.get(ParameterUtils.SnowparkRequestTimeoutInSeconds)
    if (timeoutInput.isDefined) {
      try {
        timeoutInput.get.trim.toInt
      } catch {
        case _: NumberFormatException =>
          throw ErrorMessage.MISC_INVALID_INT_PARAMETER(
            timeoutInput.get,
            SnowparkRequestTimeoutInSeconds,
            MIN_REQUEST_TIMEOUT_IN_SECONDS,
            MAX_REQUEST_TIMEOUT_IN_SECONDS)
      }
    } else {
      // Avoid query server for the parameter if JDBC does not have the parameter in GS's response
      getParameterValue(
        ParameterUtils.SnowparkRequestTimeoutInSeconds,
        skipActiveRead = true,
        Some(DEFAULT_REQUEST_TIMEOUT_IN_SECONDS)).toInt
    }
  }

  def execute(plan: SnowflakePlan): Array[Row] = withValidConnection {
    executePlanInternal(plan, false).rows.get
  }

  def executePlanGetQueryId(
      plan: SnowflakePlan,
      statementParameters: Map[String, Any] = Map.empty): String =
    withValidConnection {
      val queryResult = executePlanInternal(
        plan,
        true,
        statementParameters,
        useStatementParametersForLastQueryOnly = true)
      queryResult.iterator.foreach(_.asInstanceOf[CloseableIterator[Row]].close())
      queryResult.queryId
    }

  def getRowIterator(plan: SnowflakePlan): Iterator[Row] = withValidConnection {
    // Snowpark can't close the result statement before the result is not consumed yet.
    executePlanInternal(plan, true).iterator.get
  }

  // get result and Attribute from ResultSet (not prepare)
  def getResultAndMetadata(plan: SnowflakePlan): (Array[Row], Seq[Attribute]) =
    withValidConnection {
      val queryResult = executePlanInternal(plan, false)
      (queryResult.rows.get, queryResult.attributes)
    }

  private def executePlanInternal(
      plan: SnowflakePlan,
      returnIterator: Boolean,
      statementParameters: Map[String, Any] = Map.empty,
      useStatementParametersForLastQueryOnly: Boolean = false): QueryResult =
    withValidConnection {
      SnowflakePlan.wrapException(plan) {
        val actionID = plan.session.generateNewActionID
        val statementsParameterForLastQuery = statementParameters
        val statementParametersForOthers: Map[String, Any] =
          if (useStatementParametersForLastQueryOnly) Map.empty else statementParameters
        logDebug(s"""
                  |----------SNOW-----------
                  |$plan
                  |-------------------------
                  |""".stripMargin)

        // use try finally to ensure postActions is always run
        try {
          val placeholders = mutable.HashMap.empty[String, String]
          // prerequisites
          plan.queries
            .dropRight(1)
            .foreach(query => {
              if (actionID <= plan.session.getLastCanceledID) {
                throw ErrorMessage.MISC_QUERY_IS_CANCELLED()
              }
              query.runQuery(this, placeholders, statementParametersForOthers)
            })
          val result = plan.queries.last.runQueryGetResult(
            this,
            placeholders,
            returnIterator,
            statementsParameterForLastQuery)
          plan.reportSimplifierUsage(result.queryId)
          result
        } finally {
          // delete created tmp object
          val placeholders = mutable.HashMap.empty[String, String]
          plan.postActions.foreach(_.runQuery(this, placeholders, statementParametersForOthers))
        }
      }
    }

  private[snowpark] def executeAsync[T: TypeTag](
      plan: SnowflakePlan,
      mergeBuilder: Option[MergeBuilder] = None): TypedAsyncJob[T] =
    withValidConnection {
      SnowflakePlan.wrapException(plan) {
        if (!plan.supportAsyncMode) {
          throw ErrorMessage.PLAN_CANNOT_EXECUTE_IN_ASYNC_MODE(plan.toString)
        }
        val actionID = plan.session.generateNewActionID

        logDebug(s"""execute plan in async mode:
                   |----------SNOW-----------
                   |$plan
                   |-------------------------
                   |""".stripMargin)

        // use try finally to ensure postActions is always run
        val queries = plan.queries.map(_.sql)
        val multipleStatements = queries.mkString("; ")
        val statement = connection.prepareStatement(multipleStatements)
        try {
          // Note binding parameters only supported for single query
          val bindingParameters = plan.queries.map(_.params).flatten
          if (plan.queries.length > 1 && bindingParameters.length > 0) {
            throw ErrorMessage.BINDING_PARAMETER_MULTI_STATEMENT_NOT_SUPPORTED
          }
          val statementParameters = getStatementParameters() +
            ("MULTI_STATEMENT_COUNT" -> plan.queries.size)
          setBindingParameters(statement, bindingParameters)
          setStatementParameters(statement, statementParameters)
          val rs = jdbc.executeAsyncQuery(statement)
          val queryID = jdbc.resultSetQueryId(rs)
          if (actionID <= plan.session.getLastCanceledID) {
            throw ErrorMessage.MISC_QUERY_IS_CANCELLED()
          }
          if (mergeBuilder.isEmpty) {
            new TypedAsyncJob[T](queryID, plan.session, Some(plan))
          } else {
            new MergeTypedAsyncJob(queryID, plan.session, Some(plan), mergeBuilder.get)
              .asInstanceOf[TypedAsyncJob[T]]
          }
        } finally {
          statement.close()
        }
      }
    }

  private[snowpark] def isDone(queryID: String): Boolean =
    !jdbc.queryState(queryID).isStillRunning

  private[snowpark] def waitForQueryDone(
      queryID: String,
      maxWaitTimeInSeconds: Long): QueryState = {
    // This function needs to check query status in a loop.
    // Sleep for an amount before trying again. Exponential backoff up to 5 seconds
    // implemented. The sleep backoff strategy comes from JDBC Async query.
    val retryPattern = Array(1, 1, 2, 3, 4, 8, 10)
    def getSeepTime(retry: Int) = retryPattern(retry.min(retryPattern.length - 1)) * 500

    var qs = jdbc.queryState(queryID)
    var retry = 0
    var lastLogTime = 0
    var totalWaitTime = 0
    while (qs.isStillRunning &&
      totalWaitTime + getSeepTime(retry + 1) < maxWaitTimeInSeconds * 1000) {
      Thread.sleep(getSeepTime(retry))
      totalWaitTime = totalWaitTime + getSeepTime(retry)
      qs = jdbc.queryState(queryID)
      retry = retry + 1
      if (totalWaitTime - lastLogTime > 60 * 1000 || lastLogTime == 0) {
        logWarning(
          s"Checking the query status for $queryID at ${LocalDateTime.now()}," +
            s" the current status is $qs.")
        lastLogTime = totalWaitTime
      }
    }
    if (qs.isStillRunning) {
      throw ErrorMessage.PLAN_QUERY_IS_STILL_RUNNING(queryID, qs.toString, totalWaitTime / 1000)
    }
    qs
  }

  private[snowpark] def getAsyncResult(
      queryID: String,
      maxWaitTimeInSecond: Long,
      plan: Option[SnowflakePlan]): (Iterator[Row], StructType) =
    withValidConnection {
      SnowflakePlan.wrapException(plan.toSeq: _*) {
        val statement = connection.createStatement()
        setStatementParameters(statement, getStatementParameters())
        // get last result set
        try {
          // Wait for the query done.
          waitForQueryDone(queryID, maxWaitTimeInSecond)
          val queryIDs = jdbc.childQueryIds(queryID)
          val lastQueryId = queryIDs.last
          statement.executeQuery(Query.resultScanQuery(lastQueryId).sql)
          val placeholders = mutable.HashMap.empty[String, String]
          plan.foreach(_.postActions.foreach(_.runQuery(this, placeholders)))
          resultSetToIterator(statement)
        } catch {
          case t: Throwable =>
            val details = if (plan.nonEmpty) s" The plan is: ${plan.get}" else ""
            logError(s"Fail to get the async query result for $queryID.$details", t)
            statement.close()
            throw t
        }
      }
    }

  // There are three ways to get a parameter and this function will perform these in order:
  // 1. Try to read from JDBC.getOtherParameter
  // 2. If no result and if skipActiveRead == false, try to issue a `show parameters like ...`
  //    to read the value
  // 3. If skipActiveRead == true or the active read failed, try to return the provided
  //    default value
  private[snowpark] def getParameterValue(
      parameterName: String,
      skipActiveRead: Boolean = false,
      defaultValue: Option[String] = None): String = withValidConnection {
    // Step 1:
    var result: String = jdbc.sessionParameter(parameterName.toUpperCase())
    if (result == null && !skipActiveRead) {
      // Step 2:
      // This rarely happens and usually indicates bug during parameter reading, so logging an info.
      // Most parameter reading should be done in Session.getOtherParameter
      logInfo(s"Actively querying parameter $parameterName from server.")
      val statement = connection.prepareStatement(s"SHOW PARAMETERS LIKE '$parameterName'")
      try {
        val resultSet = statement.executeQuery()
        if (resultSet.next()) {
          result = resultSet.getString("value")
          if (resultSet.next()) {
            result = null
            throw ErrorMessage.MISC_MULTIPLE_VALUES_RETURNED_FOR_PARAMETER(parameterName)
          }
        } else {
          throw ErrorMessage.MISC_NO_VALUES_RETURNED_FOR_PARAMETER(parameterName)
        }
      } catch {
        // If default value if provided, do not throw error in this step
        case e: Exception =>
          if (defaultValue.isEmpty) throw e
          logInfo(
            s"Actively query failed for parameter $parameterName." +
              s" Error: ${e.getMessage} Use default value: $defaultValue.")
      } finally {
        statement.close()
      }
    }
    // Step 3:
    if (result != null) {
      result
    } else if (defaultValue.isDefined) {
      defaultValue.get
    } else {
      throw ErrorMessage.MISC_NO_SERVER_VALUE_NO_DEFAULT_FOR_PARAMETER(parameterName)
    }
  }

  private def withValidConnection[T](thunk: => T): T =
    if (connection.isClosed) {
      throw ErrorMessage.MISC_SESSION_HAS_BEEN_CLOSED()
    } else {
      try {
        thunk
      } catch {
        case e: Exception if JdbcExceptions.isReauthenticationRequest(e) =>
          // can't report via telemetry since no connection
          throw ErrorMessage.MISC_SESSION_EXPIRED(e.getMessage)
        case e: Exception =>
          telemetry.reportErrorMessage(e)
          throw e
      }
    }
}

private[snowflake] object SnowflakeResultSetExt {
  def apply(data: ResultSet, jdbc: JdbcBridge): SnowflakeResultSetExt =
    new SnowflakeResultSetExt(data, jdbc)
}
// Reads structured values through the driver-specific reader of the connection's JDBC version
private[snowflake] class SnowflakeResultSetExt(data: ResultSet, jdbc: JdbcBridge) {
  private lazy val reader: StructuredResultReader = jdbc.structuredResult(data)

  def isNull(index: Int): Boolean =
    reader.rawObject(index) == null

  def getObject(index: Int): Any = {
    val meta = data.getMetaData
    // convert meta to field meta
    val field = FieldInfo(
      meta.getColumnName(index),
      meta.getColumnTypeName(index),
      meta.getColumnType(index),
      0,
      0,
      true,
      jdbc.columnFields(meta, index))
    convertToSnowparkValue(reader.rawObject(index), field)
  }

  private def convertToSnowparkValue(value: Any, meta: FieldInfo): Any = {
    meta.typeName match {
      // semi structured
      case "ARRAY" if meta.fields.isEmpty => value.toString
      // structured array
      case "ARRAY" if meta.fields.size == 1 =>
        value match {
          case arr: util.ArrayList[_] => // for JDBC older than 3.20.0
            arr.toArray
              .map(v => convertToSnowparkValue(v, meta.fields.head))
          case arr if reader.unwrapStructuredObject(arr).isDefined => // for JDBC 3.21.0+
            reader
              .unwrapStructuredObject(arr)
              .get
              .asInstanceOf[JsonStringArrayList[_]]
              .asScala
              .map(v => convertToSnowparkValue(v, meta.fields.head))
          case _ =>
            throw new IllegalArgumentException(
              s"Unsupported Structured Array Type: ${value.getClass.getSimpleName}");
        }

      // semi-structured
      case "OBJECT" if meta.fields.isEmpty => value.toString
      // structured map, Map type has two fields, and both field names are empty
      case "OBJECT" if meta.fields.size == 2 && meta.fields.head.name.isEmpty =>
        value match {
          // nested structured maps are JsonStringArrayValues
          case subMap: JsonStringArrayList[_] =>
            subMap.asScala.map { case mapValue: JsonStringHashMap[_, _] =>
              convertToSnowparkValue(mapValue.get("key"), meta.fields.head) ->
                convertToSnowparkValue(mapValue.get("value"), meta.fields(1))
            }.toMap
          case map: util.HashMap[_, _] =>
            map.asScala.map { case (key, value) =>
              convertToSnowparkValue(key, meta.fields.head) ->
                convertToSnowparkValue(value, meta.fields(1))
            }.toMap
          case map if reader.unwrapStructuredObject(map).isDefined => // for JDBC 3.21.0 +
            reader
              .unwrapStructuredObject(map)
              .get
              .asInstanceOf[util.HashMap[_, _]]
              .asScala
              .map { case (key, value) =>
                convertToSnowparkValue(key, meta.fields.head) ->
                  convertToSnowparkValue(value, meta.fields(1))
              }
              .toMap
        }
      // object, object's field name can't be empty
      case "OBJECT" if reader.unwrapStructuredObject(value).isDefined =>
        reader.unwrapStructuredObject(value).get match {
          case arrowSqlInput if reader.unwrapSqlInput(arrowSqlInput).isDefined =>
            convertToSnowparkValue(reader.unwrapSqlInput(arrowSqlInput).get, meta)
          case map: java.util.Map[String, _] =>
            Row.fromMap(
              map.asScala.toList
                .zip(meta.fields)
                .map { case ((key, value), metadata) =>
                  key -> convertToSnowparkValue(value, metadata)
                }
                .toMap)
        }
      case "OBJECT" => // JDBC older than 3.20.0
        value match {
          case arrowSqlInput if reader.unwrapSqlInput(arrowSqlInput).isDefined =>
            convertToSnowparkValue(reader.unwrapSqlInput(arrowSqlInput).get, meta)
          case map: java.util.Map[String, _] =>
            Row.fromMap(
              map.asScala.toList
                .zip(meta.fields)
                .map { case ((key, value), metadata) =>
                  key -> convertToSnowparkValue(value, metadata)
                }
                .toMap)
        }

      case "NUMBER" if meta.sqlType == java.sql.Types.BIGINT =>
        value match {
          case str: String => str.toLong // number key in structured map
          case bd: java.math.BigDecimal => bd.toBigInteger.longValue()
        }
      case "DOUBLE" | "BOOLEAN" | "BINARY" | "NUMBER" => value
      case "VARCHAR" | "VARIANT" => value.toString // Text to String
      case "INTERVAL_DAY_TIME" =>
        val nanos = value.asInstanceOf[Long]
        val sign = java.lang.Long.signum(nanos)
        val abs = Math.abs(nanos)
        val d = java.time.Duration.ofSeconds(abs / 1000000000L, abs % 1000000000L)
        if (sign < 0) d.negated() else d
      case "INTERVAL_YEAR_MONTH" =>
        val months = value.asInstanceOf[Number].intValue()
        java.time.Period.of(months / 12, months % 12, 0)
      case "DATE" =>
        reader.toDate(value)
      case "TIME" =>
        reader.toTime(value, meta.scale)
      case _
          if meta.sqlType == java.sql.Types.TIMESTAMP ||
            meta.sqlType == java.sql.Types.TIMESTAMP_WITH_TIMEZONE =>
        reader.toTimestamp(value, meta.sqlType, meta.scale)
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported type: ${meta.typeName}")
    }
  }
}
