package com.snowflake.snowpark.internal.analyzer

import java.util.Locale

import com.snowflake.snowpark.internal.{
  ErrorMessage,
  Logging,
  SchemaUtils,
  ServerConnection,
  QueryResult
}
import com.snowflake.snowpark.internal.analyzer.SnowflakePlan.wrapException
import com.snowflake.snowpark.{DataFrame, Row, SaveMode, Session}
import com.snowflake.snowpark.FileOperationCommand._
import com.snowflake.snowpark.internal.Utils.{TempObjectType, randomNameForTempObject}
import com.snowflake.snowpark.types.StructType
import net.snowflake.client.jdbc.SnowflakeSQLException

import scala.collection.immutable.HashSet
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class SnowflakePlan(
    val queries: Seq[Query],
    // a query to analyze result schema.
    // it will be replaced by values statement every time when querying attributes
    // if it is empty string, attributes will be an empty seq.
    private var _schemaQuery: String,
    val postActions: Seq[Query],
    val session: Session,
    // the plan that this SnowflakePlan translated from
    val sourcePlan: Option[LogicalPlan],
    val supportAsyncMode: Boolean)
    extends LogicalPlan {

  lazy val attributes: Seq[Attribute] = {
    val output = SchemaUtils.analyzeAttributes(_schemaQuery, session)
    _schemaQuery = schemaValueStatement(output)
    output
  }

  private[snowpark] def analyzeIfneeded(): Unit = wrapException(this) {
    if (!session.isLazyAnalysis) {
      attributes
    }
  }

  // Turns schema analyze to lazy mode, save processing time when constructing
  // DataFrame, but delay the input query verification.
  analyzeIfneeded()

  def withSubqueries(subqueryPlans: Array[SnowflakePlan]): SnowflakePlan = wrapException(this) {
    val preQueries = ArrayBuffer(queries.dropRight(1): _*)
    var newSchemaQuery = schemaQuery
    val newPostActions = ArrayBuffer(postActions: _*)
    subqueryPlans.foreach { plan =>
      // Add preceding queries with de-duplication
      plan.queries
        .dropRight(1)
        .foreach { query =>
          if (!preQueries.contains(query)) {
            preQueries.append(query)
          }
        }
      // Replace schema query
      newSchemaQuery = newSchemaQuery.replace(plan.queries.last.sql, plan.schemaQuery)
      // Append post actions
      plan.postActions.foreach { action =>
        if (!newPostActions.contains(action)) {
          newPostActions.append(action)
        }
      }
    }
    val supportAsyncMode = subqueryPlans.forall(_.supportAsyncMode)
    SnowflakePlan(
      preQueries :+ queries.last,
      newSchemaQuery,
      newPostActions,
      session,
      sourcePlan,
      supportAsyncMode)
  }

  def schemaQuery: String = {
    require(_schemaQuery.nonEmpty, "Schema query can't be empty")
    _schemaQuery
  }

  override def children: Seq[LogicalPlan] = Nil

  lazy val output = attributes
  lazy val schema: StructType = StructType.fromAttributes(output)

  override def toString: String =
    s"SnowflakePlan[${queries.mkString(";\n")}]"

  override def clone: SnowflakePlan =
    new SnowflakePlan(queries, schemaQuery, postActions, session, sourcePlan, supportAsyncMode)

  override def equals(obj: Any): Boolean = obj.toString.equals(toString)

  override def hashCode(): Int = super.hashCode()

  override protected def analyze: LogicalPlan =
    sourcePlan.map(_.analyzed).getOrElse(this)

  override protected def analyzer: ExpressionAnalyzer =
    ExpressionAnalyzer(sourcePlan.map(_.aliasMap).getOrElse(Map.empty), dfAliasMap)

  override def getSnowflakePlan: Option[SnowflakePlan] = Some(this)

  // SnowflakePlan is a leaf node in optimizing process,
  // if source plan is not empty, SnowflakePlan will be replaced
  // by source plan in analyzing process.
  override def updateChildren(func: LogicalPlan => LogicalPlan): LogicalPlan = this

  // telemetry: simplifier usage
  private var simplifierUsageGenerator: Option[String => Unit] = None
  // set a function sends simplifier usage report to server
  // the argument of this function is the ID of the
  // query processed this sql query.
  // this func generated by analyzer since all plans be simplified at there.
  def setSimplifierUsageGenerator(func: String => Unit): Unit = {
    this.simplifierUsageGenerator = Option(func)
  }
  def reportSimplifierUsage(queryID: String): Unit = {
    simplifierUsageGenerator.foreach {
      case func => func(queryID)
      case _ => // do nothing, if no generator set
    }
  }

  lazy override val internalRenamedColumns: Map[String, String] =
    sourcePlan.map(_.internalRenamedColumns).getOrElse(Map.empty)
}

object SnowflakePlan extends Logging {

  def apply(
      queries: Seq[Query],
      schemaQuery: String,
      session: Session,
      sourcePlan: Option[LogicalPlan],
      supportAsyncMode: Boolean): SnowflakePlan =
    new SnowflakePlan(queries, schemaQuery, Seq.empty, session, sourcePlan, supportAsyncMode)

  def apply(
      queries: Seq[Query],
      schemaQuery: String,
      postActions: Seq[Query],
      session: Session,
      sourcePlan: Option[LogicalPlan],
      supportAsyncMode: Boolean): SnowflakePlan =
    new SnowflakePlan(queries, schemaQuery, postActions, session, sourcePlan, supportAsyncMode)

  def wrapException[T](children: LogicalPlan*)(thunk: => T): T = {
    try {
      thunk
    } catch {
      // If an aliased column is used in an expression, this results in a cryptic compilation
      // error.
      case ex: SnowflakeSQLException
          if ex.getMessage.toLowerCase(Locale.ROOT).contains(s"unexpected 'as'") =>
        val newEx = ErrorMessage.PLAN_JDBC_REPORT_UNEXPECTED_ALIAS()
        newEx.initCause(ex)
        throw newEx

      // Because we generate column names automatically for joins, we want to make the
      // invalid identifier error messages more useful, because the invalid identifiers may
      // be auto-generated columns the user has no idea about.
      case ex: SnowflakeSQLException
          if (ex.getSQLState() == "42000" && ex.getMessage.contains("invalid identifier")) =>
        // Parse out the actual invalid identifier from the message.
        val ColPattern = """(?s).*invalid identifier '"?([^'"]*)"?'.*""".r
        val col = ex.getMessage() match {
          case ColPattern(colName) => colName
          case _ => throw ex
        }
        // Check if the column deemed "invalid" is an auto-generated alias.
        // The replaceAll strips surrounding quotes.
        val remapped = children.flatMap(_.aliasMap.values).map(_.replaceAll("^\"|\"$", ""))
        if (remapped.count(_.equals(col)) > 0) {
          val origColName = DataFrame.getUnaliased(col).headOption.getOrElse("<colname>")
          val newEx = ErrorMessage.PLAN_JDBC_REPORT_INVALID_ID(origColName)
          newEx.initCause(ex)
          throw newEx
          // Check if the invalid column was auto-aliased to something else. We check for > 1
          // because we  only want to change the error message if it was aliased
          // to avoid join-ambiguity.
        } else if (remapped.flatMap(DataFrame.getUnaliased).count(_.equals(col)) > 1) {
          val newEx = ErrorMessage.PLAN_JDBC_REPORT_JOIN_AMBIGUOUS(col, col)
          newEx.initCause(ex)
          throw newEx
        } else {
          throw ex
        }
    }
  }

  val CopyOptionForCopyIntoTable: HashSet[String] = HashSet(
    "ON_ERROR",
    "SIZE_LIMIT",
    "PURGE",
    "RETURN_FAILED_ONLY",
    "MATCH_BY_COLUMN_NAME",
    "ENFORCE_LENGTH",
    "TRUNCATECOLUMNS",
    "FORCE",
    "LOAD_UNCERTAIN_FILES")

  private[snowpark] final val FormatTypeOptionsForCopyIntoLocation = HashSet(
    "FORMAT_NAME",
    // Below are format Type Options
    "COMPRESSION",
    "RECORD_DELIMITER",
    "FIELD_DELIMITER",
    "FILE_EXTENSION",
    "ESCAPE",
    "ESCAPE_UNENCLOSED_FIELD",
    "DATE_FORMAT",
    "TIME_FORMAT",
    "TIMESTAMP_FORMAT",
    "BINARY_FORMAT",
    "FIELD_OPTIONALLY_ENCLOSED_BY",
    "NULL_IF",
    "EMPTY_FIELD_AS_NULL",
    "FILE_EXTENSION",
    "SNAPPY_COMPRESSION")

  private[snowpark] final val CopyOptionsForCopyIntoLocation =
    HashSet(
      // "OVERWRITE", // OVERWRITE need to be set by DataFrameWriter.mode()
      "SINGLE",
      "MAX_FILE_SIZE",
      "INCLUDE_QUERY_ID",
      "DETAILED_OUTPUT",
      "VALIDATION_MODE")

  private[snowpark] final val CopySubClausesForCopyIntoLocation =
    HashSet("PARTITION BY", "HEADER")
}

class SnowflakePlanBuilder(session: Session) extends Logging {
  import SnowflakePlan._

  private def build(
      sqlGenerator: String => String,
      child: SnowflakePlan,
      sourcePlan: Option[LogicalPlan],
      schemaQuery: Option[String] = None,
      isDDLOnTempObject: Boolean = false): SnowflakePlan = {
    val multipleSqlGenerator = (sql: String) => Seq(sqlGenerator(sql))
    buildFromMultipleQueries(
      multipleSqlGenerator,
      child,
      sourcePlan,
      schemaQuery,
      isDDLOnTempObject)
  }

  private def buildFromMultipleQueries(
      multipleSqlGenerator: String => Seq[String],
      child: SnowflakePlan,
      sourcePlan: Option[LogicalPlan],
      schemaQuery: Option[String],
      isDDLOnTempObject: Boolean): SnowflakePlan = wrapException(child) {
    val selectChild = addResultScanIfNotSelect(child)
    val lastQuery = selectChild.queries.last
    val queries: Seq[Query] = selectChild.queries.slice(0, selectChild.queries.length - 1) ++
      multipleSqlGenerator(lastQuery.sql).map(Query(_, isDDLOnTempObject, lastQuery.params))
    val newSchemaQuery = schemaQuery.getOrElse(multipleSqlGenerator(child.schemaQuery).last)
    SnowflakePlan(
      queries,
      newSchemaQuery,
      selectChild.postActions,
      session,
      sourcePlan,
      selectChild.supportAsyncMode)
  }

  private def build(
      sqlGenerator: (String, String) => String,
      left: SnowflakePlan,
      right: SnowflakePlan,
      sourcePlan: Option[LogicalPlan]): SnowflakePlan = wrapException(left, right) {
    val selectLeft = addResultScanIfNotSelect(left)
    val lastQueryLeft = selectLeft.queries.last
    val selectRight = addResultScanIfNotSelect(right)
    val lastQueryRight = selectRight.queries.last
    val queries: Seq[Query] =
      selectLeft.queries.slice(0, selectLeft.queries.length - 1) ++
        selectRight.queries.slice(0, selectRight.queries.length - 1) :+ Query(
        sqlGenerator(lastQueryLeft.sql, lastQueryRight.sql),
        false,
        lastQueryLeft.params ++ lastQueryRight.params)
    val leftSchemaQuery = schemaValueStatement(selectLeft.attributes)
    val rightSchemaQuery = schemaValueStatement(selectRight.attributes)
    val schemaQuery = sqlGenerator(leftSchemaQuery, rightSchemaQuery)
    val supportAsyncMode = selectLeft.supportAsyncMode && selectRight.supportAsyncMode
    SnowflakePlan(
      queries,
      schemaQuery,
      selectLeft.postActions ++ selectRight.postActions,
      session,
      sourcePlan,
      supportAsyncMode)
  }

  private def buildGroup(
      sqlGenerator: Seq[String] => String,
      children: Seq[SnowflakePlan],
      sourcePlan: Option[LogicalPlan]): SnowflakePlan = wrapException(children: _*) {
    val selectChildren = children.map(addResultScanIfNotSelect)
    val params: Seq[Any] = selectChildren.map(_.queries.last.params).flatten
    val queries: Seq[Query] =
      selectChildren
        .map(c => c.queries.slice(0, c.queries.length - 1))
        .reduce(_ ++ _) :+ Query(
        sqlGenerator(selectChildren.map(_.queries.last.sql)),
        false,
        params)

    val schemaQueries = children.map(c => schemaValueStatement(c.attributes))
    val schemaQuery = sqlGenerator(schemaQueries)
    val supportAsyncMode = selectChildren.map(_.supportAsyncMode).reduce(_ & _)
    val postActions = children.map(_.postActions).reduce(_ ++ _)
    SnowflakePlan(queries, schemaQuery, postActions, session, sourcePlan, supportAsyncMode)
  }

  def query(
      sql: String,
      sourcePlan: Option[LogicalPlan],
      supportAsyncMode: Boolean = true,
      params: Seq[Any] = Seq.empty): SnowflakePlan =
    SnowflakePlan(Seq(Query(sql, false, params)), sql, session, sourcePlan, supportAsyncMode)

  def largeLocalRelationPlan(
      output: Seq[Attribute],
      data: Seq[Row],
      sourcePlan: Option[LogicalPlan]): SnowflakePlan = {
    val tempTableName = randomNameForTempObject(TempObjectType.Table)
    val attributes = output.map { spAtt =>
      Attribute(spAtt.name, spAtt.dataType, spAtt.nullable)
    }
    val tempType: TempType = session.getTempType(isTemp = true, isNameGenerated = true)
    val crtStmt = createTableStatement(
      tempTableName,
      attributeToSchemaString(attributes),
      tempType = tempType)
    // In the post action we dropped this temp table. Still adding this to the deletion list to
    // be safe in deletion. We rely on 15 digits alphabetic-numeric random string in naming temp
    // objects on not shadowing user's permanent objects.
    session.recordTempObjectIfNecessary(TempObjectType.Table, tempTableName, tempType)
    val insertStmt = batchInsertIntoStatement(tempTableName, attributes.map { _.name })
    val selectStmt = projectStatement(Seq.empty, tempTableName)
    val dropTableStmt = dropTableIfExistsStatement(tempTableName)
    val schemaQuery = schemaValueStatement(attributes)
    val queries =
      Seq(Query(crtStmt, true), Query(insertStmt, attributes, data), Query(selectStmt))
    // Batch insert cannot be executed in async mode
    SnowflakePlan(
      queries,
      schemaQuery,
      Seq(Query(dropTableStmt, true)),
      session,
      sourcePlan,
      supportAsyncMode = false)
  }

  def table(tableName: String): SnowflakePlan =
    query(projectStatement(Seq.empty, tableName), None)

  def fileOperationPlan(
      command: FileOperationCommand,
      fileName: String,
      stageLocation: String,
      options: Map[String, String]): SnowflakePlan =
    // source plan is not necessary in action
    query(
      fileOperationStatement(command, fileName, stageLocation, options),
      None,
      supportAsyncMode = false)

  def project(
      projectList: Seq[String],
      child: SnowflakePlan,
      sourcePlan: Option[LogicalPlan],
      isDistinct: Boolean = false): SnowflakePlan =
    build(projectStatement(projectList, _, isDistinct), child, sourcePlan)

  def projectAndFilter(
      projectList: Seq[String],
      condition: String,
      child: SnowflakePlan,
      sourcePlan: Option[LogicalPlan]): SnowflakePlan =
    build(projectAndFilterStatement(projectList, condition, _), child, sourcePlan)

  def aggregate(
      groupingExpressions: Seq[String],
      aggregateExpressions: Seq[String],
      child: SnowflakePlan,
      sourcePlan: Option[LogicalPlan]): SnowflakePlan =
    build(aggregateStatement(groupingExpressions, aggregateExpressions, _), child, sourcePlan)

  def filter(
      condition: String,
      child: SnowflakePlan,
      sourcePlan: Option[LogicalPlan]): SnowflakePlan =
    build(filterStatement(condition, _), child, sourcePlan)

  def update(
      tableName: String,
      assignments: Map[String, String],
      condition: Option[String],
      sourceData: Option[SnowflakePlan]): SnowflakePlan = {
    query(
      updateStatement(tableName, assignments, condition, sourceData.map(_.queries.last.sql)),
      None)
  }

  def delete(
      tableName: String,
      condition: Option[String],
      sourceData: Option[SnowflakePlan]): SnowflakePlan = {
    query(deleteStatement(tableName, condition, sourceData.map(_.queries.last.sql)), None)
  }

  def merge(
      tableName: String,
      source: SnowflakePlan,
      joinExpr: String,
      clauses: Seq[String]): SnowflakePlan = {
    query(mergeStatement(tableName, source.queries.last.sql, joinExpr, clauses), None)
  }

  def sample(
      probabilityFraction: Option[Double],
      rowCount: Option[Long],
      child: SnowflakePlan,
      sourcePlan: Option[LogicalPlan]): SnowflakePlan =
    build(sampleStatement(probabilityFraction, rowCount, _), child, sourcePlan)

  def sort(
      order: Seq[String],
      child: SnowflakePlan,
      sourcePlan: Option[LogicalPlan]): SnowflakePlan =
    build(sortStatement(order, _), child, sourcePlan)

  def setOperator(
      left: SnowflakePlan,
      right: SnowflakePlan,
      op: String,
      sourcePlan: Option[LogicalPlan]): SnowflakePlan =
    build(setOperatorStatement(_, _, op), left, right, sourcePlan)

  def setOperator(
      children: Seq[SnowflakePlan],
      op: String,
      sourcePlan: Option[LogicalPlan]): SnowflakePlan =
    buildGroup(setOperatorStatement(_: Seq[String], op), children, sourcePlan)

  def join(
      left: SnowflakePlan,
      right: SnowflakePlan,
      joinType: JoinType,
      condition: Option[String],
      sourcePlan: Option[LogicalPlan]): SnowflakePlan =
    build(joinStatement(_, _, joinType, condition), left, right, sourcePlan)

  def saveAsTable(tableName: String, mode: SaveMode, child: SnowflakePlan): SnowflakePlan =
    mode match {
      case SaveMode.Append =>
        val createTable = createTableStatement(
          tableName,
          attributeToSchemaString(child.attributes),
          error = false)
        val createTableAndInsert = if (session.tableExists(tableName)) {
          Seq(Query(insertIntoStatement(tableName, child.queries.last.sql)))
        } else {
          Seq(Query(createTable), Query(insertIntoStatement(tableName, child.queries.last.sql)))
        }
        SnowflakePlan(
          child.queries.slice(0, child.queries.length - 1) ++ createTableAndInsert,
          // can't prepare INSERT INTO since table may not exist, use CREATE TABLE instead.
          createTable,
          child.postActions,
          session,
          // source plan is not necessary in action
          None,
          child.supportAsyncMode)
      case SaveMode.Overwrite =>
        build(createTableAsSelectStatement(tableName, _, replace = true), child, None)
      case SaveMode.Ignore =>
        build(createTableAsSelectStatement(tableName, _, error = false), child, None)
      case SaveMode.ErrorIfExists =>
        build(createTableAsSelectStatement(tableName, _), child, None)
    }

  def copyIntoLocation(
      stagedFileWriter: StagedFileWriter,
      child: SnowflakePlan): SnowflakePlan = {
    val selectChild = addResultScanIfNotSelect(child)
    val copy = stagedFileWriter.getCopyIntoLocationQuery(selectChild.queries.last.sql)
    val newQueries = selectChild.queries.slice(0, selectChild.queries.length - 1) ++ Seq(
      Query(copy))
    SnowflakePlan(
      newQueries,
      copy,
      selectChild.postActions,
      session,
      // source plan is not necessary in action
      None,
      selectChild.supportAsyncMode)
  }

  def limitOnSort(
      child: SnowflakePlan,
      limitExpr: String,
      order: Seq[String],
      sourcePlan: Option[LogicalPlan]): SnowflakePlan =
    build(limitOnSortStatement(_, limitExpr, order), child, sourcePlan)

  def limit(
      limitExpr: String,
      child: SnowflakePlan,
      sourcePlan: Option[LogicalPlan]): SnowflakePlan =
    build(limitStatement(limitExpr, _), child, sourcePlan)

  def offset(
      offsetExpr: String,
      child: SnowflakePlan,
      sourcePlan: Option[LogicalPlan]): SnowflakePlan =
    build(offsetStatement(offsetExpr, _), child, sourcePlan)

  def pivot(
      pivotColumn: String,
      pivotValues: Seq[String],
      aggregate: String,
      child: SnowflakePlan,
      sourcePlan: Option[LogicalPlan]): SnowflakePlan =
    build(pivotStatement(pivotColumn, pivotValues, aggregate, _), child, sourcePlan)

  def createOrReplaceView(name: String, child: SnowflakePlan, isTemp: Boolean): SnowflakePlan = {
    require(
      child.queries.size == 1,
      "Your dataframe may include DDL or DML operations. " +
        "Creating a view from this DataFrame is currently not supported.")

    // scalastyle:off caselocale
    val plan: SnowflakePlan =
      if (child.queries.head.sql.toLowerCase.trim.startsWith("select")) {
        child
      } else if (child.queries.head.sql.toLowerCase.replaceAll("\\s", "").startsWith("(select")) {
        session.analyzer.resolve(Project.apply(Star(Seq.empty).expressions, child))
      } else {
        throw new IllegalArgumentException(
          "requirement failed: Only support creating view from SELECT queries")
      }
    // scalastyle:on caselocale

    val tempType: TempType = session.getTempType(isTemp, name)
    session.recordTempObjectIfNecessary(TempObjectType.View, name, tempType)

    // source plan is not necessary in action
    build(createOrReplaceViewStatement(name, _, tempType), plan, None)
  }

  def createTempTable(name: String, child: SnowflakePlan): SnowflakePlan = {
    // source plan is not necessary in action
    buildFromMultipleQueries(
      createTableAndInsert(session, name, child.schemaQuery, _),
      child,
      None,
      Some(child.schemaQuery),
      true)
  }

  private def createTableAndInsert(
      session: Session,
      name: String,
      schemaQuery: String,
      query: String): Seq[String] = {
    val attributes = session.conn.getResultAttributes(schemaQuery)
    val tempType: TempType = session.getTempType(isTemp = true, name)
    session.recordTempObjectIfNecessary(TempObjectType.Table, name, tempType)
    val createTable =
      createTableStatement(name, attributeToSchemaString(attributes), tempType = tempType)
    Seq(createTable, insertIntoStatement(name, query))
  }

  def readFile(
      path: String,
      format: String,
      options: Map[String, String], // key should be upper case
      fullyQualifiedSchema: String,
      schema: Seq[Attribute]): SnowflakePlan = {
    val (copyOptions, formatTypeOptions) = options
      .filter {
        case (k, _) => !k.equals("PATTERN")
      }
      .partition {
        case (k, _) => CopyOptionForCopyIntoTable.contains(k)
      }
    val pattern = options.get("PATTERN")
    // track usage of pattern, will refactor this function in future
    if (pattern.nonEmpty) {
      session.conn.telemetry.reportUsageOfCopyPattern()
    }

    if (copyOptions.isEmpty) { // use select
      val tempFileFormatName = fullyQualifiedSchema + "." +
        randomNameForTempObject(TempObjectType.FileFormat)
      val tempType: TempType = session.getTempType(isTemp = true, isNameGenerated = true)
      val queries: Seq[Query] = Seq(
        Query(
          createFileFormatStatement(
            tempFileFormatName,
            format,
            formatTypeOptions,
            tempType,
            ifNotExist = true),
          true),
        Query(
          selectFromPathWithFormatStatement(
            schemaCastSeq(schema),
            path,
            Some(tempFileFormatName),
            pattern)))
      session.recordTempObjectIfNecessary(TempObjectType.FileFormat, tempFileFormatName, tempType)
      val postActions = Seq(Query(dropFileFormatIfExistStatement(tempFileFormatName), true))
      SnowflakePlan(
        queries,
        schemaValueStatement(schema),
        postActions,
        session,
        None,
        supportAsyncMode = true)
    } else { // otherwise, use COPY
      val tempTableName = fullyQualifiedSchema + "." + randomNameForTempObject(
        TempObjectType.Table)

      val tempTableSchema =
        schema.zipWithIndex.map {
          case (att, index) => Attribute(s""""COL$index"""", att.dataType, att.nullable)
        }
      val tempType: TempType = session.getTempType(isTemp = true, isNameGenerated = true)
      val queries: Seq[Query] = Seq(
        Query(
          createTableStatement(
            tempTableName,
            attributeToSchemaString(tempTableSchema),
            tempType = tempType),
          true),
        Query(
          copyIntoTable(
            tempTableName,
            path,
            format,
            formatTypeOptions,
            copyOptions,
            pattern,
            Seq.empty,
            Seq.empty)),
        Query(projectStatement(tempTableSchema.zip(schema).map {
          case (newAtt, inputAtt) => s"${newAtt.name} AS ${inputAtt.name}"
        }, tempTableName))) // rename col1 to $1
      // In the post action we dropped this temp table. Still adding this to the deletion list to
      // be safe in deletion. We rely on 15 digits alphabetic-numeric random string in naming temp
      // objects on not shadowing user's permanent objects.
      session.recordTempObjectIfNecessary(TempObjectType.Table, tempTableName, tempType)
      val postActions = Seq(Query(dropTableIfExistsStatement(tempTableName), true))
      SnowflakePlan(
        queries,
        schemaValueStatement(schema),
        postActions,
        session,
        None,
        supportAsyncMode = true)
    }
  }

  def copyInto(
      tableName: String,
      path: String,
      format: String,
      options: Map[String, String], // key should be upper case
      fullyQualifiedSchema: String,
      columnNames: Seq[String],
      transformations: Seq[String],
      userSchema: Option[StructType]): SnowflakePlan = {
    val (copyOptions, formatTypeOptions) = options
      .filter {
        case (k, _) => !k.equals("PATTERN")
      }
      .partition {
        case (k, _) => CopyOptionForCopyIntoTable.contains(k)
      }
    val pattern = options.get("PATTERN")
    // track usage of pattern, will refactor this function in future
    if (pattern.nonEmpty) {
      session.conn.telemetry.reportUsageOfCopyPattern()
    }

    val copyCommand = copyIntoTable(
      tableName,
      path,
      format,
      formatTypeOptions,
      copyOptions,
      pattern,
      columnNames,
      transformations)

    val queries = if (session.tableExists(tableName)) {
      Seq(Query(copyCommand))
    } else if (userSchema.nonEmpty && transformations.isEmpty) {
      // If target table doesn't exist,
      // Generate CREATE TABLE command from user input schema.
      val attributes = userSchema.get.toAttributes
      Seq(
        Query(
          createTableStatement(tableName, attributeToSchemaString(attributes), false, false),
          true),
        Query(copyCommand))
    } else {
      throw ErrorMessage.DF_COPY_INTO_CANNOT_CREATE_TABLE(tableName)
    }

    SnowflakePlan(queries, copyCommand, Seq.empty, session, None, true)
  }

  def lateral(
      tableFunction: String,
      child: SnowflakePlan,
      sourcePlan: Option[LogicalPlan]): SnowflakePlan =
    build(lateralStatement(tableFunction, _), child, sourcePlan)

  def fromTableFunction(func: String): SnowflakePlan =
    query(tableFunctionStatement(func), None)

  def fromStoredProcedure(spName: String, args: Seq[String]): SnowflakePlan =
    query(storedProcedureStatement(spName, args), None)

  def joinTableFunction(
      func: String,
      child: SnowflakePlan,
      over: Option[String],
      sourcePlan: Option[LogicalPlan]): SnowflakePlan = {
    build(joinTableFunctionStatement(func, _, over), child, sourcePlan)
  }

  // transform a plan to use result scan if it contains non select query
  private def addResultScanIfNotSelect(plan: SnowflakePlan): SnowflakePlan = {
    plan.sourcePlan match {
      case Some(_: SetOperation) => plan
      case Some(_: MultiChildrenNode) => plan
      // scalastyle:off
      case _ if plan.queries.last.sql.trim.toLowerCase.startsWith("select") => plan
      // scalastyle:on
      case _ =>
        val newQueries = plan.queries :+ Query(
          resultScanStatement(plan.queries.last.queryIdPlaceHolder))
        // Query with result_scan cannot be executed in async mode
        SnowflakePlan(
          newQueries,
          schemaValueStatement(plan.attributes),
          plan.postActions,
          session,
          plan.sourcePlan,
          supportAsyncMode = false)
    }
  }
}

/**
 * Assign a place holder for all queries. replace this place holder by real
 * uuid if necessary.
 * for example, a query list
 * 1. show tables , "query_id_place_holder_XXXX"
 * 2. select * from table(result_scan('query_id_place_holder_XXXX')) , "query_id_place_holder_YYYY"
 * when executing
 * 1, execute query 1, and get read uuid, such as 1234567
 * 2, replace uuid_place_holder_XXXXX by 1234567 in query 2, and execute it
 */
private[snowpark] class Query(
    val sql: String,
    val queryIdPlaceHolder: String,
    val isDDLOnTempObject: Boolean,
    val params: Seq[Any])
    extends Logging {
  logDebug(s"Creating a new Query: $sql ID: $queryIdPlaceHolder")
  override def toString: String = sql
  def runQuery(
      conn: ServerConnection,
      placeholders: mutable.HashMap[String, String],
      statementParameters: Map[String, Any] = Map.empty): String = {
    var finalQuery = sql
    placeholders.foreach {
      case (holder, id) => finalQuery = finalQuery.replaceAll(holder, id)
    }
    val queryId = conn.runQuery(finalQuery, isDDLOnTempObject, statementParameters, params)
    placeholders += (queryIdPlaceHolder -> queryId)
    queryId
  }

  def runQueryGetResult(
      conn: ServerConnection,
      placeholders: mutable.HashMap[String, String],
      returnIterator: Boolean,
      statementParameters: Map[String, Any] = Map.empty): QueryResult = {
    var finalQuery = sql
    placeholders.foreach {
      case (holder, id) => finalQuery = finalQuery.replaceAll(holder, id)
    }
    val result =
      conn.runQueryGetResult(
        finalQuery,
        !returnIterator,
        returnIterator,
        conn.getStatementParameters(isDDLOnTempObject, statementParameters),
        params)
    placeholders += (queryIdPlaceHolder -> result.queryId)
    result
  }
}

private[snowpark] class BatchInsertQuery(
    override val sql: String,
    override val queryIdPlaceHolder: String,
    attributes: Seq[Attribute],
    rows: Seq[Row])
    extends Query(sql, queryIdPlaceHolder, false, Seq.empty) {
  override def runQuery(
      conn: ServerConnection,
      placeholders: mutable.HashMap[String, String],
      statementParameters: Map[String, Any] = Map.empty): String = {
    conn.runBatchInsert(
      sql,
      attributes,
      rows,
      conn.getStatementParameters(false, statementParameters))
  }

  override def runQueryGetResult(
      conn: ServerConnection,
      placeholders: mutable.HashMap[String, String],
      returnIterator: Boolean,
      statementParameters: Map[String, Any] = Map.empty): QueryResult = {
    throw ErrorMessage.PLAN_LAST_QUERY_RETURN_RESULTSET()
  }
}

object Query {
  private def placeHolder(): String =
    s"query_id_place_holder_${Random.alphanumeric.take(10).mkString}"

  def apply(
      sql: String,
      isDDLOnTempObject: Boolean = false,
      params: Seq[Any] = Seq.empty): Query = {
    new Query(sql, placeHolder(), isDDLOnTempObject, params)
  }

  def apply(sql: String, attributes: Seq[Attribute], rows: Seq[Row]): Query = {
    new BatchInsertQuery(sql, placeHolder(), attributes, rows)
  }

  def resultScanQuery(queryID: String): Query = Query(resultScanStatement(queryID))
}
