package com.snowflake.snowpark.internal.analyzer

import java.util.Locale

import com.snowflake.snowpark.Session
import com.snowflake.snowpark.internal.{ErrorMessage, Logging, Utils}
import com.snowflake.snowpark.types.{StructType, VariantType}

private[snowpark] class StagedFileReader(
    val session: Session,
    var curOptions: Map[String, String],
    var stageLocation: String,
    var formatType: String,
    var fullyQualifiedSchema: String,
    var userSchema: Option[StructType],
    var tableName: Option[String],
    var columnNames: Seq[String],
    var transformations: Seq[Expression])
    extends Logging {

  def this(session: Session) = {
    this(session, Map.empty, "", "CSV", "", None, None, Seq.empty, Seq.empty)
  }

  def this(stagedFileReader: StagedFileReader) = {
    this(
      stagedFileReader.session,
      stagedFileReader.curOptions,
      stagedFileReader.stageLocation,
      stagedFileReader.formatType,
      stagedFileReader.fullyQualifiedSchema,
      stagedFileReader.userSchema,
      stagedFileReader.tableName,
      stagedFileReader.columnNames,
      stagedFileReader.transformations)
  }

  private final val supportedFileTypes = Set("CSV", "JSON", "PARQUET", "AVRO", "ORC", "XML")

  def path(stageLocation: String): StagedFileReader = {
    this.stageLocation = stageLocation
    this
  }

  def format(formatType: String): StagedFileReader = {
    val upperFormatType = formatType.toUpperCase(Locale.ROOT)
    if (supportedFileTypes.contains(upperFormatType)) {
      this.formatType = upperFormatType
    } else {
      throw ErrorMessage.PLAN_UNSUPPORTED_FILE_FORMAT_TYPE(formatType)
    }
    this
  }

  def option(key: String, value: Any): StagedFileReader = {
    // upper case to deduplicate
    curOptions += (key.toUpperCase(Locale.ROOT) -> Utils.quoteForOption(value))
    this
  }

  def options(configs: Map[String, Any]): StagedFileReader = {
    configs.foreach { case (k, v) =>
      option(k, v)
    }
    this
  }

  def databaseSchema(fullyQualifiedSchema: String): StagedFileReader = {
    this.fullyQualifiedSchema = fullyQualifiedSchema
    this
  }

  def userSchema(schema: StructType): StagedFileReader = {
    this.userSchema = Some(schema)
    this
  }

  def table(tableName: String): StagedFileReader = {
    this.tableName = Some(tableName)
    this
  }

  def columnNames(columnNames: Seq[String]): StagedFileReader = {
    this.columnNames = columnNames
    this
  }

  def transformations(transformations: Seq[Expression]): StagedFileReader = {
    this.transformations = transformations
    this
  }

  def createSnowflakePlan(): SnowflakePlan = {
    if (tableName.nonEmpty) {
      // copy into target table
      session.plans.copyInto(
        tableName.get,
        stageLocation,
        formatType,
        curOptions,
        fullyQualifiedSchema,
        columnNames,
        transformations.map(SqlGenerator.expressionToSql),
        userSchema)
    } else if (formatType.equals("CSV")) {
      if (userSchema.isEmpty) {
        throw ErrorMessage.DF_MUST_PROVIDE_SCHEMA_FOR_READING_FILE()
      } else {
        session.plans.readFile(
          stageLocation,
          formatType,
          curOptions,
          fullyQualifiedSchema,
          userSchema.get.toAttributes)
      }
    } else {
      require(userSchema.isEmpty, s"Read $formatType does not support user schema")
      session.plans.readFile(
        stageLocation,
        formatType,
        curOptions,
        fullyQualifiedSchema,
        Seq(Attribute("\"$1\"", VariantType)))
    }
  }

}
