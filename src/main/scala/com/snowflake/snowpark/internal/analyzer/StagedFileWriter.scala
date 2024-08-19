package com.snowflake.snowpark.internal.analyzer

import com.snowflake.snowpark.internal.analyzer.SnowflakePlan.{
  CopyOptionsForCopyIntoLocation,
  CopySubClausesForCopyIntoLocation,
  FormatTypeOptionsForCopyIntoLocation
}

import java.util.Locale
import com.snowflake.snowpark.{DataFrameWriter, SaveMode}
import com.snowflake.snowpark.internal.{ErrorMessage, Logging, Utils}

import scala.collection.mutable

private[snowpark] class StagedFileWriter(val dataframeWriter: DataFrameWriter) extends Logging {

  private val copyOptions = mutable.Map[String, String]()
  private val formatTypeOptions = mutable.Map[String, String]()
  private val copySubClauses = mutable.Map[String, String]()
  var saveMode: SaveMode = SaveMode.ErrorIfExists
  var stageLocation: String = ""
  var formatType: String = ""

  def option(key: String, value: Any): StagedFileWriter = {
    val upperKey = key.toUpperCase(Locale.ROOT)
    if (CopyOptionsForCopyIntoLocation.contains(upperKey)) {
      copyOptions += (upperKey -> Utils.quoteForOption(value))
    } else if (FormatTypeOptionsForCopyIntoLocation.contains(upperKey)) {
      formatTypeOptions += (upperKey -> Utils.quoteForOption(value))
    } else if ("PARTITION BY".equals(upperKey)) {
      // Doesn't add single quote for PARTITION BY because it is not a String
      copySubClauses += (upperKey -> s"$value")
    } else if (CopySubClausesForCopyIntoLocation.contains(upperKey)) {
      copySubClauses += (upperKey -> Utils.quoteForOption(value))
    } else {
      throw ErrorMessage.DF_WRITER_INVALID_OPTION_NAME(key, "file")
    }

    this
  }

  def options(options: Map[String, Any]): StagedFileWriter = {
    options.foreach(e => option(e._1, e._2))
    this
  }

  def mode(saveMode: SaveMode): StagedFileWriter = {
    saveMode match {
      case SaveMode.ErrorIfExists => this.saveMode = saveMode
      case SaveMode.Overwrite     => this.saveMode = saveMode
      case _ => throw ErrorMessage.DF_WRITER_INVALID_MODE(saveMode.toString, "file")
    }
    this
  }

  def path(path: String): StagedFileWriter = {
    stageLocation = path
    this
  }

  def format(format: String): StagedFileWriter = {
    formatType = format
    this
  }

  private def getPartitionByClause(): String =
    if (copySubClauses.keySet.contains("PARTITION BY")) {
      s"PARTITION BY ${copySubClauses("PARTITION BY")}\n"
    } else {
      ""
    }

  private def getHeaderClause(): String =
    if (copySubClauses.keySet.contains("HEADER") && copySubClauses("HEADER").toBoolean) {
      s"\n HEADER = TRUE"
    } else {
      ""
    }

  private def getFileFormatClause(formatType: String): String = {
    val formatPrefix = if (formatTypeOptions.keySet.contains("FORMAT_NAME")) {
      s"FORMAT_NAME = ${formatTypeOptions("FORMAT_NAME")}"
    } else {
      s"TYPE = $formatType"
    }
    val formatOptions = formatTypeOptions
      .filter(!_._1.equals("FORMAT_NAME"))
      .map(x => s"${x._1} = ${x._2}")
      .mkString(" ")
    s" FILE_FORMAT = ( $formatPrefix $formatOptions )"
  }

  private def getCopyOptionClause(): String = {
    val adjustCopyOptions = saveMode match {
      case SaveMode.ErrorIfExists => copyOptions + ("OVERWRITE" -> "FALSE")
      case SaveMode.Overwrite     => copyOptions + ("OVERWRITE" -> "TRUE")
    }
    val copyOptionsClause = adjustCopyOptions.map(x => s"${x._1} = ${x._2}").mkString(" ")
    copyOptionsClause
  }

  def getCopyIntoLocationQuery(query: String): String = {
    // COPY INTO { internalStage | externalStage | externalLocation }
    //     FROM { [<namespace>.]<table_name> | ( <query> ) }
    // [ PARTITION BY <expr> ]
    // [ FILE_FORMAT = ( { FORMAT_NAME = '[<namespace>.]<file_format_name>' |
    //                    TYPE = { CSV | JSON | PARQUET } [ formatTypeOptions ] } ) ]
    // [ copyOptions ]
    // [ HEADER ]
    s"""COPY INTO $stageLocation
         | FROM ( $query )
         | ${getPartitionByClause()}${getFileFormatClause(formatType)}
         | ${getCopyOptionClause()}${getHeaderClause()}""".stripMargin
  }
}
