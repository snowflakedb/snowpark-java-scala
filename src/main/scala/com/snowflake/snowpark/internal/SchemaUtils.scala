package com.snowflake.snowpark.internal

import com.snowflake.snowpark.internal.analyzer.Attribute
import com.snowflake.snowpark.Session
import com.snowflake.snowpark.types._

import scala.util.Random

/**
 * All functions in this object are temporary solutions.
 */
private[snowpark] object SchemaUtils {

  val CommandAttributes: Seq[Attribute] = Seq(Attribute("\"status\"", StringType))

  val ListStageAttributes: Seq[Attribute] = Seq(
    Attribute("\"name\"", StringType),
    Attribute("\"size\"", LongType),
    Attribute("\"md5\"", StringType),
    Attribute("\"last_modified\"", StringType))

  val RemoveStageFileAttributes: Seq[Attribute] =
    Seq(Attribute("\"name\"", StringType), Attribute("\"result\"", StringType))

  val PutAttributes: Seq[Attribute] = Seq(
    Attribute("\"source\"", StringType, nullable = false),
    Attribute("\"target\"", StringType, nullable = false),
    Attribute("\"source_size\"", DecimalType(10, 0), nullable = false),
    Attribute("\"target_size\"", DecimalType(10, 0), nullable = false),
    Attribute("\"source_compression\"", StringType, nullable = false),
    Attribute("\"target_compression\"", StringType, nullable = false),
    Attribute("\"status\"", StringType, nullable = false),
    Attribute("\"encryption\"", StringType, nullable = false),
    Attribute("\"message\"", StringType, nullable = false))

  val GetAttributes: Seq[Attribute] = Seq(
    Attribute("\"file\"", StringType, nullable = false),
    Attribute("\"size\"", DecimalType(10, 0), nullable = false),
    Attribute("\"status\"", StringType, nullable = false),
    Attribute("\"encryption\"", StringType, nullable = false),
    Attribute("\"message\"", StringType, nullable = false))

  def analyzeAttributes(sql: String, session: Session): Seq[Attribute] = {
    val attributes = session.getResultAttributes(sql)
    if (attributes.nonEmpty) {
      attributes
    } else {
      // scalastyle:off caselocale
      val tokens: Seq[String] = sql.trim.split("\\s").filter(_.nonEmpty).map(_.toLowerCase)
      // scalastyle:on caselocale
      tokens.head match {
        case "alter" => CommandAttributes

        case "ls" | "list" => ListStageAttributes

        case "rm" | "remove" => RemoveStageFileAttributes

        case "put" => PutAttributes

        case "get" => GetAttributes

        case _ => Seq.empty
      }
    }
  }
}
