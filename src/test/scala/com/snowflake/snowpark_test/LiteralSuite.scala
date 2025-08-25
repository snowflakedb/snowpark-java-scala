package com.snowflake.snowpark_test

import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.TestData

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import java.util.TimeZone

class LiteralSuite extends TestData {

  test("Literal scala basic types") {
    val df = session
      .range(2)
      .withColumn("null", lit(null))
      .withColumn("str", lit("string"))
      .withColumn("char", lit('C'))
      .withColumn("bool", lit(true))
      .withColumn("byte", lit(10.toByte))
      .withColumn("short", lit(11.toShort))
      .withColumn("int", lit(12))
      .withColumn("long", lit(13.toLong))
      .withColumn("float", lit(14.toFloat))
      .withColumn("double", lit(15.toDouble))

    assert(
      getSchemaString(df.schema) ==
        """root
          | |--ID: Long (nullable = false)
          | |--NULL: String (nullable = true)
          | |--STR: String (nullable = false)
          | |--CHAR: String (nullable = false)
          | |--BOOL: Boolean (nullable = true)
          | |--BYTE: Long (nullable = false)
          | |--SHORT: Long (nullable = false)
          | |--INT: Long (nullable = false)
          | |--LONG: Long (nullable = false)
          | |--FLOAT: Double (nullable = false)
          | |--DOUBLE: Double (nullable = false)
          |""".stripMargin)
    df.show()

    // scalastyle:off
    assert(
      getShowString(df, 10) ==
        """-----------------------------------------------------------------------------------------------------
          ||"ID"  |"NULL"  |"STR"   |"CHAR"  |"BOOL"  |"BYTE"  |"SHORT"  |"INT"  |"LONG"  |"FLOAT"  |"DOUBLE"  |
          |-----------------------------------------------------------------------------------------------------
          ||0     |NULL    |string  |C       |true    |10      |11       |12     |13      |14.0     |15.0      |
          ||1     |NULL    |string  |C       |true    |10      |11       |12     |13      |14.0     |15.0      |
          |-----------------------------------------------------------------------------------------------------
          |""".stripMargin)
    // scalastyle:on
  }

  test("Literal java primitive types") {
    val df = session
      .range(2)
      .withColumn("null", lit(null))
      .withColumn("str", lit(new java.lang.String("string")))
      .withColumn("char", lit(java.lang.Character.valueOf('C').charValue()))
      .withColumn("bool", lit(java.lang.Boolean.TRUE.booleanValue()))
      .withColumn("byte", lit(java.lang.Byte.valueOf(10.toByte).byteValue()))
      .withColumn("short", lit(java.lang.Short.valueOf("11").shortValue()))
      .withColumn("int", lit(java.lang.Integer.valueOf(12).intValue()))
      .withColumn("long", lit(java.lang.Long.valueOf(13).longValue()))
      .withColumn("float", lit(java.lang.Float.valueOf(14).floatValue()))
      .withColumn("double", lit(java.lang.Double.valueOf(15).doubleValue()))

    assert(
      getSchemaString(df.schema) ==
        """root
          | |--ID: Long (nullable = false)
          | |--NULL: String (nullable = true)
          | |--STR: String (nullable = false)
          | |--CHAR: String (nullable = false)
          | |--BOOL: Boolean (nullable = true)
          | |--BYTE: Long (nullable = false)
          | |--SHORT: Long (nullable = false)
          | |--INT: Long (nullable = false)
          | |--LONG: Long (nullable = false)
          | |--FLOAT: Double (nullable = false)
          | |--DOUBLE: Double (nullable = false)
          |""".stripMargin)

    // scalastyle:off
    assert(
      getShowString(df, 10) ==
        """-----------------------------------------------------------------------------------------------------
          ||"ID"  |"NULL"  |"STR"   |"CHAR"  |"BOOL"  |"BYTE"  |"SHORT"  |"INT"  |"LONG"  |"FLOAT"  |"DOUBLE"  |
          |-----------------------------------------------------------------------------------------------------
          ||0     |NULL    |string  |C       |true    |10      |11       |12     |13      |14.0     |15.0      |
          ||1     |NULL    |string  |C       |true    |10      |11       |12     |13      |14.0     |15.0      |
          |-----------------------------------------------------------------------------------------------------
          |""".stripMargin)
    // scalastyle:on
  }

  test("Literal java basic types") {
    val df = session
      .range(2)
      .withColumn("null", lit(null))
      .withColumn("str", lit(new java.lang.String("string")))
      .withColumn("char", lit(java.lang.Character.valueOf('C')))
      .withColumn("bool", lit(java.lang.Boolean.TRUE))
      .withColumn("byte", lit(java.lang.Byte.valueOf(10.toByte)))
      .withColumn("short", lit(java.lang.Short.valueOf("11")))
      .withColumn("int", lit(java.lang.Integer.valueOf(12)))
      .withColumn("long", lit(java.lang.Long.valueOf(13)))
      .withColumn("float", lit(java.lang.Float.valueOf(14)))
      .withColumn("double", lit(java.lang.Double.valueOf(15)))

    assert(
      getSchemaString(df.schema) ==
        """root
          | |--ID: Long (nullable = false)
          | |--NULL: String (nullable = true)
          | |--STR: String (nullable = false)
          | |--CHAR: String (nullable = false)
          | |--BOOL: Boolean (nullable = true)
          | |--BYTE: Long (nullable = false)
          | |--SHORT: Long (nullable = false)
          | |--INT: Long (nullable = false)
          | |--LONG: Long (nullable = false)
          | |--FLOAT: Double (nullable = false)
          | |--DOUBLE: Double (nullable = false)
          |""".stripMargin)

    // scalastyle:off
    assert(
      getShowString(df, 10) ==
        """-----------------------------------------------------------------------------------------------------
          ||"ID"  |"NULL"  |"STR"   |"CHAR"  |"BOOL"  |"BYTE"  |"SHORT"  |"INT"  |"LONG"  |"FLOAT"  |"DOUBLE"  |
          |-----------------------------------------------------------------------------------------------------
          ||0     |NULL    |string  |C       |true    |10      |11       |12     |13      |14.0     |15.0      |
          ||1     |NULL    |string  |C       |true    |10      |11       |12     |13      |14.0     |15.0      |
          |-----------------------------------------------------------------------------------------------------
          |""".stripMargin)
    // scalastyle:on
  }

  test("Literal scala & java binary type") {
    val df = session
      .range(2)
      .withColumn("scala", lit(Array('a'.toByte, 'b'.toByte, 'c'.toByte)))
      .withColumn("java", lit(new java.lang.String("efg").getBytes()))

    assert(
      getSchemaString(df.schema) ==
        """root
          | |--ID: Long (nullable = false)
          | |--SCALA: Binary (nullable = false)
          | |--JAVA: Binary (nullable = false)
          |""".stripMargin)

    assert(
      getShowString(df, 10) ==
        """------------------------------
          ||"ID"  |"SCALA"   |"JAVA"    |
          |------------------------------
          ||0     |'616263'  |'656667'  |
          ||1     |'616263'  |'656667'  |
          |------------------------------
          |""".stripMargin)
  }

  test("Literal TimeStamp and Instant") {
    testWithTimezone() {
      val instant = LocalDateTime
        .of(2020, 10, 11, 12, 13, 14, 123000000)
        .toInstant(ZoneOffset.UTC)
      val df = session
        .range(2)
        .withColumn("timestamp", lit(Timestamp.valueOf("2018-10-11 12:13:14.123")))
        .withColumn("instant", lit(instant))

      assert(
        getSchemaString(df.schema) ==
          """root
            | |--ID: Long (nullable = false)
            | |--TIMESTAMP: Timestamp (nullable = false)
            | |--INSTANT: Timestamp (nullable = false)
            |""".stripMargin)

      assert(
        getShowString(df, 10) ==
          """------------------------------------------------------------
            ||"ID"  |"TIMESTAMP"              |"INSTANT"                |
            |------------------------------------------------------------
            ||0     |2018-10-11 12:13:14.123  |2020-10-11 05:13:14.123  |
            ||1     |2018-10-11 12:13:14.123  |2020-10-11 05:13:14.123  |
            |------------------------------------------------------------
            |""".stripMargin)
    }
  }

  test("Literal Date and LocalDate") {
    testWithTimezone() {
      val df = session
        .range(2)
        .withColumn("local_date", lit(LocalDate.of(2020, 10, 11)))
        .withColumn("date", lit(Date.valueOf("2018-10-11")))

      assert(
        getSchemaString(df.schema) ==
          """root
            | |--ID: Long (nullable = false)
            | |--LOCAL_DATE: Date (nullable = false)
            | |--DATE: Date (nullable = false)
            |""".stripMargin)

      assert(
        getShowString(df, 10) ==
          """------------------------------------
            ||"ID"  |"LOCAL_DATE"  |"DATE"      |
            |------------------------------------
            ||0     |2020-10-11    |2018-10-11  |
            ||1     |2020-10-11    |2018-10-11  |
            |------------------------------------
            |""".stripMargin)
    }
  }

  test("special literals") {
    val sourceLiteral = lit(123)

    val df = session
      .range(2)
      .withColumn("null", lit(null))
      .withColumn("literal", lit(sourceLiteral))

    assert(
      getSchemaString(df.schema) ==
        """root
          | |--ID: Long (nullable = false)
          | |--NULL: String (nullable = true)
          | |--LITERAL: Long (nullable = false)
          |""".stripMargin)

    assert(
      getShowString(df, 10) ==
        """-----------------------------
          ||"ID"  |"NULL"  |"LITERAL"  |
          |-----------------------------
          ||0     |NULL    |123        |
          ||1     |NULL    |123        |
          |-----------------------------
          |""".stripMargin)
  }
}
