package com.snowflake.snowpark

import com.snowflake.snowpark.internal.analyzer.Attribute
import com.snowflake.snowpark.types._

class ResultAttributesSuite extends SNTestBase {

  val tableName: String = randomName()

  override def afterAll(): Unit = {
    dropTable(tableName)
    super.afterAll()
  }

  private def getTableAttributes(name: String): Seq[Attribute] =
    session.getResultAttributes(s"select * from $name")

  private def getAttributesWithTypes(name: String, types: Seq[String]): Seq[Attribute] = {
    var attribute: Seq[Attribute] = Seq.empty
    try {
      createTable(
        tableName,
        types.zipWithIndex
          .map { case (tpe, index) =>
            s"col_$index $tpe"
          }
          .mkString(","))
      attribute = getTableAttributes(tableName)
    } finally {
      dropTable(name)
    }
    attribute
  }

  // Snowflake Data Type Doc
  // https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html

  test("integer data type") {
    val integers = Seq("number", "decimal", "numeric", "bigint", "int", "integer", "smallint")
    val attribute = getAttributesWithTypes(tableName, integers)
    assert(attribute.length == integers.length)
    integers.indices.foreach(index => assert(attribute(index).dataType == LongType))
  }

  test("float data type") {
    val floats = Seq("float", "float4", "double", "real")
    val attribute = getAttributesWithTypes(tableName, floats)
    assert(attribute.length == floats.length)
    floats.indices.foreach(index => assert(attribute(index).dataType == DoubleType))
  }

  test("string data types") {
    val strings = Seq("varchar", "char", "character", "string", "text")
    val attribute = getAttributesWithTypes(tableName, strings)
    assert(attribute.length == strings.length)
    strings.indices.foreach(index => assert(attribute(index).dataType == StringType))
  }

  test("binary data types") {
    val binaries = Seq("binary", "varbinary")
    val attribute = getAttributesWithTypes(tableName, binaries)
    assert(attribute.length == binaries.length)
    binaries.indices.foreach(index => assert(attribute(index).dataType == BinaryType))
  }

  test("logical data type") {
    createTable(tableName, "bool boolean")
    val attributes = getTableAttributes(tableName)
    assert(attributes.length == 1)
    assert(attributes.head.dataType == BooleanType)
    dropTable(tableName)
  }

  test("date & time data type") {
    val dates = Seq(
      "date" -> DateType,
      "datetime" -> TimestampType,
      "time" -> TimeType,
      "timestamp" -> TimestampType,
      "timestamp_ltz" -> TimestampType,
      "timestamp_ntz" -> TimestampType,
      "timestamp_tz" -> TimestampType)
    val attribute = getAttributesWithTypes(tableName, dates.map(_._1))
    assert(attribute.length == dates.length)
    dates.indices.foreach(index => assert(attribute(index).dataType == dates(index)._2))
  }

  test("semi-structured data types") {
    val variants = Seq("variant", "object")
    val attribute = getAttributesWithTypes(tableName, variants)
    assert(attribute.length == variants.length)

    assert(
      attribute(0).dataType ==
        VariantType)
    assert(
      attribute(1).dataType ==
        MapType(StringType, StringType))
  }

  test("Array Type") {
    val variants = Seq("array")
    val attribute = getAttributesWithTypes(tableName, variants)
    assert(attribute.length == variants.length)
    variants.indices.foreach(index =>
      assert(
        attribute(index).dataType ==
          ArrayType(StringType)))
  }
}
