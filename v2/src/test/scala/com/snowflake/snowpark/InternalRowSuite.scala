package com.snowflake.snowpark

import com.snowflake.snowpark.types._

class InternalRowSuite extends UnitTestBase {

  test("from seq with schema") {
    val seq = Seq(1, true, "aaa")
    val schema = StructType.apply(
      Seq(
        StructField("num", IntegerType),
        StructField("bool", BooleanType),
        StructField("str", StringType)))
    val row = Row.fromSeqWithSchema(seq, Some(schema))

    assert(row.getInt(row.fieldIndex("num")) == 1)
    assert(row.getBoolean(row.fieldIndex("bool")))
    assert(row.getString(row.fieldIndex("str")) == "aaa")

    assert(
      intercept[UnsupportedOperationException](Row.apply(1, 2, 3).fieldIndex("dummy")).getMessage
        .contains("Cannot get field index for row without schema"))
  }
}
