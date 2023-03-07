package com.snowflake.snowpark

import com.snowflake.snowpark.internal.ScalaFunctions
import com.snowflake.snowpark.types._
import com.snowflake.snowpark.udtf._

class UDTFInternalSuite extends SNTestBase {

  val udtfHandler = session.udtf.handler

  test("Unit test for UDTF0") {
    class TestUDTF0 extends UDTF0 {
      override def process(): Iterable[Row] = throw new Exception("test")
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("a", StringType))
    }

    val udtf0 = new UDTF0 {
      override def process(): Iterable[Row] = throw new Exception("test")
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("a", StringType))
    }

    ScalaFunctions.checkSupportedUdtf(udtf0)
    assert(
      udtfHandler
        .generateUDTFClassSignature(udtf0, udtf0.inputColumns)
        .equals("com.snowflake.snowpark.udtf.UDTF0"))
    val udtf00 = new TestUDTF0()
    assert(
      udtfHandler
        .generateUDTFClassSignature(udtf00, udtf00.inputColumns)
        .equals("com.snowflake.snowpark.udtf.UDTF0"))
  }

  test("Unit test for UDTF1") {
    class TestUDTF1 extends UDTF1[String] {
      override def process(arg0: String): Iterable[Row] = throw new Exception("test")
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("a", StringType))
    }

    val udtf1 = new UDTF1[Int] {
      override def process(arg0: Int): Iterable[Row] = throw new Exception("test")
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("a", StringType))
    }
    ScalaFunctions.checkSupportedUdtf(udtf1)
    assert(
      udtfHandler
        .generateUDTFClassSignature(udtf1, udtf1.inputColumns)
        .equals("com.snowflake.snowpark.udtf.UDTF1<java.lang.Integer>"))
    val udtf11 = new TestUDTF1()
    assert(
      udtfHandler
        .generateUDTFClassSignature(udtf11, udtf11.inputColumns)
        .equals("com.snowflake.snowpark.udtf.UDTF1<java.lang.String>"))

  }

  test("unit test for UDTF2") {
    class TestUDTF2 extends UDTF2[String, String] {
      override def process(arg0: String, arg1: String): Iterable[Row] =
        throw new Exception("test")
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("a", StringType))
    }

    val udtf2 = new UDTF2[Int, Int] {
      override def process(arg0: Int, arg1: Int): Iterable[Row] = throw new Exception("test")
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("a", StringType))
    }
    ScalaFunctions.checkSupportedUdtf(udtf2)
    assert(
      udtfHandler
        .generateUDTFClassSignature(udtf2, udtf2.inputColumns)
        .equals("com.snowflake.snowpark.udtf.UDTF2<java.lang.Integer, java.lang.Integer>"))
    val udtf22 = new TestUDTF2()
    assert(
      udtfHandler
        .generateUDTFClassSignature(udtf22, udtf22.inputColumns)
        .equals("com.snowflake.snowpark.udtf.UDTF2<java.lang.String, java.lang.String>"))
  }

  test("negative test: Unsupported type is used") {
    val invalidTypeUdtf = new UDTF1[TestUnsupportedType] {
      override def process(arg0: TestUnsupportedType): Iterable[Row] = throw new Exception("test")
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("a", StringType))
    }

    val ex = intercept[UnsupportedOperationException] {
      invalidTypeUdtf.inputColumns
    }
    assert(ex.getMessage.startsWith("Unsupported type"))
  }

  test("negative test: Unsupported output column name are used") {
    val invalidNameUdtf = new UDTF1[String] {
      override def process(arg0: String): Iterable[Row] = throw new Exception("test")
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType =
        StructType(StructField("invalid name", StringType))
    }

    val ex = intercept[SnowparkClientException] {
      ScalaFunctions.checkSupportedUdtf(invalidNameUdtf)
    }
    assert(ex.errorCode.equals("0208"))
  }
}

class TestUnsupportedType
