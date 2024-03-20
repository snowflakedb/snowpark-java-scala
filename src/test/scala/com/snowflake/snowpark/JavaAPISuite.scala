package com.snowflake.snowpark

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import com.snowflake.snowpark_test._
import java.io.ByteArrayOutputStream

// entry of all Java JUnit test suite
// modify this class each time when adding a new Java test suite.
@JavaAPITest
class JavaAPISuite extends FunSuite {
  test("Java Session") {
    TestRunner.run(classOf[JavaSessionSuite])
  }

  test("Java Session without stored proc support") {
    TestRunner.run(classOf[JavaSessionNonStoredProcSuite])
  }

  test("Java Variant") {
    TestRunner.run(classOf[JavaVariantSuite])
  }

  test("Java Geography") {
    TestRunner.run(classOf[JavaGeographySuite])
  }

  test("Java Row") {
    TestRunner.run(classOf[JavaRowSuite])
  }

  test("Java DataType") {
    TestRunner.run(classOf[JavaDataTypesSuite])
  }

  test("Java Column") {
    TestRunner.run(classOf[JavaColumnSuite])
  }

  test("Java Window") {
    TestRunner.run(classOf[JavaWindowSuite])
  }

  test("Java Functions") {
    TestRunner.run(classOf[JavaFunctionSuite])
  }

  test("Java UDF") {
    TestRunner.run(classOf[JavaUDFSuite])
  }

  test("Java UDF without stored proc support") {
    TestRunner.run(classOf[JavaUDFNonStoredProcSuite])
  }

  test("Java UDTF") {
    TestRunner.run(classOf[JavaUDTFSuite])
  }

  test("Java UDTF without stored proc support") {
    TestRunner.run(classOf[JavaUDTFNonStoredProcSuite])
  }

  test("Java DataFrame") {
    TestRunner.run(classOf[JavaDataFrameSuite])
  }

  test("Java DataFrame which doesn't support stored proc") {
    TestRunner.run(classOf[JavaDataFrameNonStoredProcSuite])
  }

  test("Java DataFrameWriter") {
    TestRunner.run(classOf[JavaDataFrameWriterSuite])
  }

  test("Java DataFrameReader") {
    TestRunner.run(classOf[JavaDataFrameReaderSuite])
  }

  test("Java DataFrame Aggregate") {
    TestRunner.run(classOf[JavaDataFrameAggregateSuite])
  }

  test("Java Internal Utils") {
    TestRunner.run(classOf[JavaUtilsSuite])
  }

  test("Java Updatable") {
    TestRunner.run(classOf[JavaUpdatableSuite])
  }

  test("Java DataFrameNaFunctions") {
    TestRunner.run(classOf[JavaDataFrameNaFunctionsSuite])
  }

  test("Java DataFrameStatFunctions") {
    TestRunner.run(classOf[JavaDataFrameStatFunctionsSuite])
  }

  test("Java TableFunction") {
    TestRunner.run(classOf[JavaTableFunctionSuite])
  }

  test("Java CopyableDataFrame") {
    TestRunner.run(classOf[JavaCopyableDataFrameSuite])
  }

  test("Java AsyncJob") {
    TestRunner.run(classOf[JavaAsyncJobSuite])
  }

  test("Java FileOperation") {
    TestRunner.run(classOf[JavaFileOperationSuite])
  }

  test("Java StoredProcedure") {
    TestRunner.run(classOf[JavaStoredProcedureSuite])
  }

  test("Java SProc without stored proc support") {
    TestRunner.run(classOf[JavaSProcNonStoredProcSuite])
  }

  // some tests can't be implemented in Java are listed below

  // console redirect doesn't work in Java since run JUnit from Scala
  test("dataframe explain") {
    val session = com.snowflake.snowpark_java.Session
      .builder()
      .configFile(TestUtils.defaultProfile)
      .create()
    val df = session.sql("select * from values(1, 2),(3, 4) as t(a, b)")
    val outContent = new ByteArrayOutputStream()
    Console.withOut(outContent) {
      df.explain()
    }
    val result = outContent.toString("UTF-8")
    assert(result.contains("Query List:"))
    assert(result.contains("select\n" + "  *\n" + "from\n" + "values(1, 2),(3, 4) as t(a, b)"))
    assert(result.contains("Logical Execution Plan"))
  }

  test("dataframe show") {
    val session = com.snowflake.snowpark_java.Session
      .builder()
      .configFile(TestUtils.defaultProfile)
      .create()
    val df = session.sql("select * from values(1, 2),(3, 4) as t(a, b)")
    val outContent = new ByteArrayOutputStream()
    Console.withOut(outContent) {
      df.show()
    }
    val result = outContent.toString("UTF-8")
    assert(result.contains("""-------------
                             ||"A"  |"B"  |
                             |-------------
                             ||1    |2    |
                             ||3    |4    |
                             |-------------""".stripMargin))

    val outContent1 = new ByteArrayOutputStream()
    Console.withOut(outContent1) {
      df.show(1)
    }
    val result1 = outContent1.toString("UTF-8")
    assert(result1.contains("""-------------
                             ||"A"  |"B"  |
                             |-------------
                             ||1    |2    |
                             |-------------""".stripMargin))

    val df1 = session.sql("select * from values(11111, 22222),(33333, 44444) as t(a, b)")
    df1.show(1, 3)

    val outContent2 = new ByteArrayOutputStream()
    Console.withOut(outContent2) {
      df1.show(1, 3)
    }
    val result2 = outContent2.toString("UTF-8")
    assert(result2.contains("""-------------
                              ||"A"  |"B"  |
                              |-------------
                              ||...  |...  |
                              |-------------""".stripMargin))
  }
}
