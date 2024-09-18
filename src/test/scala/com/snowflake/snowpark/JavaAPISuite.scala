package com.snowflake.snowpark

import org.scalatest.FunSuite
import java.io.ByteArrayOutputStream

@JavaAPITest
class JavaAPISuite extends FunSuite {
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
