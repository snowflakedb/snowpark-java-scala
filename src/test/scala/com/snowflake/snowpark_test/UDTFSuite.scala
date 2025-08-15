package com.snowflake.snowpark_test

import java.math.RoundingMode
import com.snowflake.snowpark.TestUtils._
import com.snowflake.snowpark.functions._

import java.nio.file._
import java.sql.{Date, Time, Timestamp}
import java.util.TimeZone
import com.snowflake.snowpark.{Row, _}
import com.snowflake.snowpark.internal._
import com.snowflake.snowpark.types._
import com.snowflake.snowpark.udtf._

import scala.collection.mutable

@UDFTest
class UDTFSuite extends TestData {
  import session.implicits._

  val wordCountTableName = randomTableName()
  val tableName = randomTableName()

  override def beforeAll: Unit = {
    super.beforeAll
    if (!isStoredProc(session)) {
      TestUtils.addDepsToClassPath(session)
    }

    Seq(("w1 w2", "g1"), ("w1 w1 w1", "g2"))
      .toDF("c1", "c2")
      .write
      .saveAsTable(wordCountTableName)
  }

  override def afterAll: Unit = {
    dropTable(wordCountTableName)
    dropTable(tableName)
    super.afterAll
  }

  test("basic UDTF word count without endPartition()") {
    val funcName: String = randomFunctionName()
    try {
      class MyWordCount extends UDTF1[String] {
        override def process(s1: String): Iterable[Row] = {
          val wordCounts = mutable.Map[String, Int]()
          s1.split(" ").foreach { x =>
            wordCounts.update(x, wordCounts.getOrElse(x, 0) + 1)
          }
          wordCounts.toArray.map { p =>
            Row(p._1, p._2)
          }
        }

        override def endPartition(): Iterable[Row] = Seq.empty

        override def outputSchema(): StructType =
          StructType(StructField("word", StringType), StructField("count", IntegerType))
      }

      val tableFunction = session.udtf.registerTemporary(funcName, new MyWordCount())

      // Check output columns name and type
      val df1 = session.tableFunction(tableFunction, lit("w1 w2 w2 w3 w3 w3"))
      assert(
        getSchemaString(df1.schema) ==
          """root
            | |--WORD: String (nullable = true)
            | |--COUNT: Long (nullable = true)
            |""".stripMargin)
      checkAnswer(df1, Seq(Row("w1", 1), Row("w2", 2), Row("w3", 3)))

      // Call the UDTF with funcName and named parameters, result should be the same
      val df2 = session
        .tableFunction(TableFunction(funcName), Map("arg1" -> lit("w1 w2 w2 w3 w3 w3")))
        .select("count", "word")
      assert(
        getSchemaString(df2.schema) ==
          """root
            | |--COUNT: Long (nullable = true)
            | |--WORD: String (nullable = true)
            |""".stripMargin)
      checkAnswer(df2, Seq(Row(1, "w1"), Row(2, "w2"), Row(3, "w3")))

      // Use UDTF with table join
      val df3 = session.sql(
        s"select * from $wordCountTableName, table($funcName(c1) over (partition by c2))")
      assert(
        getSchemaString(df3.schema) ==
          """root
            | |--C1: String (nullable = true)
            | |--C2: String (nullable = true)
            | |--WORD: String (nullable = true)
            | |--COUNT: Long (nullable = true)
            |""".stripMargin)
      checkAnswer(
        df3,
        Seq(
          Row("w1 w2", "g1", "w2", 1),
          Row("w1 w2", "g1", "w1", 1),
          Row("w1 w1 w1", "g2", "w1", 3)))
    } finally {
      runQuery(s"drop function if exists $funcName(STRING)", session)
    }
  }

  test("basic UDTF word count with endPartition()") {
    val funcName: String = randomFunctionName()
    try {
      // Use anonymous class for UDF2
      val myWordCount = new UDTF2[String, String] {
        val partitionWordCounts = mutable.Map[String, Int]()

        override def process(s1: String, s2: String): Iterable[Row] = {
          val wordCounts = mutable.Map[String, Int]()
          Seq(s1, s2).foreach(_.split(" ").foreach { x =>
            wordCounts.update(x, wordCounts.getOrElse(x, 0) + 1)
            partitionWordCounts.update(x, partitionWordCounts.getOrElse(x, 0) + 1)
          })
          wordCounts.toArray.map { p =>
            Row(p._1, p._2)
          }
        }

        override def endPartition(): Iterable[Row] = partitionWordCounts.toArray.map { p =>
          Row(p._1, p._2)
        }

        override def outputSchema(): StructType =
          StructType(StructField("word", StringType), StructField("count", IntegerType))
      }

      val tableFunction = session.udtf.registerTemporary(funcName, myWordCount)

      // Check output columns name and type
      val df1 =
        session.tableFunction(tableFunction, lit("w1 w2 w2 w3 w3 w3"), lit("w1 w2 w2 w3 w3 w3"))
      assert(
        getSchemaString(df1.schema) ==
          """root
            | |--WORD: String (nullable = true)
            | |--COUNT: Long (nullable = true)
            |""".stripMargin)
      checkAnswer(
        df1,
        Seq(Row("w3", 6), Row("w2", 4), Row("w1", 2), Row("w3", 6), Row("w2", 4), Row("w1", 2)))

      // Call the UDTF with funcName and named parameters, result should be the same
      val df2 = session
        .tableFunction(
          TableFunction(funcName),
          Map("arg1" -> lit("w1 w2 w2 w3 w3 w3"), "arg2" -> lit("w1 w2 w2 w3 w3 w3")))
        .select("count", "word")
      assert(
        getSchemaString(df2.schema) ==
          """root
            | |--COUNT: Long (nullable = true)
            | |--WORD: String (nullable = true)
            |""".stripMargin)
      checkAnswer(
        df2,
        Seq(Row(6, "w3"), Row(4, "w2"), Row(2, "w1"), Row(6, "w3"), Row(4, "w2"), Row(2, "w1")))

      // scalastyle:off
      // Use UDTF with table join
      // If the statement does not explicitly specify partitioning, then the Snowflake
      // execution engine partitions the input according to multiple factors, such as
      // the size of the warehouse processing the function and the cardinality of
      // the input relation. When running in this mode, the user code can make no assumptions
      // about partitions. Running without an explicit partition is most useful when the UDTF
      // only needs to look at rows in isolation to produce its output and no state is aggregated
      // across rows.
      // https://docs.snowflake.com/en/developer-guide/udf/java/udf-java-tabular-functions.html#calling-without-explicit-partitioning
      // So use "over (partition by 1)" to avoid unstable result.
      // scalastyle:on
      val df3 = session.sql(
        s"select * from $wordCountTableName, " +
          s"table($funcName(c1, c2) over (partition by 1))")
      assert(
        getSchemaString(df3.schema) ==
          """root
            | |--C1: String (nullable = true)
            | |--C2: String (nullable = true)
            | |--WORD: String (nullable = true)
            | |--COUNT: Long (nullable = true)
            |""".stripMargin)
      checkAnswer(
        df3,
        Seq(
          Row("w1 w2", "g1", "w2", 1),
          Row("w1 w2", "g1", "g1", 1),
          Row("w1 w2", "g1", "w1", 1),
          Row("w1 w1 w1", "g2", "g2", 1),
          Row("w1 w1 w1", "g2", "w1", 3),
          Row(null, null, "g2", 1),
          Row(null, null, "w2", 1),
          Row(null, null, "g1", 1),
          Row(null, null, "w1", 4)))

      // Use UDTF with table function + over partition
      val df4 = session.sql(
        s"select * from $wordCountTableName, table($funcName(c1, c2) over (partition by c2))")
      checkAnswer(
        df4,
        Seq(
          Row("w1 w2", "g1", "w2", 1),
          Row("w1 w2", "g1", "g1", 1),
          Row("w1 w2", "g1", "w1", 1),
          Row(null, "g1", "w2", 1),
          Row(null, "g1", "g1", 1),
          Row(null, "g1", "w1", 1),
          Row("w1 w1 w1", "g2", "g2", 1),
          Row("w1 w1 w1", "g2", "w1", 3),
          Row(null, "g2", "g2", 1),
          Row(null, "g2", "w1", 3)))
    } finally {
      runQuery(s"drop function if exists $funcName(VARCHAR,VARCHAR)", session)
    }
  }

  test("basic UDTF: RANGE(start, count)") {
    val funcName: String = randomFunctionName()
    try {
      class MyRangeUdtf2 extends UDTF2[Int, Int] {
        override def process(start: Int, count: Int): Iterable[Row] =
          (start until (start + count)).map(Row(_))

        override def endPartition(): Iterable[Row] = Array.empty[Row]

        override def outputSchema(): StructType =
          StructType(StructField("C1", IntegerType))
      }

      val tableFunction = session.udtf.registerTemporary(funcName, new MyRangeUdtf2())

      val df1 = session.tableFunction(tableFunction, lit(10), lit(5))
      assert(
        getSchemaString(df1.schema) ==
          """root
            | |--C1: Long (nullable = true)
            |""".stripMargin)
      checkAnswer(df1, Seq(Row(10), Row(11), Row(12), Row(13), Row(14)))

      val df2 = session.tableFunction(TableFunction(funcName), lit(20), lit(5))
      assert(
        getSchemaString(df2.schema) ==
          """root
            | |--C1: Long (nullable = true)
            |""".stripMargin)
      checkAnswer(df2, Seq(Row(20), Row(21), Row(22), Row(23), Row(24)))

      val df3 = session
        .tableFunction(tableFunction, Map("arg1" -> lit(30), "arg2" -> lit(5)))
      assert(
        getSchemaString(df3.schema) ==
          """root
            | |--C1: Long (nullable = true)
            |""".stripMargin)
      checkAnswer(df3, Seq(Row(30), Row(31), Row(32), Row(33), Row(34)))
    } finally {
      runQuery(s"drop function if exists $funcName(NUMBER, NUMBER)", session)
    }
  }

  test("register an UDTF multiple times") {
    val funcName: String = randomFunctionName()
    val funcName2: String = randomFunctionName()

    try {
      class MyDuplicateRegisterUDTF1 extends UDTF1[String] {
        override def process(s1: String): Iterable[Row] = {
          val wordCounts = mutable.Map[String, Int]()
          s1.split(" ").foreach { x =>
            wordCounts.update(x, wordCounts.getOrElse(x, 0) + 1)
          }
          wordCounts.toArray.map { p =>
            Row(p._1, p._2)
          }
        }

        override def endPartition(): Iterable[Row] = Seq.empty

        override def outputSchema(): StructType =
          StructType(StructField("word", StringType), StructField("count", IntegerType))
      }

      session.udtf.registerTemporary(funcName, new MyDuplicateRegisterUDTF1())
      session.udtf.registerTemporary(funcName2, new MyDuplicateRegisterUDTF1())
    } finally {
      runQuery(s"drop function if exists $funcName(STRING)", session)
    }
  }

  test("register large UDTF object") {
    val funcName: String = randomFunctionName()

    try {
      class LargeUDTF extends UDTF1[String] {
        val largeString = (1 to 500).map(_ => "large_data_string").reduce(_ + "_" + _)

        override def process(s1: String): Iterable[Row] = {
          val wordCounts = mutable.Map[String, Int]()
          s1.split(" ").foreach { x =>
            wordCounts.update(x, wordCounts.getOrElse(x, 0) + 1)
          }
          wordCounts.toArray.map { p =>
            Row(p._1, p._2, largeString.length)
          }
        }
        override def endPartition(): Iterable[Row] = Seq.empty
        override def outputSchema(): StructType =
          StructType(
            StructField("word", StringType),
            StructField("count", IntegerType),
            StructField("size", IntegerType))
      }

      val largeUdTF = new LargeUDTF()
      val dataLength = largeUdTF.largeString.length
      assert(JavaUtils.serialize(largeUdTF).length > 8192)

      val tableFunction = session.udtf.registerTemporary(funcName, new LargeUDTF())
      // Check output columns name and type
      val df1 = session.tableFunction(tableFunction, lit("w1 w2"))
      assert(
        getSchemaString(df1.schema) ==
          """root
            | |--WORD: String (nullable = true)
            | |--COUNT: Long (nullable = true)
            | |--SIZE: Long (nullable = true)
            |""".stripMargin)
      checkAnswer(df1, Seq(Row("w1", 1, dataLength), Row("w2", 1, dataLength)))
    } finally {
      runQuery(s"drop function if exists $funcName(STRING)", session)
    }
  }

  test("negative test with invalid output column name") {
    val funcName2 = randomFunctionName()

    val ex = intercept[SnowparkClientException] {
      class MyInvalidNameUdtf2 extends UDTF1[String] {
        override def process(s1: String): Iterable[Row] = Seq.empty
        override def endPartition(): Iterable[Row] = Seq.empty
        override def outputSchema(): StructType = StructType(StructField("bad name", StringType))
      }
      session.udtf.registerTemporary(funcName2, new MyInvalidNameUdtf2)
    }
    assert(ex.errorCode.equals("0208"))
  }

  test("UDTF with DataFrame columns") {
    // Test table function with multiple columns from the same DataFrame
    val funcName: String = randomFunctionName()
    try {
      class MyRangeUdtf3 extends UDTF2[Int, Int] {
        override def process(start: Int, count: Int): Iterable[Row] =
          (start until (start + count)).map(Row(_))

        override def endPartition(): Iterable[Row] = Array.empty[Row]

        override def outputSchema(): StructType =
          StructType(StructField("C1", IntegerType))
      }
      val tableFunction = session.udtf.registerTemporary(funcName, new MyRangeUdtf3())
      // Check table function with df column arguments as sequence
      val sourceDF = Seq((10, 100, 2), (20, 200, 4)).toDF(Seq("a", "b", "c"))
      val df1 = session.tableFunction(tableFunction, sourceDF("a"), sourceDF("c"))
      assert(
        getSchemaString(df1.schema) ==
          """root
            | |--C1: Long (nullable = true)
            |""".stripMargin)
      checkAnswer(df1, Seq(Row(10), Row(11), Row(20), Row(21), Row(22), Row(23)))

      val df2 = session.tableFunction(TableFunction(funcName), sourceDF("b"), sourceDF("c"))
      assert(
        getSchemaString(df2.schema) ==
          """root
            | |--C1: Long (nullable = true)
            |""".stripMargin)
      checkAnswer(df2, Seq(Row(100), Row(101), Row(200), Row(201), Row(202), Row(203)))

      // Check table function with df column arguments as Map
      val sourceDF2 = Seq(30).toDF("a")
      val df3 = session
        .tableFunction(tableFunction, Map("arg1" -> sourceDF2("a"), "arg2" -> lit(5)))
      assert(
        getSchemaString(df3.schema) ==
          """root
            | |--C1: Long (nullable = true)
            |""".stripMargin)
      checkAnswer(df3, Seq(Row(30), Row(31), Row(32), Row(33), Row(34)))

      // Check table function with nested functions on df column
      val df4 = session.tableFunction(tableFunction, abs(ceil(sourceDF("a"))), lit(2))
      assert(
        getSchemaString(df4.schema) ==
          """root
            | |--C1: Long (nullable = true)
            |""".stripMargin)
      checkAnswer(df4, Seq(Row(10), Row(11), Row(20), Row(21)))

      // Check result df column filtering with duplicate column names
      val sourceDF3 = Seq(30).toDF("C1")
      val df5 = session
        .tableFunction(tableFunction, sourceDF3("C1"), lit(5))
      val df6 = session
        .tableFunction(tableFunction, Map("arg1" -> sourceDF3("C1"), "arg2" -> lit(5)))
      Seq(df5, df6).foreach { df =>
        assert(
          getSchemaString(df.schema) ==
            """root
              | |--C1: Long (nullable = true)
              |""".stripMargin)
        checkAnswer(df, Seq(Row(30), Row(31), Row(32), Row(33), Row(34)))
      }
    } finally {
      runQuery(s"drop function if exists $funcName(NUMBER, NUMBER)", session)
    }
  }

  test("Columns from multiple DataFrames as input to session.TableFunction throws exception") {
    val funcName: String = randomFunctionName()
    try {
      class MyRangeUdtf3 extends UDTF2[Int, Int] {
        override def process(start: Int, count: Int): Iterable[Row] =
          (start until (start + count)).map(Row(_))

        override def endPartition(): Iterable[Row] = Array.empty[Row]

        override def outputSchema(): StructType =
          StructType(StructField("C1", IntegerType))
      }
      val tableFunction = session.udtf.registerTemporary(funcName, new MyRangeUdtf3())
      val sourceDF = Seq((10, 100, 2), (20, 200, 4)).toDF(Seq("a", "b", "c"))
      val sourceDF2 = Seq(30).toDF("a")
      // Check exception for column arguments as Seq and Map from multiple DFs
      val ex1 = intercept[SnowparkClientException] {
        session
          .tableFunction(tableFunction, sourceDF("a"), sourceDF2("a"))
      }
      assert(ex1.errorCode.equals("0212"))
      val ex2 = intercept[SnowparkClientException] {
        session
          .tableFunction(tableFunction, Map("arg1" -> sourceDF("a"), "arg2" -> sourceDF2("a")))
      }
      assert(ex2.errorCode.equals("0212"))
      // Check exception for column arguments from clone DF
      val ex3 = intercept[SnowparkClientException] {
        session
          .tableFunction(tableFunction, sourceDF("a"), sourceDF.clone()("a"))
      }
      assert(ex3.errorCode.equals("0212"))
    } finally {
      runQuery(s"drop function if exists $funcName(NUMBER, NUMBER)", session)
    }
  }

  /*
  test("generate test for UDTF 1 - 22") {
    (1 to 22).foreach { x =>
      val types = (1 to x).map(_ => s"Int").mkString(", ")
      val processArgs = (1 to x).map(i => s"a$i: Int").mkString(", ")
      val sumArgs = (1 to x).map(i => s"a$i").mkString(", ")
      val literals = (1 to x).map(i => s"lit($i)").mkString(", ")
      val code =
        s"""  test("test UDTFX of UDTF$x") {
           |    class MyUDTF$x extends UDTF$x[$types] {
           |      override def process($processArgs): Iterable[Row] = {
           |        val sum = Seq($sumArgs).sum
           |        Seq(Row(sum), Row(sum))
           |      }
           |      override def endPartition(): Iterable[Row] = Seq.empty
           |      override def outputSchema(): StructType =
           |       StructType(StructField("sum", IntegerType))
           |    }
           |
           |    val tableFunction = session.udtf.registerTemporary(new MyUDTF$x())
           |    val df1 = session.tableFunction(tableFunction, $literals)
           |    val result = (1 to $x).sum
           |    checkAnswer(df1, Seq(Row(result), Row(result)))
           |  }
           |""".stripMargin
    // println(code)
    }
  }
   */

  // Test case for UDTF0 is manually created.
  test("test UDTFX of UDTF0") {
    class MyUDTF0 extends UDTF0 {
      override def process(): Iterable[Row] = {
        Seq(Row(123), Row(123))
      }
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("sum", IntegerType))
    }

    val tableFunction = session.udtf.registerTemporary(new MyUDTF0())
    val df1 = session.sql(s"select * from TABLE(${tableFunction.funcName}())")
    checkAnswer(df1, Seq(Row(123), Row(123)))
  }

  test("test UDTFX of UDTF1") {
    class MyUDTF1 extends UDTF1[Int] {
      override def process(a1: Int): Iterable[Row] = {
        val sum = Seq(a1).sum
        Seq(Row(sum), Row(sum))
      }
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("sum", IntegerType))
    }

    val tableFunction = session.udtf.registerTemporary(new MyUDTF1())
    val df1 = session.tableFunction(tableFunction, lit(1))
    val result = (1 to 1).sum
    checkAnswer(df1, Seq(Row(result), Row(result)))
  }

  test("test UDTFX of UDTF2", JavaStoredProcExclude) {
    class MyUDTF2 extends UDTF2[Int, Int] {
      override def process(a1: Int, a2: Int): Iterable[Row] = {
        val sum = Seq(a1, a2).sum
        Seq(Row(sum), Row(sum))
      }
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("sum", IntegerType))
    }

    val tableFunction = session.udtf.registerTemporary(new MyUDTF2())
    val df1 = session.tableFunction(tableFunction, lit(1), lit(2))
    val result = (1 to 2).sum
    checkAnswer(df1, Seq(Row(result), Row(result)))
  }

  test("test UDTFX of UDTF3, JavaStoredProcExclude") {
    class MyUDTF3 extends UDTF3[Int, Int, Int] {
      override def process(a1: Int, a2: Int, a3: Int): Iterable[Row] = {
        val sum = Seq(a1, a2, a3).sum
        Seq(Row(sum), Row(sum))
      }
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("sum", IntegerType))
    }

    val tableFunction = session.udtf.registerTemporary(new MyUDTF3())
    val df1 = session.tableFunction(tableFunction, lit(1), lit(2), lit(3))
    val result = (1 to 3).sum
    checkAnswer(df1, Seq(Row(result), Row(result)))
  }

  test("test UDTFX of UDTF4", JavaStoredProcExclude) {
    class MyUDTF4 extends UDTF4[Int, Int, Int, Int] {
      override def process(a1: Int, a2: Int, a3: Int, a4: Int): Iterable[Row] = {
        val sum = Seq(a1, a2, a3, a4).sum
        Seq(Row(sum), Row(sum))
      }
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("sum", IntegerType))
    }

    val tableFunction = session.udtf.registerTemporary(new MyUDTF4())
    val df1 = session.tableFunction(tableFunction, lit(1), lit(2), lit(3), lit(4))
    val result = (1 to 4).sum
    checkAnswer(df1, Seq(Row(result), Row(result)))
  }

  test("test UDTFX of UDTF5", JavaStoredProcExclude) {
    class MyUDTF5 extends UDTF5[Int, Int, Int, Int, Int] {
      override def process(a1: Int, a2: Int, a3: Int, a4: Int, a5: Int): Iterable[Row] = {
        val sum = Seq(a1, a2, a3, a4, a5).sum
        Seq(Row(sum), Row(sum))
      }
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("sum", IntegerType))
    }

    val tableFunction = session.udtf.registerTemporary(new MyUDTF5())
    val df1 = session.tableFunction(tableFunction, lit(1), lit(2), lit(3), lit(4), lit(5))
    val result = (1 to 5).sum
    checkAnswer(df1, Seq(Row(result), Row(result)))
  }

  test("test UDTFX of UDTF6", JavaStoredProcExclude) {
    class MyUDTF6 extends UDTF6[Int, Int, Int, Int, Int, Int] {
      override def process(a1: Int, a2: Int, a3: Int, a4: Int, a5: Int, a6: Int): Iterable[Row] = {
        val sum = Seq(a1, a2, a3, a4, a5, a6).sum
        Seq(Row(sum), Row(sum))
      }
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("sum", IntegerType))
    }

    val tableFunction = session.udtf.registerTemporary(new MyUDTF6())
    val df1 = session.tableFunction(tableFunction, lit(1), lit(2), lit(3), lit(4), lit(5), lit(6))
    val result = (1 to 6).sum
    checkAnswer(df1, Seq(Row(result), Row(result)))
  }

  test("test UDTFX of UDTF7", JavaStoredProcExclude) {
    class MyUDTF7 extends UDTF7[Int, Int, Int, Int, Int, Int, Int] {
      override def process(a1: Int, a2: Int, a3: Int, a4: Int, a5: Int, a6: Int, a7: Int)
          : Iterable[Row] = {
        val sum = Seq(a1, a2, a3, a4, a5, a6, a7).sum
        Seq(Row(sum), Row(sum))
      }
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("sum", IntegerType))
    }

    val tableFunction = session.udtf.registerTemporary(new MyUDTF7())
    val df1 =
      session.tableFunction(tableFunction, lit(1), lit(2), lit(3), lit(4), lit(5), lit(6), lit(7))
    val result = (1 to 7).sum
    checkAnswer(df1, Seq(Row(result), Row(result)))
  }

  test("test UDTFX of UDTF8", JavaStoredProcExclude) {
    class MyUDTF8 extends UDTF8[Int, Int, Int, Int, Int, Int, Int, Int] {
      override def process(a1: Int, a2: Int, a3: Int, a4: Int, a5: Int, a6: Int, a7: Int, a8: Int)
          : Iterable[Row] = {
        val sum = Seq(a1, a2, a3, a4, a5, a6, a7, a8).sum
        Seq(Row(sum), Row(sum))
      }
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("sum", IntegerType))
    }

    val tableFunction = session.udtf.registerTemporary(new MyUDTF8())
    val df1 = session.tableFunction(
      tableFunction,
      lit(1),
      lit(2),
      lit(3),
      lit(4),
      lit(5),
      lit(6),
      lit(7),
      lit(8))
    val result = (1 to 8).sum
    checkAnswer(df1, Seq(Row(result), Row(result)))
  }

  test("test UDTFX of UDTF9", JavaStoredProcExclude) {
    class MyUDTF9 extends UDTF9[Int, Int, Int, Int, Int, Int, Int, Int, Int] {
      override def process(
          a1: Int,
          a2: Int,
          a3: Int,
          a4: Int,
          a5: Int,
          a6: Int,
          a7: Int,
          a8: Int,
          a9: Int): Iterable[Row] = {
        val sum = Seq(a1, a2, a3, a4, a5, a6, a7, a8, a9).sum
        Seq(Row(sum), Row(sum))
      }
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("sum", IntegerType))
    }

    val tableFunction = session.udtf.registerTemporary(new MyUDTF9())
    val df1 = session.tableFunction(
      tableFunction,
      lit(1),
      lit(2),
      lit(3),
      lit(4),
      lit(5),
      lit(6),
      lit(7),
      lit(8),
      lit(9))
    val result = (1 to 9).sum
    checkAnswer(df1, Seq(Row(result), Row(result)))
  }

  test("test UDTFX of UDTF10", JavaStoredProcExclude) {
    class MyUDTF10 extends UDTF10[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int] {
      override def process(
          a1: Int,
          a2: Int,
          a3: Int,
          a4: Int,
          a5: Int,
          a6: Int,
          a7: Int,
          a8: Int,
          a9: Int,
          a10: Int): Iterable[Row] = {
        val sum = Seq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10).sum
        Seq(Row(sum), Row(sum))
      }
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("sum", IntegerType))
    }

    val tableFunction = session.udtf.registerTemporary(new MyUDTF10())
    val df1 = session.tableFunction(
      tableFunction,
      lit(1),
      lit(2),
      lit(3),
      lit(4),
      lit(5),
      lit(6),
      lit(7),
      lit(8),
      lit(9),
      lit(10))
    val result = (1 to 10).sum
    checkAnswer(df1, Seq(Row(result), Row(result)))
  }

  test("test UDTFX of UDTF11", JavaStoredProcExclude) {
    class MyUDTF11 extends UDTF11[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int] {
      // scalastyle:off
      override def process(
          a1: Int,
          a2: Int,
          a3: Int,
          a4: Int,
          a5: Int,
          a6: Int,
          a7: Int,
          a8: Int,
          a9: Int,
          a10: Int,
          a11: Int): Iterable[Row] = {
        val sum = Seq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11).sum
        Seq(Row(sum), Row(sum))
      }
      // scalastyle:on
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("sum", IntegerType))
    }

    val tableFunction = session.udtf.registerTemporary(new MyUDTF11())
    val df1 = session.tableFunction(
      tableFunction,
      lit(1),
      lit(2),
      lit(3),
      lit(4),
      lit(5),
      lit(6),
      lit(7),
      lit(8),
      lit(9),
      lit(10),
      lit(11))
    val result = (1 to 11).sum
    checkAnswer(df1, Seq(Row(result), Row(result)))
  }

  test("test UDTFX of UDTF12", JavaStoredProcExclude) {
    class MyUDTF12 extends UDTF12[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int] {
      // scalastyle:off
      override def process(
          a1: Int,
          a2: Int,
          a3: Int,
          a4: Int,
          a5: Int,
          a6: Int,
          a7: Int,
          a8: Int,
          a9: Int,
          a10: Int,
          a11: Int,
          a12: Int): Iterable[Row] = {
        val sum = Seq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12).sum
        Seq(Row(sum), Row(sum))
      }
      // scalastyle:on
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("sum", IntegerType))
    }

    val tableFunction = session.udtf.registerTemporary(new MyUDTF12())
    val df1 = session.tableFunction(
      tableFunction,
      lit(1),
      lit(2),
      lit(3),
      lit(4),
      lit(5),
      lit(6),
      lit(7),
      lit(8),
      lit(9),
      lit(10),
      lit(11),
      lit(12))
    val result = (1 to 12).sum
    checkAnswer(df1, Seq(Row(result), Row(result)))
  }

  test("test UDTFX of UDTF13", JavaStoredProcExclude) {
    class MyUDTF13 extends UDTF13[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int] {
      // scalastyle:off
      override def process(
          a1: Int,
          a2: Int,
          a3: Int,
          a4: Int,
          a5: Int,
          a6: Int,
          a7: Int,
          a8: Int,
          a9: Int,
          a10: Int,
          a11: Int,
          a12: Int,
          a13: Int): Iterable[Row] = {
        val sum = Seq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13).sum
        Seq(Row(sum), Row(sum))
      }
      // scalastyle:on
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("sum", IntegerType))
    }

    val tableFunction = session.udtf.registerTemporary(new MyUDTF13())
    val df1 = session.tableFunction(
      tableFunction,
      lit(1),
      lit(2),
      lit(3),
      lit(4),
      lit(5),
      lit(6),
      lit(7),
      lit(8),
      lit(9),
      lit(10),
      lit(11),
      lit(12),
      lit(13))
    val result = (1 to 13).sum
    checkAnswer(df1, Seq(Row(result), Row(result)))
  }

  test("test UDTFX of UDTF14", JavaStoredProcExclude) {
    class MyUDTF14
        extends UDTF14[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int] {
      // scalastyle:off
      override def process(
          a1: Int,
          a2: Int,
          a3: Int,
          a4: Int,
          a5: Int,
          a6: Int,
          a7: Int,
          a8: Int,
          a9: Int,
          a10: Int,
          a11: Int,
          a12: Int,
          a13: Int,
          a14: Int): Iterable[Row] = {
        val sum = Seq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14).sum
        Seq(Row(sum), Row(sum))
      }
      // scalastyle:off
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("sum", IntegerType))
    }

    val tableFunction = session.udtf.registerTemporary(new MyUDTF14())
    val df1 = session.tableFunction(
      tableFunction,
      lit(1),
      lit(2),
      lit(3),
      lit(4),
      lit(5),
      lit(6),
      lit(7),
      lit(8),
      lit(9),
      lit(10),
      lit(11),
      lit(12),
      lit(13),
      lit(14))
    val result = (1 to 14).sum
    checkAnswer(df1, Seq(Row(result), Row(result)))
  }

  test("test UDTFX of UDTF15", JavaStoredProcExclude) {
    class MyUDTF15
        extends UDTF15[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int] {
      override def process(
          a1: Int,
          a2: Int,
          a3: Int,
          a4: Int,
          a5: Int,
          a6: Int,
          a7: Int,
          a8: Int,
          a9: Int,
          a10: Int,
          a11: Int,
          a12: Int,
          a13: Int,
          a14: Int,
          a15: Int): Iterable[Row] = {
        val sum = Seq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15).sum
        Seq(Row(sum), Row(sum))
      }
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("sum", IntegerType))
    }

    val tableFunction = session.udtf.registerTemporary(new MyUDTF15())
    val df1 = session.tableFunction(
      tableFunction,
      lit(1),
      lit(2),
      lit(3),
      lit(4),
      lit(5),
      lit(6),
      lit(7),
      lit(8),
      lit(9),
      lit(10),
      lit(11),
      lit(12),
      lit(13),
      lit(14),
      lit(15))
    val result = (1 to 15).sum
    checkAnswer(df1, Seq(Row(result), Row(result)))
  }

  test("test UDTFX of UDTF16", JavaStoredProcExclude) {
    class MyUDTF16
        extends UDTF16[
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int] {
      override def process(
          a1: Int,
          a2: Int,
          a3: Int,
          a4: Int,
          a5: Int,
          a6: Int,
          a7: Int,
          a8: Int,
          a9: Int,
          a10: Int,
          a11: Int,
          a12: Int,
          a13: Int,
          a14: Int,
          a15: Int,
          a16: Int): Iterable[Row] = {
        val sum = Seq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16).sum
        Seq(Row(sum), Row(sum))
      }
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("sum", IntegerType))
    }

    val tableFunction = session.udtf.registerTemporary(new MyUDTF16())
    val df1 = session.tableFunction(
      tableFunction,
      lit(1),
      lit(2),
      lit(3),
      lit(4),
      lit(5),
      lit(6),
      lit(7),
      lit(8),
      lit(9),
      lit(10),
      lit(11),
      lit(12),
      lit(13),
      lit(14),
      lit(15),
      lit(16))
    val result = (1 to 16).sum
    checkAnswer(df1, Seq(Row(result), Row(result)))
  }

  test("test UDTFX of UDTF17", JavaStoredProcExclude) {
    class MyUDTF17
        extends UDTF17[
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int] {
      override def process(
          a1: Int,
          a2: Int,
          a3: Int,
          a4: Int,
          a5: Int,
          a6: Int,
          a7: Int,
          a8: Int,
          a9: Int,
          a10: Int,
          a11: Int,
          a12: Int,
          a13: Int,
          a14: Int,
          a15: Int,
          a16: Int,
          a17: Int): Iterable[Row] = {
        val sum =
          Seq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17).sum
        Seq(Row(sum), Row(sum))
      }
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("sum", IntegerType))
    }

    val tableFunction = session.udtf.registerTemporary(new MyUDTF17())
    val df1 = session.tableFunction(
      tableFunction,
      lit(1),
      lit(2),
      lit(3),
      lit(4),
      lit(5),
      lit(6),
      lit(7),
      lit(8),
      lit(9),
      lit(10),
      lit(11),
      lit(12),
      lit(13),
      lit(14),
      lit(15),
      lit(16),
      lit(17))
    val result = (1 to 17).sum
    checkAnswer(df1, Seq(Row(result), Row(result)))
  }

  test("test UDTFX of UDTF18", JavaStoredProcExclude) {
    class MyUDTF18
        extends UDTF18[
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int] {
      override def process(
          a1: Int,
          a2: Int,
          a3: Int,
          a4: Int,
          a5: Int,
          a6: Int,
          a7: Int,
          a8: Int,
          a9: Int,
          a10: Int,
          a11: Int,
          a12: Int,
          a13: Int,
          a14: Int,
          a15: Int,
          a16: Int,
          a17: Int,
          a18: Int): Iterable[Row] = {
        val sum =
          Seq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18).sum
        Seq(Row(sum), Row(sum))
      }
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("sum", IntegerType))
    }

    val tableFunction = session.udtf.registerTemporary(new MyUDTF18())
    val df1 = session.tableFunction(
      tableFunction,
      lit(1),
      lit(2),
      lit(3),
      lit(4),
      lit(5),
      lit(6),
      lit(7),
      lit(8),
      lit(9),
      lit(10),
      lit(11),
      lit(12),
      lit(13),
      lit(14),
      lit(15),
      lit(16),
      lit(17),
      lit(18))
    val result = (1 to 18).sum
    checkAnswer(df1, Seq(Row(result), Row(result)))
  }

  test("test UDTFX of UDTF19", JavaStoredProcExclude) {
    class MyUDTF19
        extends UDTF19[
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int] {
      override def process(
          a1: Int,
          a2: Int,
          a3: Int,
          a4: Int,
          a5: Int,
          a6: Int,
          a7: Int,
          a8: Int,
          a9: Int,
          a10: Int,
          a11: Int,
          a12: Int,
          a13: Int,
          a14: Int,
          a15: Int,
          a16: Int,
          a17: Int,
          a18: Int,
          a19: Int): Iterable[Row] = {
        val sum = Seq(
          a1,
          a2,
          a3,
          a4,
          a5,
          a6,
          a7,
          a8,
          a9,
          a10,
          a11,
          a12,
          a13,
          a14,
          a15,
          a16,
          a17,
          a18,
          a19).sum
        Seq(Row(sum), Row(sum))
      }
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("sum", IntegerType))
    }

    val tableFunction = session.udtf.registerTemporary(new MyUDTF19())
    val df1 = session.tableFunction(
      tableFunction,
      lit(1),
      lit(2),
      lit(3),
      lit(4),
      lit(5),
      lit(6),
      lit(7),
      lit(8),
      lit(9),
      lit(10),
      lit(11),
      lit(12),
      lit(13),
      lit(14),
      lit(15),
      lit(16),
      lit(17),
      lit(18),
      lit(19))
    val result = (1 to 19).sum
    checkAnswer(df1, Seq(Row(result), Row(result)))
  }

  test("test UDTFX of UDTF20", JavaStoredProcExclude) {
    class MyUDTF20
        extends UDTF20[
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int] {
      override def process(
          a1: Int,
          a2: Int,
          a3: Int,
          a4: Int,
          a5: Int,
          a6: Int,
          a7: Int,
          a8: Int,
          a9: Int,
          a10: Int,
          a11: Int,
          a12: Int,
          a13: Int,
          a14: Int,
          a15: Int,
          a16: Int,
          a17: Int,
          a18: Int,
          a19: Int,
          a20: Int): Iterable[Row] = {
        val sum = Seq(
          a1,
          a2,
          a3,
          a4,
          a5,
          a6,
          a7,
          a8,
          a9,
          a10,
          a11,
          a12,
          a13,
          a14,
          a15,
          a16,
          a17,
          a18,
          a19,
          a20).sum
        Seq(Row(sum), Row(sum))
      }
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("sum", IntegerType))
    }

    val tableFunction = session.udtf.registerTemporary(new MyUDTF20())
    val df1 = session.tableFunction(
      tableFunction,
      lit(1),
      lit(2),
      lit(3),
      lit(4),
      lit(5),
      lit(6),
      lit(7),
      lit(8),
      lit(9),
      lit(10),
      lit(11),
      lit(12),
      lit(13),
      lit(14),
      lit(15),
      lit(16),
      lit(17),
      lit(18),
      lit(19),
      lit(20))
    val result = (1 to 20).sum
    checkAnswer(df1, Seq(Row(result), Row(result)))
  }

  test("test UDTFX of UDTF21", JavaStoredProcExclude) {
    class MyUDTF21
        extends UDTF21[
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int] {
      override def process(
          a1: Int,
          a2: Int,
          a3: Int,
          a4: Int,
          a5: Int,
          a6: Int,
          a7: Int,
          a8: Int,
          a9: Int,
          a10: Int,
          a11: Int,
          a12: Int,
          a13: Int,
          a14: Int,
          a15: Int,
          a16: Int,
          a17: Int,
          a18: Int,
          a19: Int,
          a20: Int,
          a21: Int): Iterable[Row] = {
        val sum = Seq(
          a1,
          a2,
          a3,
          a4,
          a5,
          a6,
          a7,
          a8,
          a9,
          a10,
          a11,
          a12,
          a13,
          a14,
          a15,
          a16,
          a17,
          a18,
          a19,
          a20,
          a21).sum
        Seq(Row(sum), Row(sum))
      }
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("sum", IntegerType))
    }

    val tableFunction = session.udtf.registerTemporary(new MyUDTF21())
    val df1 = session.tableFunction(
      tableFunction,
      lit(1),
      lit(2),
      lit(3),
      lit(4),
      lit(5),
      lit(6),
      lit(7),
      lit(8),
      lit(9),
      lit(10),
      lit(11),
      lit(12),
      lit(13),
      lit(14),
      lit(15),
      lit(16),
      lit(17),
      lit(18),
      lit(19),
      lit(20),
      lit(21))
    val result = (1 to 21).sum
    checkAnswer(df1, Seq(Row(result), Row(result)))
  }

  test("test UDTFX of UDTF22") {
    class MyUDTF22
        extends UDTF22[
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int,
          Int] {
      override def process(
          a1: Int,
          a2: Int,
          a3: Int,
          a4: Int,
          a5: Int,
          a6: Int,
          a7: Int,
          a8: Int,
          a9: Int,
          a10: Int,
          a11: Int,
          a12: Int,
          a13: Int,
          a14: Int,
          a15: Int,
          a16: Int,
          a17: Int,
          a18: Int,
          a19: Int,
          a20: Int,
          a21: Int,
          a22: Int): Iterable[Row] = {
        val sum = Seq(
          a1,
          a2,
          a3,
          a4,
          a5,
          a6,
          a7,
          a8,
          a9,
          a10,
          a11,
          a12,
          a13,
          a14,
          a15,
          a16,
          a17,
          a18,
          a19,
          a20,
          a21,
          a22).sum
        Seq(Row(sum), Row(sum))
      }
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("sum", IntegerType))
    }

    val tableFunction = session.udtf.registerTemporary(new MyUDTF22())
    val df1 = session.tableFunction(
      tableFunction,
      lit(1),
      lit(2),
      lit(3),
      lit(4),
      lit(5),
      lit(6),
      lit(7),
      lit(8),
      lit(9),
      lit(10),
      lit(11),
      lit(12),
      lit(13),
      lit(14),
      lit(15),
      lit(16),
      lit(17),
      lit(18),
      lit(19),
      lit(20),
      lit(21),
      lit(22))
    val result = (1 to 22).sum
    checkAnswer(df1, Seq(Row(result), Row(result)))
  }

  test("test input Type: Option[_]") {
    class UDTFInputOptionTypes
        extends UDTF6[
          Option[Short],
          Option[Int],
          Option[Long],
          Option[Float],
          Option[Double],
          Option[Boolean]] {
      override def process(
          si2: Option[Short],
          i2: Option[Int],
          li2: Option[Long],
          f2: Option[Float],
          d2: Option[Double],
          b2: Option[Boolean]): Iterable[Row] = {
        val row = Row(
          si2.map(_.toString).orNull,
          i2.map(_.toString).orNull,
          li2.map(_.toString).orNull,
          f2.map(_.toString).orNull,
          d2.map(_.toString).orNull,
          b2.map(_.toString).orNull)
        Seq(row)
      }
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType =
        StructType(
          StructField("si2_str", StringType),
          StructField("i2_str", StringType),
          StructField("bi2_str", StringType),
          StructField("f2_str", StringType),
          StructField("d2_str", StringType),
          StructField("b2_str", StringType))
    }

    createTable(tableName, "si2 smallint, i2 int, bi2 bigint, f2 float, d2 double, b2 boolean")
    runQuery(
      s"insert into $tableName values (1, 2, 3, 4.4, 8.8, true)," +
        s" (null, null, null, null, null, null)",
      session)
    val tableFunction = session.udtf.registerTemporary(new UDTFInputOptionTypes)
    val df1 = session
      .table(tableName)
      .join(tableFunction, col("si2"), col("i2"), col("bi2"), col("f2"), col("d2"), col("b2"))
    assert(
      getSchemaString(df1.schema) ==
        """root
          | |--SI2: Long (nullable = true)
          | |--I2: Long (nullable = true)
          | |--BI2: Long (nullable = true)
          | |--F2: Double (nullable = true)
          | |--D2: Double (nullable = true)
          | |--B2: Boolean (nullable = true)
          | |--SI2_STR: String (nullable = true)
          | |--I2_STR: String (nullable = true)
          | |--BI2_STR: String (nullable = true)
          | |--F2_STR: String (nullable = true)
          | |--D2_STR: String (nullable = true)
          | |--B2_STR: String (nullable = true)
          |""".stripMargin)
    checkAnswer(
      df1,
      Seq(
        Row(1, 2, 3, 4.4, 8.8, true, "1", "2", "3", "4.4", "8.8", "true"),
        Row(null, null, null, null, null, null, null, null, null, null, null, null)))
  }

  test("test input Type: basic types") {
    class UDTFInputBasicTypes
        extends UDTF10[
          Short,
          Int,
          Long,
          Float,
          Double,
          Boolean,
          java.math.BigDecimal,
          String,
          java.lang.String,
          Array[Byte]] {
      override def process(
          si1: Short,
          i1: Int,
          li1: Long,
          f1: Float,
          d1: Double,
          b1: Boolean,
          decimal: java.math.BigDecimal,
          str: String,
          str2: java.lang.String,
          bytes: Array[Byte]): Iterable[Row] = {
        val row = Row(
          si1.toString,
          i1.toString,
          li1.toString,
          f1.toString,
          d1.toString,
          b1.toString,
          decimal.toString,
          str,
          str2,
          bytes.map { _.toChar }.mkString)
        Seq(row)
      }
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType =
        StructType(
          StructField("si1_str", StringType),
          StructField("i1_str", StringType),
          StructField("bi1_str", StringType),
          StructField("f1_str", StringType),
          StructField("d1_str", StringType),
          StructField("b1_str", StringType),
          StructField("decimal_str", StringType),
          StructField("str1_str", StringType),
          StructField("str2_str", StringType),
          StructField("bytes_str", StringType))
    }

    val tableFunction = session.udtf.registerTemporary(new UDTFInputBasicTypes)
    val decimal = new java.math.BigDecimal(123.456).setScale(3, RoundingMode.HALF_DOWN)
    val df1 = session.tableFunction(
      tableFunction,
      lit(1.toShort),
      lit(2.toInt),
      lit(3.toLong),
      lit(4.4.toFloat),
      lit(8.8.toDouble),
      lit(true),
      lit(decimal).cast(DecimalType(38, 18)),
      lit("scala"),
      lit(new java.lang.String("java")),
      lit("bytes".getBytes()))
    assert(
      getSchemaString(df1.schema) ==
        """root
          | |--SI1_STR: String (nullable = true)
          | |--I1_STR: String (nullable = true)
          | |--BI1_STR: String (nullable = true)
          | |--F1_STR: String (nullable = true)
          | |--D1_STR: String (nullable = true)
          | |--B1_STR: String (nullable = true)
          | |--DECIMAL_STR: String (nullable = true)
          | |--STR1_STR: String (nullable = true)
          | |--STR2_STR: String (nullable = true)
          | |--BYTES_STR: String (nullable = true)
          |""".stripMargin)
    checkAnswer(
      df1,
      Seq(
        Row(
          "1",
          "2",
          "3",
          "4.4",
          "8.8",
          "true",
          "123.456000000000000000",
          "scala",
          "java",
          "bytes")))
  }

  test("test input Type: Date/Time/TimeStamp", JavaStoredProcExclude) {
    val oldTimeZone = TimeZone.getDefault
    val sfTimezone =
      session.sql("SHOW PARAMETERS LIKE 'TIMEZONE' IN SESSION").collect().head.getString(1)
    try {
      // Test with UTC timezone
      session.sql("alter session set TIMEZONE = 'UTC'").collect()
      TimeZone.setDefault(TimeZone.getTimeZone(sfTimezone))

      class UDTFInputTimestampTypes extends UDTF3[Date, Time, Timestamp] {
        override def process(date: Date, time: Time, timestamp: Timestamp): Iterable[Row] = {
          val row = Row(date.toString, time.toString, timestamp.toString)
          Seq(row)
        }
        override def endPartition(): Iterable[Row] = Seq.empty
        override def outputSchema(): StructType =
          StructType(
            StructField("date_str", StringType),
            StructField("time_str", StringType),
            StructField("timestamp_str", StringType))
      }

      createTable(tableName, "date Date, time Time, ts timestamp_ntz")
      runQuery(
        s"insert into $tableName values " +
          s"('2022-01-25', '00:00:00', '2022-01-25 00:00:00.000')," +
          s"('2022-01-25', '12:13:14', '2022-01-25 12:13:14.123')",
        session)

      val tableFunction = session.udtf.registerTemporary(new UDTFInputTimestampTypes)
      val df1 = session.table(tableName).join(tableFunction, col("date"), col("time"), col("ts"))
      assert(
        getSchemaString(df1.schema) ==
          """root
            | |--DATE: Date (nullable = true)
            | |--TIME: Time (nullable = true)
            | |--TS: Timestamp (nullable = true)
            | |--DATE_STR: String (nullable = true)
            | |--TIME_STR: String (nullable = true)
            | |--TIMESTAMP_STR: String (nullable = true)
            |""".stripMargin)
      checkAnswer(
        df1,
        Seq(
          Row(
            Date.valueOf("2022-01-25"),
            Time.valueOf("00:00:00"),
            Timestamp.valueOf("2022-01-25 00:00:00.0"),
            "2022-01-25",
            "00:00:00",
            "2022-01-25 00:00:00.0"),
          Row(
            Date.valueOf("2022-01-25"),
            Time.valueOf("12:13:14"),
            Timestamp.valueOf("2022-01-25 12:13:14.123"),
            "2022-01-25",
            "12:13:14",
            "2022-01-25 12:13:14.123")))
    } finally {
      TimeZone.setDefault(oldTimeZone)
      session.sql(s"alter session set TIMEZONE = '$sfTimezone'").collect()
    }
  }

  test("test input Type: Variant/Array[String|Variant]/Map[String,String|Variant]") {
    val udtf = new UDTF5[
      Variant,
      Array[String],
      Array[Variant],
      mutable.Map[String, String],
      mutable.Map[String, Variant]] {
      override def process(
          v: Variant,
          a1: Array[String],
          a2: Array[Variant],
          m1: mutable.Map[String, String],
          m2: mutable.Map[String, Variant]): Iterable[Row] = {
        val (r1, r2) = (a1.mkString("[", ",", "]"), a2.map(_.asString()).mkString("[", ",", "]"))
        val r3 = m1
          .map { x =>
            s"(${x._1} -> ${x._2})"
          }
          .mkString("{", ",", "}")
        val r4 = m2
          .map { x =>
            s"(${x._1} -> ${x._2.asString()})"
          }
          .mkString("{", ",", "}")
        Seq(Row(v.asString(), r1, r2, r3, r4))
      }

      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType =
        StructType(
          StructField("v_str", StringType),
          StructField("a1_str", StringType),
          StructField("a2_str", StringType),
          StructField("m1_str", StringType),
          StructField("m2_str", StringType))
    }
    val tableFunction = session.udtf.registerTemporary(udtf)
    createTable(tableName, "v variant, a1 array, a2 array, m1 object, m2 object")
    runQuery(
      s"insert into $tableName " +
        s"select to_variant('v1'), array_construct('a1', 'a1'), array_construct('a2', 'a2'), " +
        s"object_construct('m1', 'one'), object_construct('m2', 'two')",
      session)

    val df1 = session
      .table(tableName)
      .join(tableFunction, col("v"), col("a1"), col("a2"), col("m1"), col("m2"))
      .select("v_str", "a1_str", "a2_str", "m1_str", "m2_str")
    assert(
      getSchemaString(df1.schema) ==
        """root
          | |--V_STR: String (nullable = true)
          | |--A1_STR: String (nullable = true)
          | |--A2_STR: String (nullable = true)
          | |--M1_STR: String (nullable = true)
          | |--M2_STR: String (nullable = true)
          |""".stripMargin)
    checkAnswer(df1, Seq(Row("v1", "[a1,a1]", "[a2,a2]", "{(m1 -> one)}", "{(m2 -> two)}")))
  }

  test("return large amount of return columns for UDTF") {
    class ReturnManyColumns(outputColumnCount: Int) extends UDTF1[Int] {
      override def process(data: Int): Iterable[Row] = {
        Seq(Row.fromArray((1 to outputColumnCount).map(_ + data).toArray))
      }

      override def endPartition(): Iterable[Row] =
        Seq(Row.fromArray((1 to outputColumnCount).toArray))

      override def outputSchema(): StructType =
        StructType((1 to outputColumnCount).map(i => StructField(s"c$i", IntegerType)))
    }

    // verify ReturnManyColumns can generate required columns
    val tableFunction3 = session.udtf.registerTemporary(new ReturnManyColumns(3))
    val df3 = session.tableFunction(tableFunction3, lit(10))
    assert(df3.schema.length == 3)
    checkAnswer(df3, Seq(Row(11, 12, 13), Row(1, 2, 3)))

    // Test UDTF return 100 columns
    val tableFunction100 = session.udtf.registerTemporary(new ReturnManyColumns(100))
    val df100 = session.tableFunction(tableFunction100, lit(20))
    assert(df100.schema.length == 100)
    checkAnswer(df100, Seq(Row.fromArray((21 to 120).toArray), Row.fromArray((1 to 100).toArray)))

    // Test UDTF return 200 columns
    val tableFunction200 = session.udtf.registerTemporary(new ReturnManyColumns(200))
    val df200 = session.tableFunction(tableFunction200, lit(100))
    assert(df200.schema.length == 200)
    checkAnswer(df200, Seq(Row.fromArray((101 to 300).toArray), Row.fromArray((1 to 200).toArray)))
  }

  test("test output type: basic types") {
    // NOTE: JAVA UDF doesn't support Byte
    class ReturnBasicTypes extends UDTF1[String] {
      override def process(data: String): Iterable[Row] = {
        val floatValue = data.toInt * 1001 / 1000.0
        val row = Row(
          data.toInt > 0,
          data.toShort,
          data.toInt,
          data.toLong,
          floatValue.toFloat,
          floatValue.toDouble,
          java.math.BigDecimal.valueOf(floatValue).setScale(3, RoundingMode.HALF_DOWN),
          data,
          data.getBytes())
        Seq(row, row)
      }
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType =
        StructType(
          StructField("boolean", BooleanType),
          StructField("short", ShortType),
          StructField("int", IntegerType),
          StructField("long", LongType),
          StructField("float", FloatType),
          StructField("double", DoubleType),
          StructField("decimal", DecimalType(10, 3)),
          StructField("string", StringType),
          StructField("binary", BinaryType))
    }

    val tableFunction = session.udtf.registerTemporary(new ReturnBasicTypes())

    createTable(tableName, "a varchar")
    runQuery(s"insert into $tableName values ('-128'), ('128')", session)
    // Check output columns name and type
    val df1 = session.table(tableName).join(tableFunction, col("a"))
    assert(
      getSchemaString(df1.schema) ==
        """root
          | |--A: String (nullable = true)
          | |--BOOLEAN: Boolean (nullable = true)
          | |--SHORT: Long (nullable = true)
          | |--INT: Long (nullable = true)
          | |--LONG: Long (nullable = true)
          | |--FLOAT: Double (nullable = true)
          | |--DOUBLE: Double (nullable = true)
          | |--DECIMAL: Decimal(10, 3) (nullable = true)
          | |--STRING: String (nullable = true)
          | |--BINARY: Binary (nullable = true)
          |""".stripMargin)
    val b1 = "-128".getBytes()
    checkAnswer(
      df1,
      Seq(
        Row("-128", false, -128, -128, -128, -128.128, -128.128, -128.128, "-128", b1),
        Row("-128", false, -128, -128, -128, -128.128, -128.128, -128.128, "-128", b1),
        Row("128", true, 128, 128, 128, 128.128, 128.128, 128.128, "128", "128".getBytes()),
        Row("128", true, 128, 128, 128, 128.128, 128.128, 128.128, "128", "128".getBytes())))
  }

  test("test output type: Time, Date, Timestamp", JavaStoredProcExclude) {
    val oldTimeZone = TimeZone.getDefault
    val sfTimezone =
      session.sql("SHOW PARAMETERS LIKE 'TIMEZONE' IN SESSION").collect().head.getString(1)
    try {
      // Test with UTC timezone
      session.sql("alter session set TIMEZONE = 'UTC'").collect()
      TimeZone.setDefault(TimeZone.getTimeZone(sfTimezone))

      class ReturnTimestampTypes3 extends UDTF1[String] {
        override def process(data: String): Iterable[Row] = {
          val timestamp = java.sql.Timestamp.valueOf(data)
          val time = java.sql.Time.valueOf(data.split(" ").last)
          val date = java.sql.Date.valueOf(data.split(" ").head)
          Seq(Row(time, date, timestamp), Row(time, date, timestamp))
        }
        override def endPartition(): Iterable[Row] = Seq.empty
        override def outputSchema(): StructType =
          StructType(
            StructField("time", TimeType),
            StructField("date", DateType),
            StructField("timestamp", TimestampType))
      }

      val tableFunction = session.udtf.registerTemporary(new ReturnTimestampTypes3)
      val (date, time) = ("2022-01-25", "12:12:12")
      val ts = s"$date $time"
      createTable(tableName, "a varchar")
      runQuery(s"insert into $tableName values ('$ts')", session)
      val df1 = session.table(tableName).join(tableFunction, col("a"))
      df1.schema.printTreeString()
      assert(
        getSchemaString(df1.schema) ==
          """root
            | |--A: String (nullable = true)
            | |--TIME: Time (nullable = true)
            | |--DATE: Date (nullable = true)
            | |--TIMESTAMP: Timestamp (nullable = true)
            |""".stripMargin)
      checkAnswer(
        df1,
        Seq(
          Row(ts, Time.valueOf(time), Date.valueOf(date), Timestamp.valueOf(ts)),
          Row(ts, Time.valueOf(time), Date.valueOf(date), Timestamp.valueOf(ts))))
    } finally {
      TimeZone.setDefault(oldTimeZone)
      session.sql(s"alter session set TIMEZONE = '$sfTimezone'").collect()
    }
  }

  test(
    "test output type: VariantType/ArrayType(StringType|VariantType)/" +
      "MapType(StringType, StringType|VariantType)") {
    class ReturnComplexTypes extends UDTF1[String] {
      override def process(data: String): Iterable[Row] = {
        val arr = data.split(" ")
        val seq = arr.toSeq
        val stringMap = Map(arr(0) -> arr(1))
        val variantMap = Map(arr(0) -> new Variant(arr(1)))
        Seq(
          Row(data, new Variant(data), arr, arr.map(new Variant(_)), stringMap, variantMap),
          Row(data, new Variant(data), seq, seq.map(new Variant(_)), stringMap, variantMap))
      }
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType =
        StructType(
          StructField("input", StringType),
          StructField("variant", VariantType),
          StructField("string_array", ArrayType(StringType)),
          StructField("variant_array", ArrayType(VariantType)),
          StructField("string_map", MapType(StringType, StringType)),
          StructField("variant_map", MapType(StringType, VariantType)))
    }

    val tableFunction = session.udtf.registerTemporary(new ReturnComplexTypes())
    val df1 = session.tableFunction(tableFunction, lit("v1 v2"))
    assert(
      getSchemaString(df1.schema) ==
        """root
          | |--INPUT: String (nullable = true)
          | |--VARIANT: Variant (nullable = true)
          | |--STRING_ARRAY: Array (nullable = true)
          | |--VARIANT_ARRAY: Array (nullable = true)
          | |--STRING_MAP: Map (nullable = true)
          | |--VARIANT_MAP: Map (nullable = true)
          |""".stripMargin)
    checkAnswer(
      df1,
      Seq(
        Row(
          "v1 v2",
          "\"v1 v2\"",
          "[\n  \"v1\",\n  \"v2\"\n]",
          "[\n  \"v1\",\n  \"v2\"\n]",
          "{\n  \"v1\": \"v2\"\n}",
          "{\n  \"v1\": \"v2\"\n}"),
        Row(
          "v1 v2",
          "\"v1 v2\"",
          "[\n  \"v1\",\n  \"v2\"\n]",
          "[\n  \"v1\",\n  \"v2\"\n]",
          "{\n  \"v1\": \"v2\"\n}",
          "{\n  \"v1\": \"v2\"\n}")))
  }

  test("use UDF and UDTF in one session", JavaStoredProcExclude) {
    class MyWordCount extends UDTF1[String] {
      override def process(s1: String): Iterable[Row] = {
        val wordCounts = mutable.Map[String, Int]()
        s1.split(" ").foreach { x =>
          wordCounts.update(x, wordCounts.getOrElse(x, 0) + 1)
        }
        wordCounts.toArray.map { p =>
          Row(p._1, p._2)
        }
      }
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType =
        StructType(StructField("word", StringType), StructField("count", IntegerType))
    }

    var newSession = Session.builder.configFile(defaultProfile).create
    try {
      // Use UDTF and then UDF
      TestUtils.addDepsToClassPath(newSession)
      val tableFunction = newSession.udtf.registerTemporary(new MyWordCount())
      val df1 = newSession.tableFunction(tableFunction, lit("a a a b b"))
      checkAnswer(df1, Seq(Row("a", 3), Row("b", 2)))
      val udf = newSession.udf.registerTemporary((x: Int) => x + x)
      val df2 = newSession.sql(s"select ${udf.name.get}(10)")
      checkAnswer(df2, Seq(Row(20)))
      newSession.close()

      // Use UDF and then UDTF
      newSession = Session.builder.configFile(defaultProfile).create
      TestUtils.addDepsToClassPath(newSession)
      val udf2 = newSession.udf.registerTemporary((x: Int) => x + x)
      val df4 = newSession.sql(s"select ${udf2.name.get}(20)")
      checkAnswer(df4, Seq(Row(40)))
      val tableFunction2 = newSession.udtf.registerTemporary(new MyWordCount())
      val df3 = newSession.tableFunction(tableFunction2, lit("a a b"))
      checkAnswer(df3, Seq(Row("a", 2), Row("b", 1)))
    } finally {
      newSession.close()
    }
  }

  test("partition by order by") {
    val TableFunc1 = new UDTF1[String] {

      private val freq = new mutable.HashMap[String, Int]

      override def process(colValue: String): Iterable[Row] = {
        val curValue = freq.getOrElse(colValue, 0)
        freq.put(colValue, curValue + 1)
        mutable.Iterable.empty
      }

      override def outputSchema(): StructType =
        StructType(StructField("FREQUENCIES", StringType))

      override def endPartition(): Iterable[Row] = {
        Seq(Row(freq.toString().replace("Hash", "")))
      }
    }
    val df = Seq(("a", "b"), ("a", "c"), ("a", "b"), ("d", "e")).toDF("a", "b")
    val tf = session.udtf.registerTemporary(TableFunc1)
    checkAnswer(
      df.join(tf, Seq(df("b")), Seq(df("a")), Seq(df("b"))),
      Seq(Row("a", null, "Map(b -> 2, c -> 1)"), Row("d", null, "Map(e -> 1)")))

    checkAnswer(
      df.join(tf, Seq(df("b")), Seq(df("a")), Seq.empty),
      Seq(Row("a", null, "Map(b -> 2, c -> 1)"), Row("d", null, "Map(e -> 1)")))
    df.join(tf, Seq(df("b")), Seq.empty, Seq(df("b"))).show()
    df.join(tf, Seq(df("b")), Seq.empty, Seq.empty).show()

    checkAnswer(
      df.join(tf, Map("arg1" -> df("b")), Seq(df("a")), Seq(df("b"))),
      Seq(Row("a", null, "Map(b -> 2, c -> 1)"), Row("d", null, "Map(e -> 1)")))

    checkAnswer(
      df.join(tf(Map("arg1" -> df("b"))), Seq(df("a")), Seq(df("b"))),
      Seq(Row("a", null, "Map(b -> 2, c -> 1)"), Row("d", null, "Map(e -> 1)")))

    checkAnswer(
      df.join(tf, Map("arg1" -> df("b")), Seq(df("a")), Seq.empty),
      Seq(Row("a", null, "Map(b -> 2, c -> 1)"), Row("d", null, "Map(e -> 1)")))
    df.join(tf, Map("arg1" -> df("b")), Seq.empty, Seq(df("b"))).show()
    df.join(tf, Map("arg1" -> df("b")), Seq.empty, Seq.empty).show()
  }

  test("single partition") {
    val TableFunc1 = new UDTF1[String] {

      private val freq = new mutable.HashMap[String, Int]

      override def process(colValue: String): Iterable[Row] = {
        val curValue = freq.getOrElse(colValue, 0)
        freq.put(colValue, curValue + 1)
        mutable.Iterable.empty
      }

      override def outputSchema(): StructType =
        StructType(StructField("FREQUENCIES", StringType))

      override def endPartition(): Iterable[Row] = {
        Seq(Row(freq.toString()))
      }
    }
    val df = Seq(("a", "b"), ("a", "c"), ("a", "b"), ("d", "e")).toDF("a", "b")
    val tf = session.udtf.registerTemporary(TableFunc1)
    val result1 = df.join(tf, Seq(df("b")), Seq(lit(1)), Seq.empty).collect()
    assert(result1.length == 1)
    assert(result1.head.get(0) == null)
    assert(result1.head.get(1) == null)
    assert(result1.head.getString(2).contains("e -> 1"))
    assert(result1.head.getString(2).contains("c -> 1"))
    assert(result1.head.getString(2).contains("b -> 2"))
    val result2 = df.join(tf, Seq(df("b")), Seq(lit("a")), Seq.empty).collect()
    assert(result2.length == 1)
    assert(result2.head.get(0) == null)
    assert(result2.head.get(1) == null)
    assert(result2.head.getString(2).contains("e -> 1"))
    assert(result2.head.getString(2).contains("c -> 1"))
    assert(result2.head.getString(2).contains("b -> 2"))
  }
}
