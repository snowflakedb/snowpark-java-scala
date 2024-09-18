package com.snowflake.snowpark_test

import com.snowflake.snowpark.functions._
import com.snowflake.snowpark._

trait DataFrameJoinSuite extends SNTestBase {
  import session.implicits._
  val tableName: String = randomName()
  val tableName1: String = randomName()
  val tableName2: String = randomName()

  lazy val disableHideInternalAliasSession: Session =
    Session.builder
      .configFile(defaultProfile)
      .config("SNOWPARK_HIDE_INTERNAL_ALIAS", "false")
      .create

  override def afterAll(): Unit = {
    dropTable(tableName)
    dropTable(tableName1)
    dropTable(tableName2)
    super.afterAll()
  }

  test("join - join using") {
    val df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")
    val df2 = Seq(1, 2, 3).map(i => (i, (i + 1).toString)).toDF("int", "str")

    checkAnswer(
      df.join(df2, "int"),
      Row(1, "1", "2") :: Row(2, "2", "3") :: Row(3, "3", "4") :: Nil)
  }

  test("join - join using multiple columns") {
    val df = Seq(1, 2, 3).map(i => (i, i + 1, i.toString)).toDF("int", "int2", "str")
    val df2 = Seq(1, 2, 3).map(i => (i, i + 1, (i + 1).toString)).toDF("int", "int2", "str")

    checkAnswer(
      df.join(df2, Seq("int", "int2")),
      Row(1, 2, "1", "2") :: Row(2, 3, "2", "3") :: Row(3, 4, "3", "4") :: Nil)
  }

  test("Full outer join followed by inner join") {
    val a = Seq((1, 2), (2, 3)).toDF("a", "b")
    val b = Seq((2, 5), (3, 4)).toDF("a", "c")
    val c = Seq((3, 1)).toDF("a", "d")
    val ab = a.join(b, Seq("a"), "fullouter")
    checkAnswer(ab.join(c, "a"), Row(3, null, 4, 1) :: Nil)
  }

  test("Test limit with join") {
    val df = Seq((1, 1, "1"), (2, 2, "3")).toDF("int", "int2", "str")
    val df2 = Seq((1, 1, "1"), (2, 3, "5")).toDF("int", "int2", "str")
    val limit = 1310721
    val innerJoin = df
      .limit(limit)
      .join(df2.limit(limit), Seq("int", "int2"), "inner")
      .agg(count($"int"))
    checkAnswer(innerJoin, Row(1) :: Nil)
  }
  test("default inner join") {
    val df = Seq(1, 2).toDF("a")
    val df2 = Seq(1, 2).map(i => (i, s"test$i")).toDF("a", "b")

    checkAnswer(
      df.join(df2),
      Seq(Row(1, 1, "test1"), Row(1, 2, "test2"), Row(2, 1, "test1"), Row(2, 2, "test2")))
  }

  test("default inner join with using column") {
    val df = Seq(1, 2).toDF("a")
    val df2 = Seq(1, 2).map(i => (i, s"test$i")).toDF("a", "b")

    checkAnswer(df.join(df2, "a"), Seq(Row(1, "test1"), Row(2, "test2")))
    checkAnswer(df.join(df2, "a").filter($"a" > 1), Seq(Row(2, "test2")))
  }

  test("3 way joins") {
    val df1 = Seq(1, 2).toDF("a")
    val df2 = Seq(1, 2).map(i => (i, s"test$i")).toDF("a", "b")
    val df3 = Seq(1, 2).map(i => (s"test$i", s"hello$i")).toDF("key", "val")
    // 3 way join with column renaming
    val res = df1.join(df2, "a").toDF("num", "key").join(df3, "key")
    checkAnswer(res, Seq(Row("test1", 1, "hello1"), Row("test2", 2, "hello2")))
  }

  test("default inner join with join conditions") {
    val df = Seq(1, 2).map(i => (i, s"test$i")).toDF("a", "b")
    val df2 = Seq(1, 2).map(i => (i, s"num$i")).toDF("num", "val")
    checkAnswer(
      df.join(df2, df("a") === df2("num")),
      Seq(Row(1, "test1", 1, "num1"), Row(2, "test2", 2, "num2")))
  }

  test("join with multiple conditions") {
    val df = Seq(1, 2).map(i => (i, s"test$i")).toDF("a", "b")
    val df2 = Seq(1, 2).map(i => (i, s"num$i")).toDF("num", "val")

    checkAnswer(df.join(df2, df("a") === df2("num") && df("b") === df2("val")), Seq.empty)
  }

  test("join with ambigious column in condition") {
    val df = Seq(1, 2).toDF("a")
    val df2 = Seq(1, 2).map(i => (i, s"test$i")).toDF("a", "b")
    val ex = intercept[SnowparkClientException] {
      df.join(df2, 'a === 'a).collect()
    }
    assert(ex.getMessage.contains("The reference to the column 'A' is ambiguous."))
  }

  test("join - join using multiple columns and specifying join type") {
    createTable(tableName1, "int int, int2 int, str string")
    runQuery(s"insert into $tableName1 values(1, 2, '1'),(3, 4, '3')", session)
    createTable(tableName2, "int int, int2 int, str string")
    runQuery(s"insert into $tableName2 values(1, 3, '1'),(5, 6, '5')", session)

    val df = session.table(tableName1)
    val df2 = session.table(tableName2)

    checkAnswer(df.join(df2, Seq("int", "str"), "inner"), Seq(Row(1, "1", 2, 3)))

    checkAnswer(
      df.join(df2, Seq("int", "str"), "left"),
      Seq(Row(1, "1", 2, 3), Row(3, "3", 4, null)))

    checkAnswer(
      df.join(df2, Seq("int", "str"), "right"),
      Seq(Row(1, "1", 2, 3), Row(5, "5", null, 6)))

    checkAnswer(
      df.join(df2, Seq("int", "str"), "outer"),
      Seq(Row(1, "1", 2, 3), Row(3, "3", 4, null), Row(5, "5", null, 6)))

    checkAnswer(df.join(df2, Seq("int", "str"), "left_semi"), Seq(Row(1, 2, "1")))

    checkAnswer(df.join(df2, Seq("int", "str"), "semi"), Seq(Row(1, 2, "1")))

    checkAnswer(df.join(df2, Seq("int", "str"), "left_anti"), Seq(Row(3, 4, "3")))

    checkAnswer(df.join(df2, Seq("int", "str"), "anti"), Seq(Row(3, 4, "3")))
  }

  test("join - join using conditions and specifying join type") {
    createTable(tableName1, "a1 int, b1 int, str1 string")
    runQuery(s"insert into $tableName1 values(1, 2, '1'),(3, 4, '3')", session)
    createTable(tableName2, "a2 int, b2 int, str2 string")
    runQuery(s"insert into $tableName2 values(1, 3, '1'),(5, 6, '5')", session)

    val df = session.table(tableName1)
    val df2 = session.table(tableName2)

    val joinCond = df("a1") === df2("a2") && df("str1") === df2("str2")

    checkAnswer(df.join(df2, joinCond, "left_semi"), Seq(Row(1, 2, "1")))

    checkAnswer(df.join(df2, joinCond, "semi"), Seq(Row(1, 2, "1")))

    checkAnswer(df.join(df2, joinCond, "left_anti"), Seq(Row(3, 4, "3")))

    checkAnswer(df.join(df2, joinCond, "anti"), Seq(Row(3, 4, "3")))
  }

  test("natural join") {
    val df = Seq(1, 2).toDF("a")
    val df2 = Seq(1, 2).map(i => (i, s"test$i")).toDF("a", "b")
    checkAnswer(df.naturalJoin(df2), Seq(Row(1, "test1"), Row(2, "test2")))
  }

  test("natural outer join") {
    val df1 = Seq((1, "1"), (3, "3")).toDF("a", "b")
    val df2 = Seq((1, "1"), (4, "4")).toDF("a", "c")

    checkAnswer(df1.naturalJoin(df2, "left"), Seq(Row(1, "1", "1"), Row(3, "3", null)))

    checkAnswer(df1.naturalJoin(df2, "right"), Seq(Row(1, "1", "1"), Row(4, null, "4")))

    checkAnswer(
      df1.naturalJoin(df2, "outer"),
      Seq(Row(1, "1", "1"), Row(3, "3", null), Row(4, null, "4")))
  }

  test("join - cross join") {
    val df1 = Seq((1, "1"), (3, "3")).toDF("int", "str")
    val df2 = Seq((2, "2"), (4, "4")).toDF("int", "str")

    checkAnswer(
      df1.crossJoin(df2),
      Row(1, "1", 2, "2") :: Row(1, "1", 4, "4") ::
        Row(3, "3", 2, "2") :: Row(3, "3", 4, "4") :: Nil)

    checkAnswer(
      df2.crossJoin(df1),
      Row(2, "2", 1, "1") :: Row(2, "2", 3, "3") ::
        Row(4, "4", 1, "1") :: Row(4, "4", 3, "3") :: Nil)
  }

  test("join -- ambiguous columns with specified sources") {
    val df = Seq(1, 2).toDF("a")
    val df2 = Seq(1, 2).map(i => (i, s"test$i")).toDF("a", "b")
    checkAnswer(df.join(df2, df("a") === df2("a")), Row(1, 1, "test1") :: Row(2, 2, "test2") :: Nil)
    checkAnswer(
      df.join(df2, df("a") === df2("a")).select(df("a") * df2("a"), 'b),
      Row(1, "test1") :: Row(4, "test2") :: Nil)
  }

  test("join -- ambiguous columns without specified sources") {
    val df = Seq((1, "one"), (2, "two")).toDF("intcol", "stringcol")
    val df2 = Seq((1, "one"), (3, "three")).toDF("intcol", "bcol")

    Seq("inner", "leftouter", "rightouter", "full_outer").foreach { joinType =>
      val ex = intercept[SnowparkClientException] {
        df.join(df2, 'intcol === 'intcol, joinType).collect()
      }
      assert(ex.getMessage.contains("ambiguous"))
      assert(ex.getMessage.contains("INTCOL"))

      val ex2 = intercept[SnowparkClientException] {
        df.join(df2, df("intcol") === df2("intcol"), joinType).select('intcol).collect()
      }
      assert(ex2.getMessage.contains("INTCOL"))
      assert(ex2.getMessage().contains("ambiguous"))
    }
  }

  test("join -- expressions on ambiguous columns") {
    val lhs = Seq((1, -1, "one"), (2, -2, "two")).toDF("intcol", "negcol", "lhscol")
    val rhs = Seq((1, -10, "one"), (2, -20, "two")).toDF("intcol", "negcol", "rhscol")
    checkAnswer(
      lhs
        .join(rhs, lhs("intcol") === rhs("intcol"))
        .select(lhs("intcol") + rhs("intcol"), lhs("negcol"), rhs("negcol"), 'lhscol, 'rhscol),
      Row(2, -1, -10, "one", "one") :: Row(4, -2, -20, "two", "two") :: Nil)
  }

  test("Semi joins with absent columns") {
    val lhs = Seq((1, -1, "one"), (2, -2, "two")).toDF("intcol", "negcol", "lhscol")
    val rhs = Seq((1, -10, "one"), (2, -20, "two")).toDF("intcol", "negcol", "rhscol")
    var ex = intercept[SnowparkClientException] {
      lhs.join(rhs, lhs("intcol") === rhs("intcol"), "leftsemi").select(rhs("intcol")).collect()
    }
    assert(ex.getMessage() contains """The column specified in df("INTCOL")""")
    assert(ex.getMessage() contains "not present")

    ex = intercept[SnowparkClientException] {
      lhs.join(rhs, lhs("intcol") === rhs("intcol"), "leftanti").select(rhs("intcol")).collect()
    }
    assert(ex.getMessage() contains """The column specified in df("INTCOL")""")
    assert(ex.getMessage() contains "not present")
  }

  test("Semi joins with columns from LHS") {
    val lhs = Seq((1, -1, "one"), (2, -2, "two")).toDF("intcol", "negcol", "lhscol")
    val rhs = Seq((1, -10, "one"), (2, -20, "two")).toDF("intcol", "negcol", "rhscol")

    checkAnswer(
      lhs.join(rhs, lhs("intcol") === rhs("intcol"), "leftsemi").select('intcol),
      Row(1) :: Row(2) :: Nil)

    checkAnswer(
      lhs.join(rhs, lhs("intcol") === rhs("intcol"), "leftsemi").select(lhs("intcol")),
      Row(1) :: Row(2) :: Nil)

    checkAnswer(
      lhs
        .join(rhs, lhs("intcol") === rhs("intcol") && lhs("negcol") === rhs("negcol"), "leftsemi")
        .select(lhs("intcol")),
      Nil)

    checkAnswer(lhs.join(rhs, lhs("intcol") === rhs("intcol"), "leftanti").select('intcol), Nil)

    checkAnswer(
      lhs.join(rhs, lhs("intcol") === rhs("intcol"), "leftanti").select(lhs("intcol")),
      Nil)

    checkAnswer(
      lhs
        .join(rhs, lhs("intcol") === rhs("intcol") && lhs("negcol") === rhs("negcol"), "leftanti")
        .select(lhs("intcol")),
      Row(1) :: Row(2) :: Nil)
  }

  test("Using joins") {
    val lhs = Seq((1, -1, "one"), (2, -2, "two")).toDF("intcol", "negcol", "lhscol")
    val rhs = Seq((1, -10, "one"), (2, -20, "two")).toDF("intcol", "negcol", "rhscol")

    Seq("inner", "leftouter", "rightouter", "full_outer").foreach { joinType =>
      checkAnswer(
        lhs.join(rhs, Seq("intcol"), joinType).select("*"),
        Row(1, -1, "one", -10, "one") :: Row(2, -2, "two", -20, "two") :: Nil)
      checkAnswer(
        lhs.join(rhs, Seq("intcol"), joinType),
        Row(1, -1, "one", -10, "one") :: Row(2, -2, "two", -20, "two") :: Nil)

      val ex2 = intercept[SnowparkClientException] {
        lhs.join(rhs, Seq("intcol"), joinType).select('negcol).collect()
      }
      assert(ex2.getMessage.contains("NEGCOL"))
      assert(ex2.getMessage().contains("ambiguous"))

      checkAnswer(lhs.join(rhs, Seq("intcol"), joinType).select('intcol), Row(1) :: Row(2) :: Nil)
      checkAnswer(
        lhs.join(rhs, Seq("intcol"), joinType).select(lhs("negcol"), rhs("negcol")),
        Row(-1, -10) :: Row(-2, -20) :: Nil)
    }

  }

  test("Columns with and without quotes") {
    val lhs = Seq((1, 1.0)).toDF("intcol", "doublecol")
    val rhs = Seq((1, 2.0)).toDF(""""INTCOL"""", """"DoubleCol"""")
    checkAnswer(
      lhs
        .join(rhs, lhs("intcol") === rhs("intcol"))
        .select(lhs(""""INTCOL""""), rhs("intcol"), 'doublecol, col(""""DoubleCol"""")),
      Row(1, 1, 1.0, 2.0) :: Nil)

    checkAnswer(
      lhs
        .join(rhs, lhs("doublecol") === rhs("\"DoubleCol\""))
        .select(lhs(""""INTCOL""""), rhs("intcol"), 'doublecol, rhs(""""DoubleCol"""")),
      Nil)

    // Below LHS and RHS are swapped but we still default to using the column name as is.
    checkAnswer(
      lhs
        .join(rhs, col("doublecol") === col("\"DoubleCol\""))
        .select(lhs(""""INTCOL""""), rhs("intcol"), 'doublecol, col(""""DoubleCol"""")),
      Nil)

    var ex = intercept[SnowparkClientException] {
      lhs.join(rhs, col("intcol") === rhs(""""INTCOL"""")).collect()
    }
    assert(ex.getMessage() contains "INTCOL")
    assert(ex.getMessage() contains "ambiguous")
  }

  test("Aliases multiple levels deep") {
    val lhs = Seq((1, -1, "one"), (2, -2, "two")).toDF("intcol", "negcol", "lhscol")
    val rhs = Seq((1, -10, "one"), (2, -20, "two")).toDF("intcol", "negcol", "rhscol")
    checkAnswer(
      lhs
        .join(rhs, lhs("intcol") === rhs("intcol"))
        .select((lhs("negcol") + rhs("negcol")) as "newCol", lhs("intcol"), rhs("intcol"))
        .select(lhs("intcol") + rhs("intcol"), 'newCol),
      Row(2, -11) :: Row(4, -22) :: Nil)
  }

  test("join - sql as the backing dataframe") {
    createTable(tableName1, "int int, int2 int, str string")
    runQuery(s"insert into $tableName1 values(1, 2, '1'),(3, 4, '3')", session)

    val df = session.sql(s"select * from $tableName1 where int2 < 10")
    val df2 = session.sql(
      s"select 1 as INT, 3 as INT2, '1' as STR "
        + "UNION select 5 as INT, 6 as INT2, '5' as STR")

    checkAnswer(df.join(df2, Seq("int", "str"), "inner"), Seq(Row(1, "1", 2, 3)))

    checkAnswer(
      df.join(df2, Seq("int", "str"), "left"),
      Seq(Row(1, "1", 2, 3), Row(3, "3", 4, null)))

    checkAnswer(
      df.join(df2, Seq("int", "str"), "right"),
      Seq(Row(1, "1", 2, 3), Row(5, "5", null, 6)))

    checkAnswer(
      df.join(df2, Seq("int", "str"), "outer"),
      Seq(Row(1, "1", 2, 3), Row(3, "3", 4, null), Row(5, "5", null, 6)))
    val res = df.join(df2, Seq("int", "str"), "left_semi").collect
    checkAnswer(df.join(df2, Seq("int", "str"), "left_semi"), Seq(Row(1, 2, "1")))

    checkAnswer(df.join(df2, Seq("int", "str"), "semi"), Seq(Row(1, 2, "1")))

    checkAnswer(df.join(df2, Seq("int", "str"), "left_anti"), Seq(Row(3, 4, "3")))

    checkAnswer(df.join(df2, Seq("int", "str"), "anti"), Seq(Row(3, 4, "3")))
  }

  test("negative test for self join with conditions") {
    createTable(tableName, s"c1 int, c2 int")
    runQuery(s"insert into $tableName values(1, 2),(2, 3)", session)
    val df = session.table(tableName)
    val joinTypes = Seq("", "inner", "left", "right", "outer")
    val selfDataFrames = Seq(df, createDataFrame(df))
    selfDataFrames.foreach { df2 =>
      joinTypes.foreach { joinType =>
        val ex = intercept[SnowparkClientException] {
          if (joinType.isEmpty) {
            df.join(df2, df("c1") === df2("c2")).collect()
          } else {
            df.join(df2, df("c1") === df2("c2"), joinType).collect()
          }
        }
        val msg = s"You cannot join a DataFrame with itself because the column references " +
          s"cannot be resolved correctly. Instead, call clone() to create a copy of " +
          s"the DataFrame, and join the DataFrame with this copy."
        assert(ex.message.contains(msg))
      }
    }
  }

  test("clone can help these self joins") {
    createTable(tableName, s"c1 int, c2 int")
    runQuery(s"insert into $tableName values(1, 2),(2, 3)", session)
    val df = session.table(tableName)
    val clonedDF = df.clone

    // "inner" self join
    checkAnswer(df.join(clonedDF, df("c1") === clonedDF("c2")), Seq(Row(2, 3, 1, 2)))

    // "left" self join
    checkAnswer(
      df.join(clonedDF, df("c1") === clonedDF("c2"), "left"),
      Seq(Row(1, 2, null, null), Row(2, 3, 1, 2)))

    // "right" self join
    checkAnswer(
      df.join(clonedDF, df("c1") === clonedDF("c2"), "right"),
      Seq(Row(2, 3, 1, 2), Row(null, null, 2, 3)))

    // "outer" self join
    checkAnswer(
      df.join(clonedDF, df("c1") === clonedDF("c2"), "outer"),
      Seq(Row(2, 3, 1, 2), Row(null, null, 2, 3), Row(1, 2, null, null)),
      false)
  }

  test("test natural/cross join") {
    createTable(tableName, s"c1 int, c2 int")
    runQuery(s"insert into $tableName values(1, 2),(2, 3)", session)
    val df = session.table(tableName)
    val df2 = df // Another reference of "df"
    val clonedDF = df.clone

    // "natural join" supports self join.
    checkAnswer(df.naturalJoin(df2), Seq(Row(1, 2), Row(2, 3)))
    checkAnswer(df.naturalJoin(clonedDF), Seq(Row(1, 2), Row(2, 3)))

    // "cross join" supports self join.
    checkAnswer(
      df.crossJoin(df2),
      Seq(Row(1, 2, 1, 2), Row(1, 2, 2, 3), Row(2, 3, 1, 2), Row(2, 3, 2, 3)))
    checkAnswer(
      df.crossJoin(clonedDF),
      Seq(Row(1, 2, 1, 2), Row(1, 2, 2, 3), Row(2, 3, 1, 2), Row(2, 3, 2, 3)))
  }

  test("clone with join DataFrame") {
    createTable(tableName, s"c1 int, c2 int")
    runQuery(s"insert into $tableName values(1, 2),(2, 3)", session)
    val df = session.table(tableName)
    checkAnswer(df, Seq(Row(1, 2), Row(2, 3)))
    val clonedDF = df.clone
    // Cloned DF has the same conent with original DF
    checkAnswer(clonedDF, Seq(Row(1, 2), Row(2, 3)))

    val joinDF = df.join(clonedDF, df("c1") === clonedDF("c2"))
    checkAnswer(joinDF, Seq(Row(2, 3, 1, 2)))
    // cloned join DF
    val clonedJoinDF = joinDF.clone
    checkAnswer(clonedJoinDF, Seq(Row(2, 3, 1, 2)))
  }

  test("test join of join", JavaStoredProcExclude) {
    createTable(tableName, s"c1 int, c2 int")
    runQuery(s"insert into $tableName values(1, 1),(2, 2)", session)
    val dfL = disableHideInternalAliasSession.table(tableName)
    val dfR = dfL.clone
    val dfJ = dfL.join(dfR, dfL("c1") === dfR("c1"))
    checkAnswer(dfJ, Seq(Row(1, 1, 1, 1), Row(2, 2, 2, 2)))

    val dfJClone = dfJ.clone()
    // Because of duplicate column name rename, we have to get a name.
    val colName = dfJ.schema.fields.head.name
    val dfJJ = dfJ.join(dfJClone, dfJ(colName) === dfJClone(colName))
    checkAnswer(dfJJ, Seq(Row(1, 1, 1, 1, 1, 1, 1, 1), Row(2, 2, 2, 2, 2, 2, 2, 2)))
  }

  test("negative test join of join") {
    createTable(tableName, s"c1 int, c2 int")
    runQuery(s"insert into $tableName values(1, 1),(2, 2)", session)
    val dfL = session.table(tableName)
    val dfR = dfL.clone
    val dfJ = dfL.join(dfR, dfL("c1") === dfR("c1"))
    val dfJClone = dfJ.clone()
    val ex =
      intercept[SnowparkClientException](dfJ.join(dfJClone, dfL("c1") === dfR("c1")).collect())
    assert(ex.getMessage.contains("The reference to the column 'C1' is ambiguous."))
  }

  ignore("Semi joins with ambiguous columns") {
    val lhs = Seq((1, -1, "one"), (2, -2, "two")).toDF("intcol", "negcol", "lhscol")
    val rhs = Seq((1, -10, "one"), (2, -20, "two")).toDF("intcol", "negcol", "rhscol")
    // This will just provide a warning that the predicate is trivial, not an exception.
    val ex = intercept[SnowparkClientException] {
      lhs.join(rhs, 'intcol === 'intcol, "leftsemi").collect()
    }
    assert(ex.getMessage() contains "The reference to the column 'INTCOL' is ambiguous.")
    assert(ex.getMessage() contains """<df>("INTCOL")""")
  }

  test("drop on join") {
    val tableName1 = randomName()
    val tableName2 = randomName()
    try {
      Seq((1, "a", true), (2, "b", false)).toDF("a", "b", "c").write.saveAsTable(tableName1)
      Seq((3, "c", true), (4, "d", false)).toDF("a", "b", "c").write.saveAsTable(tableName2)
      val df1 = session.table(tableName1)
      val df2 = session.table(tableName2)
      val df3 = df1.join(df2, df1("c") === df2("c")).drop(df1("a"), df2("b"), df1("c"))
      checkAnswer(df3, Seq(Row("a", 3, true), Row("b", 4, false)))
      val df4 = df3.drop(df2("c"), df1("b"), col("other"))
      checkAnswer(df4, Seq(Row(3), Row(4)))
    } finally {
      dropTable(tableName1)
      dropTable(tableName2)
    }
  }

  test("drop on self join") {
    val tableName1 = randomName()
    try {
      Seq((1, "a", true), (2, "b", false)).toDF("a", "b", "c").write.saveAsTable(tableName1)
      val df1 = session.table(tableName1)
      val df2 = df1.clone
      val df3 = df1.join(df2, df1("c") === df2("c")).drop(df1("a"), df2("b"), df1("c"))
      checkAnswer(df3, Seq(Row("a", 1, true), Row("b", 2, false)))
      val df4 = df3.drop(df2("c"), df1("b"), col("other"))
      checkAnswer(df4, Seq(Row(1), Row(2)))
    } finally {
      dropTable(tableName1)
    }
  }

  test("withColumn on join") {
    val tableName1 = randomName()
    val tableName2 = randomName()
    try {
      Seq((1, "a", true), (2, "b", false)).toDF("a", "b", "c").write.saveAsTable(tableName1)
      Seq((3, "c", true), (4, "d", false)).toDF("a", "b", "c").write.saveAsTable(tableName2)
      val df1 = session.table(tableName1)
      val df2 = session.table(tableName2)
      checkAnswer(
        df1
          .join(df2, df1("c") === df2("c"))
          .drop(df1("b"), df2("b"), df1("c"))
          .withColumn("newColumn", df1("a") + df2("a")),
        Seq(Row(1, 3, true, 4), Row(2, 4, false, 6)))
    } finally {
      dropTable(tableName1)
      dropTable(tableName2)
    }
  }

  test("process outer join results using the non-nullable columns in the join input") {
    // Filter data using a non-nullable column from a right table
    val df1 = Seq((0, 0), (1, 0), (2, 0), (3, 0), (4, 0)).toDF("id", "count")
    val df2 = Seq(Tuple1(0), Tuple1(1)).toDF("id").groupBy("id").count
    checkAnswer(
      df1.join(df2, df1("id") === df2("id"), "left_outer").filter(is_null(df2("count"))),
      Row(2, 0, null, null) ::
        Row(3, 0, null, null) ::
        Row(4, 0, null, null) :: Nil)

    // Coalesce data using non-nullable columns in input tables
    val df3 = Seq((1, 1)).toDF("a", "b")
    val df4 = Seq((2, 2)).toDF("a", "b")
    checkAnswer(
      df3
        .join(df4, df3("a") === df4("a"), "outer")
        .select(coalesce(df3("a"), df3("b")), coalesce(df4("a"), df4("b"))),
      Row(1, null) :: Row(null, 2) :: Nil)
  }

  test("SN: join - outer join conversion") {
    val df = Seq((1, 2, "1"), (3, 4, "3")).toDF("int", "int2", "str")
    val df2 = Seq((1, 3, "1"), (5, 6, "5")).toDF("int", "int2", "str")

    // outer -> left
    val outerJoin2Left = df.join(df2, df("int") === df2("int"), "outer").where(df("int") >= 3)

    checkAnswer(outerJoin2Left, Row(3, 4, "3", null, null, null) :: Nil)

    // outer -> right
    val outerJoin2Right = df.join(df2, df("int") === df2("int"), "outer").where(df2("int") >= 3)

    checkAnswer(outerJoin2Right, Row(null, null, null, 5, 6, "5") :: Nil)

    // outer -> inner
    val outerJoin2Inner =
      df.join(df2, df("int") === df2("int"), "outer").where(df("int") === 1 && df2("int2") === 3)

    checkAnswer(outerJoin2Inner, Row(1, 2, "1", 1, 3, "1") :: Nil)

    // right -> inner
    val rightJoin2Inner = df.join(df2, df("int") === df2("int"), "right").where(df("int") > 0)

    checkAnswer(rightJoin2Inner, Row(1, 2, "1", 1, 3, "1") :: Nil)

    // left -> inner
    val leftJoin2Inner = df.join(df2, df("int") === df2("int"), "left").where(df2("int") > 0)

    checkAnswer(leftJoin2Inner, Row(1, 2, "1", 1, 3, "1") :: Nil)
  }

  test(
    "Don't throw Analysis Exception in CheckCartesianProduct when join condition " +
      "is false or null") {
    val df = session.range(10).toDF("id")
    val dfNull = session.range(10).select(lit(null).as("b"))
    df.join(dfNull, $"id" === $"b", "left").collect()

    val dfOne = df.select(lit(1).as("a"))
    val dfTwo = session.range(10).select(lit(2).as("b"))
    dfOne.join(dfTwo, $"a" === $"b", "left").collect()
  }

  test("name alias in multiple join") {
    val tableTrips = randomName()
    val tableStations = randomName()
    try {
      runQuery(
        s"create or replace table $tableTrips " +
          "(starttime timestamp, start_station_id int, end_station_id int)",
        session)
      runQuery(
        s"create or replace table $tableStations " +
          "(station_id int, station_name string)",
        session)

      val df_trips = session.table(tableTrips)
      val df_start_stations = session.table(tableStations)
      val df_end_stations = session.table(tableStations)

      // assert no error
      df_trips
        .join(df_end_stations, df_trips("end_station_id") === df_end_stations("station_id"))
        .join(df_start_stations, df_trips("start_station_id") === df_start_stations("station_id"))
        .filter(df_trips("starttime") >= "2021-01-01")
        .select(
          df_start_stations("station_name"),
          df_end_stations("station_name"),
          df_trips("starttime"))
        .collect()

    } finally {
      dropTable(tableTrips)
      dropTable(tableStations)
    }
  }

  test("name alias in multiple join, un-normalized name") {
    val tableTrips = randomName()
    val tableStations = randomName()
    try {
      runQuery(
        s"create or replace table $tableTrips " +
          "(starttime timestamp, \"start<station>id\" int, \"end+station+id\" int)",
        session)
      runQuery(
        s"create or replace table $tableStations " +
          "(\"station^id\" int, \"station%name\" string)",
        session)

      val df_trips = session.table(tableTrips)
      val df_start_stations = session.table(tableStations)
      val df_end_stations = session.table(tableStations)

      // assert no error
      df_trips
        .join(df_end_stations, df_trips("end+station+id") === df_end_stations("station^id"))
        .join(df_start_stations, df_trips("start<station>id") === df_start_stations("station^id"))
        .filter(df_trips("starttime") >= "2021-01-01")
        .select(
          df_start_stations("station%name"),
          df_end_stations("station%name"),
          df_trips("starttime"))
        .collect()

    } finally {
      dropTable(tableTrips)
      dropTable(tableStations)
    }
  }

  test("report error when refer common col") {
    val df1 = Seq((1, 2)).toDF("a", "b")
    val df2 = Seq((1, 2)).toDF("c", "d")
    val df3 = Seq((1, 2)).toDF("e", "f")

    val df4 = df1.join(df2, df1("a") === df2("c"))
    val df5 = df3.join(df2, df2("c") === df3("e"))

    val df6 = df4.join(df5, df4("a") === df5("e"))

    val ex = intercept[SnowparkClientException](df6.select("*").select(df2("c")).collect())
    assert(ex.message.contains("The reference to the column 'C' is ambiguous."))
  }

  test("select all on join result") {
    val dfLeft = Seq((1, 2)).toDF("a", "b")
    val dfRight = Seq((3, 4)).toDF("c", "d")

    val df = dfLeft.join(dfRight)
    // Select all: *, df("*"), dfLeft(*) + dfRight(*), dfRight(*) + dfLeft(*)
    assert(
      getShowString(df.select("*"), 10) ==
        """-------------------------
          ||"A"  |"B"  |"C"  |"D"  |
          |-------------------------
          ||1    |2    |3    |4    |
          |-------------------------
          |""".stripMargin)
    assert(
      getShowString(df.select(df("*")), 10) ==
        """-------------------------
          ||"A"  |"B"  |"C"  |"D"  |
          |-------------------------
          ||1    |2    |3    |4    |
          |-------------------------
          |""".stripMargin)
    assert(
      getShowString(df.select(dfLeft("*"), dfRight("*")), 10) ==
        """-------------------------
          ||"A"  |"B"  |"C"  |"D"  |
          |-------------------------
          ||1    |2    |3    |4    |
          |-------------------------
          |""".stripMargin)
    assert(
      getShowString(df.select(dfRight("*"), dfLeft("*")), 10) ==
        """-------------------------
          ||"C"  |"D"  |"A"  |"B"  |
          |-------------------------
          ||3    |4    |1    |2    |
          |-------------------------
          |""".stripMargin)
  }

  test("select left/right on join result") {
    val dfLeft = Seq((1, 2)).toDF("a", "b")
    val dfRight = Seq((3, 4)).toDF("c", "d")

    val df = dfLeft.join(dfRight)
    // Select left or right
    assert(
      getShowString(df.select(dfLeft("*")), 10) ==
        """-------------
          ||"A"  |"B"  |
          |-------------
          ||1    |2    |
          |-------------
          |""".stripMargin)
    assert(
      getShowString(df.select(dfRight("*")), 10) ==
        """-------------
          ||"C"  |"D"  |
          |-------------
          ||3    |4    |
          |-------------
          |""".stripMargin)
  }

  test("select left/right combination on join result") {
    val dfLeft = Seq((1, 2)).toDF("a", "b")
    val dfRight = Seq((3, 4)).toDF("c", "d")

    val df = dfLeft.join(dfRight)
    // Select left(*) and right("c")
    assert(
      getShowString(df.select(dfLeft("*"), dfRight("c")), 10) ==
        """-------------------
          ||"A"  |"B"  |"C"  |
          |-------------------
          ||1    |2    |3    |
          |-------------------
          |""".stripMargin)
    // Select left(*) and left("a")
    assert(
      getShowString(df.select(dfLeft("*"), dfLeft("a").as("l_a")), 10) ==
        """---------------------
          ||"A"  |"B"  |"L_A"  |
          |---------------------
          ||1    |2    |1      |
          |---------------------
          |""".stripMargin)
    // Select right(*) and right("c")
    assert(
      getShowString(df.select(dfRight("*"), dfRight("c").as("R_C")), 10) ==
        """---------------------
          ||"C"  |"D"  |"R_C"  |
          |---------------------
          ||3    |4    |3      |
          |---------------------
          |""".stripMargin)
    // Select right(*) and left("a")
    assert(
      getShowString(df.select(dfRight("*"), dfLeft("a")), 10) ==
        """-------------------
          ||"C"  |"D"  |"A"  |
          |-------------------
          ||3    |4    |1    |
          |-------------------
          |""".stripMargin)
    df.select(dfRight("*"), dfRight("c")).show()
  }

  test("select columns on join result with conflict name", JavaStoredProcExclude) {
    import disableHideInternalAliasSession.implicits._
    val dfLeft = Seq((1, 2)).toDF("a", "b")
    val dfRight = Seq((3, 4)).toDF("a", "d")
    val df = dfLeft.join(dfRight)

    val df1 = df.select(dfLeft("*"), dfRight("*"))
    // Get all columns
    // The result column column name will be like:
    // |"l_Z36B_A"  |"B"  |"r_ztcn_A"  |"D"  |
    assert(df1.schema(0).name.matches("\"l_.*_A\""))
    assert(df1.schema(1).name.equals("B"))
    assert(df1.schema(2).name.matches("\"r_.*_A\""))
    assert(df1.schema(3).name.equals("D"))
    checkAnswer(df1, Seq(Row(1, 2, 3, 4)))

    val df2 = df.select(dfRight("a"), dfLeft("a"))
    // Get right-left conflict columns
    // The result column column name will be like:
    // |"r_v3Ms_A"  |"l_Xb7d_A"  |
    assert(df2.schema(0).name.matches("\"r_.*_A\""))
    assert(df2.schema(1).name.matches("\"l_.*_A\""))
    checkAnswer(df2, Seq(Row(3, 1)))

    val df3 = df.select(dfLeft("a"), dfRight("a"))
    // Get left-right conflict columns
    // The result column column name will be like:
    // |"l_v3Ms_A"  |"r_Xb7d_A"  |
    assert(df3.schema(0).name.matches("\"l_.*_A\""))
    assert(df3.schema(1).name.matches("\"r_.*_A\""))
    checkAnswer(df3, Seq(Row(1, 3)))

    val df4 = df.select(dfRight("*"), dfLeft("a"))
    // Get rightAll-left conflict columns
    // The result column column name will be like:
    // |"r_ClxT_A"  |"D"  |"l_q8l5_A"  |
    assert(df4.schema(0).name.matches("\"r_.*_A\""))
    assert(df4.schema(1).name.equals("D"))
    assert(df4.schema(2).name.matches("\"l_.*_A\""))
    checkAnswer(df4, Seq(Row(3, 4, 1)))
  }

}

class EagerDataFrameJoinSuite extends DataFrameJoinSuite with EagerSession

class LazyDataFrameJoinSuite extends DataFrameJoinSuite with LazySession
