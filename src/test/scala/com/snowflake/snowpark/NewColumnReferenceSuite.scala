package com.snowflake.snowpark

import com.snowflake.snowpark.internal.ParameterUtils
import com.snowflake.snowpark.internal.analyzer._
import com.snowflake.snowpark.types.IntegerType

import scala.language.implicitConversions

class NewColumnReferenceSuite extends SNTestBase {
  import session.implicits._
  lazy val disabledHideInternalAliasSession: Session =
    Session.builder
      .configFile(defaultProfile)
      .config(ParameterUtils.SnowparkHideInternalAlias, "false")
      .create
  private val df1 = Seq((1, 2)).toDF("a", "b")
  private val df2 = Seq((2, 3)).toDF("b", "c")

  lazy private val df1_disabled = disabledHideInternalAliasSession
    .createDataFrame(Seq((1, 2)))
    .toDF("a", "b")
  lazy private val df2_disabled = disabledHideInternalAliasSession
    .createDataFrame(Seq((2, 3)))
    .toDF("b", "c")

  test("schema") {
    val schema1 = df1.join(df2).schema

    assert(
      schema1.treeString(0) ==
        """root
      | |--A: Long (nullable = false)
      | |--B: Long (nullable = false)
      | |--B: Long (nullable = false)
      | |--C: Long (nullable = false)
      |""".stripMargin)
  }

  test("show", JavaStoredProcExclude) {
    assert(
      df1.join(df2).showString(10) ==
        """-------------------------
      ||"A"  |"B"  |"B"  |"C"  |
      |-------------------------
      ||1    |2    |2    |3    |
      |-------------------------
      |""".stripMargin)

    assert(!df1_disabled.join(df2_disabled).showString(10).contains(""""B""""))
  }

  test("dedup - drop", JavaStoredProcExclude) {
    val df1 = Seq((1, 2, 3, 4)).toDF("a", "b", "c", "d")
    val df2 = Seq((2, 3, 4, 5)).toDF("b", "c", "d", "e")
    val df3 = df1.join(df2)
    val df4 = df3.drop(df1("b"))
    verifyOutputName(
      df4.output,
      Seq(
        "a",
        TestInternalAlias("c"),
        TestInternalAlias("d"),
        "b",
        TestInternalAlias("c"),
        TestInternalAlias("d"),
        "e"))
    val df5 = df4.drop(df2("c"))
    verifyOutputName(
      df5.output,
      Seq("a", "c", TestInternalAlias("d"), "b", TestInternalAlias("d"), "e"))
    val df1_disabled1 = disabledHideInternalAliasSession
      .createDataFrame(Seq((1, 2, 3, 4)))
      .toDF("a", "b", "c", "d")
    val df2_disabled1 = disabledHideInternalAliasSession
      .createDataFrame(Seq((2, 3, 4, 5)))
      .toDF("b", "c", "d", "e")
    val df6 = df1_disabled1.join(df2_disabled1)
    val df7 = df6.drop(df1_disabled1("b"))
    verifyOutputName(
      df7.output,
      Seq(
        "a",
        TestInternalAlias("c"),
        TestInternalAlias("d"),
        TestInternalAlias("b"),
        TestInternalAlias("c"),
        TestInternalAlias("d"),
        "e"))
    val df8 = df7.drop(df2_disabled1("c"))
    verifyOutputName(
      df8.output,
      Seq(
        "a",
        TestInternalAlias("c"),
        TestInternalAlias("d"),
        TestInternalAlias("b"),
        TestInternalAlias("d"),
        "e"))
  }

  test("dedup - select", JavaStoredProcExclude) {
    val df1 = Seq((1, 2, 3, 4)).toDF("a", "b", "c", "d")
    val df2 = Seq((2, 3, 4, 5)).toDF("b", "c", "d", "e")
    val df3 = df1.join(df2)
    val df4 = df3.select(df1("a"), df1("b"), df1("d"), df2("c"), df2("d"), df2("e"))
    verifyOutputName(
      df4.output,
      Seq("a", "b", TestInternalAlias("d"), "c", TestInternalAlias("d"), "e"))
    assert(
      df4.showString(10) ==
        """-------------------------------------
      ||"A"  |"B"  |"D"  |"C"  |"D"  |"E"  |
      |-------------------------------------
      ||1    |2    |4    |3    |4    |5    |
      |-------------------------------------
      |""".stripMargin)
    assert(
      df4.schema.treeString(0) ==
        """root
      | |--A: Long (nullable = false)
      | |--B: Long (nullable = false)
      | |--D: Long (nullable = false)
      | |--C: Long (nullable = false)
      | |--D: Long (nullable = false)
      | |--E: Long (nullable = false)
      |""".stripMargin)
    val df1_disabled1 = disabledHideInternalAliasSession
      .createDataFrame(Seq((1, 2, 3, 4)))
      .toDF("a", "b", "c", "d")
    val df2_disabled1 = disabledHideInternalAliasSession
      .createDataFrame(Seq((2, 3, 4, 5)))
      .toDF("b", "c", "d", "e")
    val df5 = df1_disabled1.join(df2_disabled1)
    val df6 = df5.select(
      df1_disabled1("a"),
      df1_disabled1("b"),
      df1_disabled1("d"),
      df2_disabled1("c"),
      df2_disabled1("d"),
      df2_disabled1("e"))
    verifyOutputName(
      df6.output,
      Seq(
        "a",
        TestInternalAlias("b"),
        TestInternalAlias("d"),
        TestInternalAlias("c"),
        TestInternalAlias("d"),
        "e"))
    val showString = df6.showString(10)
    assert(!showString.contains(""""B""""))
    assert(!showString.contains(""""C""""))
    assert(!showString.contains(""""D""""))
    val schemaString = df6.schema.treeString(0)
    assert(!schemaString.contains("""-B: Long"""))
    assert(!schemaString.contains("""-C: Long"""))
    assert(!schemaString.contains("""-D: Long"""))
  }

  test("dedup - select - alias", JavaStoredProcExclude) {
    import functions.lit
    val df1 = Seq((1, 2, 3, 4)).toDF("a", "b", "c", "d")
    val df2 = Seq((2, 3, 4, 5)).toDF("b", "c", "d", "e")
    val df3 = df1.join(df2)
    val df4 =
      df3.select(df1("a"), df1("b"), df2("b").as("f"), df2("c"), df2("e"), lit(10).as("c"))
    verifyOutputName(df4.output, Seq("a", "b", "f", TestInternalAlias("c"), "e", "c"))
    assert(
      df4.showString(10) ==
        """-------------------------------------
        ||"A"  |"B"  |"F"  |"C"  |"E"  |"C"  |
        |-------------------------------------
        ||1    |2    |2    |3    |5    |10   |
        |-------------------------------------
        |""".stripMargin)
    val df1_disabled1 = disabledHideInternalAliasSession
      .createDataFrame(Seq((1, 2, 3, 4)))
      .toDF("a", "b", "c", "d")
    val df2_disabled1 = disabledHideInternalAliasSession
      .createDataFrame(Seq((2, 3, 4, 5)))
      .toDF("b", "c", "d", "e")
    val df5 = df1_disabled1.join(df2_disabled1)
    val df6 =
      df5.select(
        df1_disabled1("a"),
        df1_disabled1("b"),
        df2_disabled1("b").as("f"),
        df2_disabled1("c"),
        df2_disabled1("e"),
        lit(10).as("c"))
    verifyOutputName(
      df6.output,
      Seq("a", TestInternalAlias("b"), "f", TestInternalAlias("c"), "e", "c"))
    val showString = df6.showString(10)
    assert(!showString.contains(""""B""""))
    assert(showString.contains(""""C""""))
    assert(showString.contains("""_C"""))
  }

  test("error message when refer duplicated columns", JavaStoredProcExclude) {
    val df1 = Seq((1, 2, 3, 4)).toDF("a", "b", "c", "d")
    val df2 = Seq((2, 3, 4, 5)).toDF("b", "c", "d", "e")
    val df3 = df1.join(df2)
    val error1 = intercept[SnowparkClientException](df3.select(df3("b")))
    assert(error1.message.contains("The reference to the column 'b' is ambiguous"))
    val df1_disabled1 = disabledHideInternalAliasSession
      .createDataFrame(Seq((1, 2, 3, 4)))
      .toDF("a", "b", "c", "d")
    val df2_disabled1 = disabledHideInternalAliasSession
      .createDataFrame(Seq((2, 3, 4, 5)))
      .toDF("b", "c", "d", "e")
    val df4 = df1_disabled1.join(df2_disabled1)
    val error2 = intercept[SnowparkClientException](df4.select(df4("b")))
    assert(error2.message.contains("The DataFrame does not contain the column named 'b'"))
  }

  trait TestColumnName
  case class TestOriginalName(name: String) extends TestColumnName
  case class TestInternalAlias(name: String) extends TestColumnName
  implicit def stringToOriginalName(name: String): TestOriginalName =
    TestOriginalName(name)
  private def verifyOutputName(output: Seq[Attribute], columnNames: Seq[TestColumnName]): Unit = {
    assert(output.size == columnNames.size)
    assert(output.map(_.name).zip(columnNames).forall {
      case (name, TestOriginalName(n)) => name == quoteName(n)
      case (name, TestInternalAlias(n)) =>
        val quoted = quoteName(n)
        val withoutQuote = quoted.substring(1, quoted.length - 1)
        name != quoted && name.contains(withoutQuote)
    })
  }

  // internal renamed columns tests
  test("internal renamed columns") {
    val tableExp = internal.analyzer.TableFunctionEx("dummy", Seq.empty)
    val att = internal.analyzer.Attribute("a", IntegerType)
    // Project
    val p1 = Project(Seq(Alias(Attribute("c", IntegerType), "d", isInternal = true)), project1)
    assert(p1.internalRenamedColumns == Map("d" -> """"C""""))
    val p2 = Project(Seq(Alias(Attribute("c", IntegerType), "d", isInternal = true)), project2)
    assert(p2.internalRenamedColumns == Map("d" -> """"C"""", "b" -> """"A""""))

    // Leaf
    verifyLeadNode(TableFunctionRelation(tableExp))
    verifyLeadNode(Range(1, 1, 1))
    verifyLeadNode(Generator(Seq(tableExp), 1))
    verifyLeadNode(UnresolvedRelation("a"))
    verifyLeadNode(StoredProcedureRelation("a", Seq.empty))
    verifyLeadNode(SnowflakeValues(Seq(att), Seq(Row(1))))
    verifyLeadNode(CopyIntoNode("a", Seq("a"), Seq(tableExp), Map.empty, null))

    // Unary
    verifyUnaryNode(SnowflakeSampleNode(Some(1.0), None, _))
    verifyUnaryNode(Sort(Seq.empty, _))
    verifyUnaryNode(Aggregate(Seq.empty, Seq.empty, _))
    verifyUnaryNode(Pivot(null, Seq.empty, Seq.empty, _))
    verifyUnaryNode(Filter(null, _))
    // ProjectAndFilter is the output of simplifier,
    // can not show up in the original plan tree, skip
    verifyUnaryNode(CopyIntoLocation(null, _))
    verifyUnaryNode(CreateViewCommand("a", _, LocalTempView))
    verifyUnaryNode(Lateral(_, tableExp))
    verifyUnaryNode(Limit(null, _))
    // LimitOnSort is the output of simplifier,
    // can not show up in the original plan tree, skip
    verifyUnaryNode(TableFunctionJoin(_, tableExp, None))
    verifyUnaryNode(TableMerge("name", _, null, Seq.empty))
    verifyUnaryNode(WithColumns(Seq.empty, _))
    verifyUnaryNode(DropColumns(Seq.empty, _))

    // binary nodes
    verifyBinaryNode(Except)
    verifyBinaryNode(Intersect)
    verifyBinaryNode(Union)
    verifyBinaryNode(UnionAll)
    verifyBinaryNode(Join(_, _, LeftAnti, None))

    // others
    verifyUnaryNode(child => TableDelete("a", None, Some(child)))
    verifyUnaryNode(child => TableUpdate("a", Map.empty, None, Some(child)))
    verifyUnaryNode(child => SnowflakeCreateTable("a", SaveMode.Append, Some(child)))
    verifyBinaryNode((plan1, plan2) => SimplifiedUnion(Seq(plan1, plan2)))
  }

  private val project1 = Project(Seq(Attribute("a", IntegerType)), Range(1, 1, 1))
  private val project2 =
    Project(Seq(Alias(Attribute("a", IntegerType), "b", isInternal = true)), Range(1, 1, 1))
  private val project3 = Project(Seq(Attribute("a1", IntegerType)), Range(1, 1, 1))
  private val project4 =
    Project(Seq(Alias(Attribute("a1", IntegerType), "b1", isInternal = true)), Range(1, 1, 1))
  private def verifyBinaryNode(func: (LogicalPlan, LogicalPlan) => LogicalPlan): Unit = {
    val plans = Seq(project1, project2, project3, project4)
    val data: Seq[(LogicalPlan, LogicalPlan)] =
      (0 to 2).flatMap(i => (i + 1 to 3).map(j => (plans(i), plans(j))))

    data.foreach { case (left, right) =>
      verifyNode(children => func(children.head, children(1)), Seq(left, right))
    }
  }
  private def verifyUnaryNode(func: LogicalPlan => LogicalPlan): Unit = {
    verifyNode(children => func(children.head), Seq(project1))
    verifyNode(children => func(children.head), Seq(project2))
  }
  private def verifyLeadNode(func: => LogicalPlan): Unit =
    verifyNode(_ => func, Seq.empty)
  private def verifyNode(
      func: Seq[LogicalPlan] => LogicalPlan,
      children: Seq[LogicalPlan]): Unit = {
    val plan = func(children)
    val expected = children
      .map(_.internalRenamedColumns)
      .foldLeft(Map.empty[String, String])(_ ++ _)
    assert(plan.internalRenamedColumns == expected)
  }
}
