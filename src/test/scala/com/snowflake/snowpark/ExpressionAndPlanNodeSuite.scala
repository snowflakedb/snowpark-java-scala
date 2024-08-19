package com.snowflake.snowpark

import com.snowflake.snowpark.internal.SnowflakeUDF
import com.snowflake.snowpark.internal.analyzer._
import com.snowflake.snowpark.types.{BooleanType, IntegerType, StringType}

class ExpressionAndPlanNodeSuite extends SNTestBase {

  test("no duplicated alias") {
    import session.implicits._
    val df = Seq(1).toDF("a")
    val query1 = df.select(df("a").as("a")).snowflakePlan.queries.last.sql
    val query2 = df.select(df("a")).snowflakePlan.queries.last.sql
    val query3 = df.select(df("a").as("A")).snowflakePlan.queries.last.sql
    val query4 = df.select(df("a").as("A").as("a").as("\"A\"")).snowflakePlan.queries.last.sql
    assert(query1 == query2)
    assert(query1 == query3)
    assert(query1 == query4)
  }

  test("Original Plan") {
    val node1 = Range(1, 1, 1)
    val node2 = Range(2, 2, 2)
    val snowflakePlan1 = SnowflakePlan(Seq.empty, "222", session, None, supportAsyncMode = false)
    val snowflakePlan2 = SnowflakePlan(Seq.empty, "111", session, None, supportAsyncMode = false)

    assert(node1.getSnowflakePlan.isEmpty)
    node1.setSnowflakePlan(snowflakePlan1)
    assert(node1.getSnowflakePlan.get == snowflakePlan1)
    node1.setSourcePlan(node2)
    assert(node1.getSnowflakePlan.isEmpty)
    assert(node2.getSnowflakePlan.isEmpty)
    node1.setSnowflakePlan(snowflakePlan1)
    assert(node1.getSnowflakePlan.get == snowflakePlan1)
    assert(node2.getSnowflakePlan.get == snowflakePlan1)

    assert(node1.getOrUpdateSnowflakePlan(snowflakePlan2) == snowflakePlan1)
    assert(node2.getOrUpdateSnowflakePlan(snowflakePlan2) == snowflakePlan2)

    val node3 = Range(3, 3, 3)
    val node4 = Range(3, 3, 3)
    assert(!node3.eq(node4))
    assert(node3 == node4)
    node3.setSourcePlan(node4)
    node4.setSnowflakePlan(snowflakePlan1)
    assert(node3.getSnowflakePlan.get == snowflakePlan1)
  }

  test("Join unit test") {
    val ex = intercept[SnowparkClientException] {
      JoinType("incorrect_join_type")
    }
    assert(ex.errorCode.equals("0116"))
    assert(ex.message.contains("Unsupported join type 'incorrect_join_type'"))

    val leftSemi = JoinType("left_semi")
    assert(leftSemi.sql.equals("LEFT SEMI"))

    val leftAnti = JoinType("Leftanti")
    assert(leftAnti.sql.equals("LEFT ANTI"))

    val ex2 = intercept[SnowparkClientException] {
      NaturalJoin(JoinType("left_semi"))
    }
    assert(ex2.errorCode.equals("0117"))
    assert(ex2.message.contains("Unsupported natural join type 'LeftSemi'."))
    assert(NaturalJoin(JoinType("inner")).sql.equals("NATURAL INNER"))

    val ex3 = intercept[SnowparkClientException] {
      UsingJoin(JoinType("cross"), Seq("col1"))
    }
    assert(ex3.errorCode.equals("0118"))
    assert(ex3.message.contains("Unsupported using join type 'Cross'."))
    assert(UsingJoin(JoinType("inner"), Seq("col1")).sql.equals("USING INNER"))

    assert(Join(null, null, JoinType("Inner"), None).sql.equals("INNER"))
  }

  // expression's children and dependent columns
  lazy val col1 = Attribute("a", IntegerType)
  lazy val col2 = Attribute("b", StringType)
  lazy val unresolvedCol1 = UnresolvedAttribute("c")
  lazy val unresolvedCol2 = UnresolvedAttribute("d")

  def childrenChecker(numOfArg: Int, func: Seq[Expression] => Expression): Unit = {
    // return expresion, expression.toSql, and expected expression.invokedColumnNames
    def attGenerator(index: Int): (Expression, String, Option[Set[String]]) = {
      val name = s""""COL$index""""
      val exp = Attribute(name, IntegerType)
      (exp, exp.toString, Some(Set(name)))
    }

    def litGenerator(index: Int): (Expression, String, Option[Set[String]]) = {
      val exp = functions.lit(index).expr
      (exp, exp.toString, Some(Set.empty))
    }

    def unresolvedGenerator(index: Int): (Expression, String, Option[Set[String]]) = {
      val name = s""""UN$index""""
      val exp = UnresolvedAttribute(name)
      (exp, exp.toString, None)
    }

    // list all combination of input
    // example result data
    // [(0, 1, 2), (1, 2, 0) ...]
    // 0 -> column, 1 -> lit, 2 -> unresolved
    def generateData(index: Int, prev: List[Int]): Seq[Seq[Int]] =
      (0 until 3).flatMap(x => {
        val result = x :: prev
        if (index == numOfArg) {
          Seq(result)
        } else {
          generateData(index + 1, result)
        }
      })

    generateData(1, List.empty).foreach { data =>
      {
        val (exprs, names, invoked) = data.zipWithIndex
          .map {
            // convert numbers to real test data
            case (0, index) => attGenerator(index)
            case (1, index) => litGenerator(index)
            case (2, index) => unresolvedGenerator(index)
          }
          .foldRight((List.empty[Expression], List.empty[String], Option(Set.empty[String]))) {
            // generate test data and expected result
            case ((exp, name, invoked), (expList, nameList, invokedSet)) =>
              (
                exp :: expList,
                name :: nameList,
                if (invoked.isEmpty || invokedSet.isEmpty) {
                  None
                } else {
                  Some(invoked.get ++ invokedSet.get)
                }
              )
          }

        val exp = func(exprs)
        assert(exp.children.map(_.toString).toSet == names.toSet)
        assert(exp.dependentColumnNames == invoked)
      }
    }
  }

  def unaryChecker(func: Expression => Expression): Unit =
    childrenChecker(1, seq => func(seq.head))

  def binaryChecker(func: (Expression, Expression) => Expression): Unit =
    childrenChecker(2, seq => func(seq.head, seq(1)))

  def emptyChecker(func: => Expression): Unit = {
    val exp: Expression = func
    assert(exp.children.isEmpty)
    assert(exp.dependentColumnNames.get.isEmpty)
  }

  test("check children and dependent columns") {
    unaryChecker(Collate(_, "dummy"))
    unaryChecker(SubfieldString(_, "dummy"))
    unaryChecker(SubfieldInt(_, 123))
    unaryChecker(FlattenFunction(_, "dummy", outer = true, recursive = true, "dummy"))

    binaryChecker(Like)
    binaryChecker(RegExp)
    binaryChecker((x, y) => FunctionExpression("dummy", Seq(x, y), isDistinct = false))
    binaryChecker((x, y) => internal.analyzer.TableFunction("dummy", Seq(x, y)))
    binaryChecker((x, y) => NamedArgumentsTableFunction("dummy", Map("a" -> x, "b" -> y)))
    binaryChecker((x, y) => GroupingSetsExpression(Seq(Set(x, y))))
    binaryChecker((x, y) => GroupingSetsExpression(Seq(Set(x), Set(y))))

    childrenChecker(3, data => WithinGroup(data.head, data.tail))
    childrenChecker(
      5,
      data => UpdateMergeExpression(Some(data.head), Map(data(1) -> data(2), data(3) -> data(4)))
    )
    childrenChecker(
      4,
      data => UpdateMergeExpression(None, Map(data(1) -> data(2), data(3) -> data.head))
    )

    unaryChecker(x => DeleteMergeExpression(Some(x)))
    emptyChecker(DeleteMergeExpression(None))

    childrenChecker(
      5,
      data => InsertMergeExpression(Some(data.head), Seq(data(1), data(2)), Seq(data(3), data(4)))
    )
    childrenChecker(
      4,
      data => InsertMergeExpression(None, Seq(data(1), data(2)), Seq(data(3), data.head))
    )

    childrenChecker(2, Cube)
    childrenChecker(2, Rollup)
    emptyChecker(ScalarSubquery(null))

    childrenChecker(
      5,
      data => CaseWhen(Seq((data.head, data(1)), (data(2), data(3))), Some(data(4)))
    )
    childrenChecker(4, data => CaseWhen(Seq((data.head, data(1)), (data(2), data(3))), None))

    childrenChecker(2, MultipleExpression)
    childrenChecker(3, data => InExpression(data.head, data.tail))

    unaryChecker(ListAgg(_, "dummy", isDistinct = false))
    unaryChecker(Cast(_, IntegerType))
    unaryChecker(UnaryMinus)
    unaryChecker(IsNull)
    unaryChecker(IsNotNull)
    unaryChecker(IsNaN)
    unaryChecker(Not)
    unaryChecker(Alias(_, "dummy"))
    unaryChecker(UnresolvedAlias(_))

    emptyChecker(Literal(1, Some(IntegerType)))

    binaryChecker(EqualTo)
    binaryChecker(NotEqualTo)
    binaryChecker(GreaterThan)
    binaryChecker(LessThan)
    binaryChecker(LessThanOrEqual)
    binaryChecker(GreaterThanOrEqual)
    binaryChecker(EqualNullSafe)
    binaryChecker(Or)
    binaryChecker(And)
    binaryChecker(Add)
    binaryChecker(Subtract)
    binaryChecker(Multiply)
    binaryChecker(Divide)
    binaryChecker(Remainder)
    binaryChecker(BitwiseAnd)
    binaryChecker(BitwiseOr)
    binaryChecker(BitwiseXor)
    binaryChecker(ShiftLeft)
    binaryChecker(ShiftRight)
    binaryChecker(Logarithm)
    binaryChecker(Atan2)
    binaryChecker(Round)
    binaryChecker(Pow)
    binaryChecker(Sha2)
    binaryChecker(StringRepeat)
    binaryChecker(AddMonths)
    binaryChecker(NextDay)
    binaryChecker(Trunc)
    binaryChecker(DateTrunc)
    binaryChecker(ArraysOverlap)
    binaryChecker(ArrayIntersect)

    childrenChecker(3, data => SortOrder(data.head, Ascending, NullsLast, data.tail.toSet))
    childrenChecker(2, SnowflakeUDF("dummy", _, IntegerType))

    emptyChecker(UnboundedPreceding)
    emptyChecker(UnboundedFollowing)
    emptyChecker(CurrentRow)
    emptyChecker(UnspecifiedFrame)
    binaryChecker(SpecifiedWindowFrame(RowFrame, _, _))
  }

  test("star children and dependent columns") {
    val exp1 = Star(Seq(col1, col2))
    assert(exp1.children.map(_.toString) == Seq(col1.toString, col2.toString))
    assert(exp1.dependentColumnNames.contains(Set("\"A\"", "\"B\"")))

    val exp2 = Star(Seq(col1, unresolvedCol1))
    assert(exp2.children.map(_.toString) == Seq(col1.toString, unresolvedCol1.toString))
    assert(exp2.dependentColumnNames.isEmpty)

    val exp3 = Star(Seq.empty)
    assert(exp3.children.isEmpty)
    assert(exp3.dependentColumnNames.isEmpty)
  }

  test("Attribute children and dependent columns") {
    val exp1 = Attribute("dummy", IntegerType)
    assert(exp1.children.isEmpty)
    assert(exp1.dependentColumnNames.contains(Set("\"DUMMY\"")))
  }

  test("UnresolvedAttribute children and dependent columns") {
    val exp1 = UnresolvedAttribute("dummy")
    assert(exp1.children.isEmpty)
    assert(exp1.dependentColumnNames.isEmpty)
  }

  test("WindowExpression children and dependent columns") {
    val windowFrame1 = SpecifiedWindowFrame(RowFrame, col1, col2)
    val windowFrame2 = SpecifiedWindowFrame(RowFrame, col1, unresolvedCol1)
    val col3 = Attribute("e", IntegerType)
    val col4 = Attribute("f", IntegerType)
    val col5 = Attribute("g", IntegerType)
    val col6 = Attribute("h", IntegerType)
    val order1 = SortOrder(col3, Ascending, NullsLast, Set.empty)
    val order2 = SortOrder(unresolvedCol2, Ascending, NullsLast, Set.empty)

    val windowSpec1 = WindowSpecDefinition(Seq(col4, col5), Seq(order1), windowFrame1)
    assert(
      windowSpec1.children.map(_.toString).toSet ==
        Set(col4.toString, col5.toString, order1.toString, windowFrame1.toString)
    )
    assert(
      windowSpec1.dependentColumnNames.contains(Set("\"F\"", "\"G\"", "\"E\"", "\"A\"", "\"B\""))
    )

    val windowSpec2 = WindowSpecDefinition(Seq(col4, col5), Seq(order1), windowFrame2)
    assert(
      windowSpec2.children.map(_.toString).toSet ==
        Set(col4.toString, col5.toString, order1.toString, windowFrame2.toString)
    )
    assert(windowSpec2.dependentColumnNames.isEmpty)

    val windowSpec3 = WindowSpecDefinition(Seq(col4, col5), Seq(order2), windowFrame1)
    assert(
      windowSpec3.children.map(_.toString).toSet ==
        Set(col4.toString, col5.toString, order2.toString, windowFrame1.toString)
    )
    assert(windowSpec3.dependentColumnNames.isEmpty)

    val windowSpec4 = WindowSpecDefinition(Seq(col4, unresolvedCol2), Seq(order1), windowFrame1)
    assert(
      windowSpec4.children.map(_.toString).toSet ==
        Set(col4.toString, unresolvedCol2.toString, order1.toString, windowFrame1.toString)
    )
    assert(windowSpec4.dependentColumnNames.isEmpty)

    val window1 = WindowExpression(col6, windowSpec1)
    assert(
      window1.children.map(_.toString).toSet ==
        Set(col6.toString, windowSpec1.toString)
    )
    assert(
      window1.dependentColumnNames.contains(
        Set("\"F\"", "\"G\"", "\"E\"", "\"A\"", "\"B\"", "\"H\"")
      )
    )

    val window2 = WindowExpression(col6, windowSpec2)
    assert(
      window2.children.map(_.toString).toSet ==
        Set(col6.toString, windowSpec2.toString)
    )
    assert(window2.dependentColumnNames.isEmpty)

    val window3 = WindowExpression(unresolvedCol1, windowSpec1)
    assert(
      window3.children.map(_.toString).toSet ==
        Set(unresolvedCol1.toString, windowSpec1.toString)
    )
    assert(window3.dependentColumnNames.isEmpty)
  }

  // analyze expression tests

  def analyzerChecker(
      dataSize: Int,
      // input: a list of generated child expressions,
      // output: a reference to the expression being tested
      func: Seq[Expression] => Expression
  ): Unit = {
    val args: Seq[Literal] = (0 until dataSize)
      .map(functions.lit)
      .map(_.expr.asInstanceOf[Literal])
    val expected: Seq[Literal] =
      args.map(x => functions.lit(x.value.asInstanceOf[Int] + 100).expr.asInstanceOf[Literal])

    val exp = func(args)
    val func1: Expression => Expression = {
      case Literal(value, dataTypeOption) =>
        Literal(value.asInstanceOf[Int] + 100, dataTypeOption)
      case x => x
    }

    // update all children
    assert(exp.analyze(func1).children == expected)

    val lit0 = functions.lit(0).expr
    val func2: Expression => Expression = _ => lit0

    // update it self
    assert(exp.analyze(func2) == lit0)

    // no update
    assert(exp.analyze(x => x) == exp)
  }

  def binaryAnalyzerChecker(func: (Expression, Expression) => Expression): Unit =
    analyzerChecker(2, data => func(data.head, data(1)))

  def unaryAnalyzerChecker(func: Expression => Expression): Unit =
    analyzerChecker(1, data => func(data.head))

  def leafAnalyzerChecker(func: => Expression): Unit =
    analyzerChecker(0, _ => func)

  test("expression analyze function") {
    binaryAnalyzerChecker(Like)
    binaryAnalyzerChecker(RegExp)
    unaryAnalyzerChecker(Collate(_, "dummy"))
    unaryAnalyzerChecker(SubfieldString(_, "dummy"))
    unaryAnalyzerChecker(SubfieldInt(_, 1))
    analyzerChecker(2, FunctionExpression("dummy", _, isDistinct = false))
    unaryAnalyzerChecker(FlattenFunction(_, "a", outer = false, recursive = true, "b"))
    analyzerChecker(2, internal.analyzer.TableFunction("dummy", _))
    analyzerChecker(
      2,
      data => NamedArgumentsTableFunction("dummy", Map("a" -> data.head, "b" -> data(1)))
    )
    analyzerChecker(
      4,
      data => GroupingSetsExpression(Seq(Set(data.head, data(1)), Set(data(2), data(3))))
    )
    analyzerChecker(2, SnowflakeUDF("dummy", _, IntegerType))
    leafAnalyzerChecker(Literal(1, Some(IntegerType)))
    analyzerChecker(3, data => SortOrder(data.head, Ascending, NullsLast, data.tail.toSet))

    analyzerChecker(
      5,
      data => UpdateMergeExpression(Some(data.head), Map(data(1) -> data(3), data(2) -> data(4)))
    )

    analyzerChecker(
      4,
      data => UpdateMergeExpression(None, Map(data.head -> data(2), data(1) -> data(3)))
    )

    unaryAnalyzerChecker(data => DeleteMergeExpression(Some(data)))
    leafAnalyzerChecker(DeleteMergeExpression(None))

    analyzerChecker(
      5,
      data => InsertMergeExpression(Some(data.head), Seq(data(1), data(2)), Seq(data(3), data(4)))
    )
    analyzerChecker(
      4,
      data => InsertMergeExpression(None, Seq(data.head, data(1)), Seq(data(2), data(3)))
    )

    analyzerChecker(2, Cube)
    analyzerChecker(2, Rollup)

    leafAnalyzerChecker(ScalarSubquery(null))

    analyzerChecker(
      5,
      data => CaseWhen(Seq((data.head, data(1)), (data(2), data(3))), Some(data(4)))
    )

    analyzerChecker(4, data => CaseWhen(Seq((data.head, data(1)), (data(2), data(3)))))

    analyzerChecker(3, MultipleExpression)
    analyzerChecker(3, data => InExpression(data.head, data.tail))
    leafAnalyzerChecker(Attribute("dummy", IntegerType))
    leafAnalyzerChecker(UnresolvedAttribute("dummy"))
    unaryAnalyzerChecker(ListAgg(_, "dummy", isDistinct = false))

    binaryAnalyzerChecker(EqualTo)
    binaryAnalyzerChecker(NotEqualTo)
    binaryAnalyzerChecker(GreaterThan)
    binaryAnalyzerChecker(LessThan)
    binaryAnalyzerChecker(LessThanOrEqual)
    binaryAnalyzerChecker(GreaterThanOrEqual)
    binaryAnalyzerChecker(EqualNullSafe)
    binaryAnalyzerChecker(Or)
    binaryAnalyzerChecker(And)
    binaryAnalyzerChecker(Add)
    binaryAnalyzerChecker(Subtract)
    binaryAnalyzerChecker(Multiply)
    binaryAnalyzerChecker(Divide)
    binaryAnalyzerChecker(Remainder)
    binaryAnalyzerChecker(BitwiseAnd)
    binaryAnalyzerChecker(BitwiseOr)
    binaryAnalyzerChecker(BitwiseXor)
    binaryAnalyzerChecker(ShiftLeft)
    binaryAnalyzerChecker(ShiftRight)
    binaryAnalyzerChecker(Logarithm)
    binaryAnalyzerChecker(Atan2)
    binaryAnalyzerChecker(Round)
    binaryAnalyzerChecker(Pow)
    binaryAnalyzerChecker(Sha2)
    binaryAnalyzerChecker(StringRepeat)
    binaryAnalyzerChecker(AddMonths)
    binaryAnalyzerChecker(NextDay)
    binaryAnalyzerChecker(Trunc)
    binaryAnalyzerChecker(DateTrunc)
    binaryAnalyzerChecker(ArraysOverlap)
    binaryAnalyzerChecker(ArrayIntersect)
    unaryAnalyzerChecker(Cast(_, IntegerType))
    unaryAnalyzerChecker(UnaryMinus)
    unaryAnalyzerChecker(IsNull)
    unaryAnalyzerChecker(IsNotNull)
    unaryAnalyzerChecker(IsNaN)
    unaryAnalyzerChecker(Not)
    unaryAnalyzerChecker(Alias(_, "dummy"))
    unaryAnalyzerChecker(UnresolvedAlias(_))
    leafAnalyzerChecker(UnboundedPreceding)
    leafAnalyzerChecker(UnboundedFollowing)
    leafAnalyzerChecker(CurrentRow)
    leafAnalyzerChecker(UnspecifiedFrame)
    binaryAnalyzerChecker(SpecifiedWindowFrame(RowFrame, _, _))
  }

  test("star - analyze") {
    val att1 = Attribute("a", IntegerType)
    val att2 = Attribute("b", IntegerType)
    val func1: Expression => Expression = {
      case _: Attribute => att2
      case x            => x
    }
    val exp = Star(Seq(att1))
    assert(exp.analyze(func1).children == Seq(att2))
    assert(exp.analyze(x => x) == exp)
    assert(exp.analyze(_ => att2) == att2)
    leafAnalyzerChecker(Star(Seq.empty))
  }

  test("WindowSpecDefinition - analyze") {
    val lit0 = Literal(0, Some(IntegerType))
    val lit1 = Literal(1, Some(IntegerType))
    val order1 = SortOrder(lit1, Ascending, NullsLast, Set.empty)
    val order0 = SortOrder(lit0, Ascending, NullsLast, Set.empty)
    val frame = SpecifiedWindowFrame(RowFrame, lit0, lit1)
    val exp = WindowSpecDefinition(Seq(lit0), Seq(order0), UnspecifiedFrame)

    val func1: Expression => Expression = {
      case _: SortOrder   => order1
      case _: WindowFrame => frame
      case Literal(0, _)  => lit1
      case x              => x
    }

    val func2: Expression => Expression = {
      case _: WindowSpecDefinition => lit1
      case x                       => x
    }

    val exp1 = exp.analyze(func1)
    assert(exp1.children == Seq(lit1, order1, frame))

    assert(exp.analyze(x => x) == exp)
    assert(exp.analyze(func2) == lit1)
  }

  test("WindowExpression - analyze") {
    val lit0 = Literal(0, Some(IntegerType))
    val lit1 = Literal(1, Some(IntegerType))
    val order = SortOrder(lit1, Ascending, NullsLast, Set.empty)
    val window1 = WindowSpecDefinition(Seq(lit0), Seq(order), UnspecifiedFrame)
    val window2 = WindowSpecDefinition(Seq(lit1), Seq(order), UnspecifiedFrame)
    val func1: Expression => Expression = {
      case _: Literal              => lit1
      case _: WindowSpecDefinition => window2
      case x                       => x
    }

    val func2: Expression => Expression = {
      case _: WindowExpression => lit0
      case x                   => x
    }

    val exp = WindowExpression(lit0, window1)
    assert(exp.analyze(func1).children == Seq(lit1, window2))
    assert(exp.analyze(x => x) == exp)

    assert(exp.analyze(func2) == lit0)
  }
  // Expression Analyzer Tests
  private val lit1 = Literal(1, Some(IntegerType))
  private val lit2 = Literal(2, Some(IntegerType))
  private val attr1 = Attribute("col1", IntegerType)
  private val attr2 = Attribute("col2", IntegerType)
  private val attr3 = Attribute("col3", IntegerType)
  private val alias1 = Alias(attr1, "\"A\"")
  private val alias2 = Alias(attr2, "\"B\"")
  private val alias3 = Alias(attr3, "\"C\"")
  private val map1 = Map(attr1.exprId -> alias1.name, attr2.exprId -> alias2.name)

  private val map2 = Map(attr3.exprId -> alias3.name)
  private val map3 = Map(attr2.exprId -> alias2.name)
  private val map4 = map2 ++ map3

  private val child1 = Generator(Seq(alias3), 1)
  private val child2 = Generator(Seq(lit1), 1)
  private val child3 = Generator(Seq(lit2), 2)
  private val child4 = Generator(Seq(alias2), 1)

  test("WithColumns - Analyzer") {
    val plan = WithColumns(Seq(attr3), child1)
    assert(plan.aliasMap == map2)
    assert(
      plan.analyzed
        .asInstanceOf[WithColumns]
        .newCols
        .head
        .asInstanceOf[Attribute]
        .name == "\"C\""
    )

    val plan1 = WithColumns(Seq(attr3), child2)
    assert(plan1.aliasMap.isEmpty)
    assert(
      plan1.analyzed
        .asInstanceOf[WithColumns]
        .newCols
        .head
        .asInstanceOf[Attribute]
        .name == "\"COL3\""
    )
  }

  test("DropColumns - Analyzer") {
    val plan = DropColumns(Seq(attr3), child1)
    assert(plan.aliasMap == map2)
    assert(
      plan.analyzed
        .asInstanceOf[DropColumns]
        .columns
        .head
        .asInstanceOf[Attribute]
        .name == "\"C\""
    )

    val plan1 = DropColumns(Seq(attr3), child2)
    assert(plan1.aliasMap.isEmpty)
    assert(
      plan1.analyzed
        .asInstanceOf[DropColumns]
        .columns
        .head
        .asInstanceOf[Attribute]
        .name == "\"COL3\""
    )
  }

  test("TableFunctionRelation - Analyzer") {
    val exp = internal.analyzer.TableFunction("dummy", Seq.empty)
    val plan = TableFunctionRelation(exp)
    assert(plan.aliasMap.isEmpty)
    assert(plan.analyzed == plan)

    val exp1 = internal.analyzer.TableFunction("dummy", Seq(alias1, alias2))
    val plan1 = TableFunctionRelation(exp1)
    assert(plan1.aliasMap == map1)
    assert(plan1.analyzed.toString == plan1.toString)
  }

  test("Range - Analyzer") {
    val plan = Range(1, 2, 3)
    assert(plan.aliasMap.isEmpty)
    assert(plan.analyzed == plan)
  }

  test("Generator - Analyzer") {
    val plan = Generator(Seq(alias1, alias2), 1)
    assert(plan.aliasMap == map1)
    assert(plan.analyzed.toString == plan.toString)

    val plan1 = Generator(Seq(lit1), 1)
    assert(plan1.aliasMap.isEmpty)
    assert(plan1.analyzed == plan1)
  }

  test("UnresolvedRelation - Analyzer") {
    val plan = UnresolvedRelation("dummy")
    assert(plan.analyzed == plan)
    assert(plan.aliasMap.isEmpty)
  }

  test("StoredProcedureRelation - Analyzer") {
    val plan = StoredProcedureRelation("dummy", Seq.empty)
    assert(plan.analyzed == plan)
    assert(plan.aliasMap.isEmpty)
  }

  test("SnowflakeValues - Analyzer") {
    val plan = SnowflakeValues(Seq.empty, Seq.empty)
    assert(plan.analyzed == plan)
    assert(plan.aliasMap.isEmpty)
  }

  test("CopyIntoNode - Analyzer") {
    val plan = CopyIntoNode("dummy", Seq("a"), Seq(alias1, alias2), Map.empty, null)
    assert(plan.aliasMap == map1)
    assert(plan.analyzed.toString == plan.toString)

    val plan1 = CopyIntoNode("dummy", Seq("a"), Seq(lit1), Map.empty, null)
    assert(plan1.aliasMap.isEmpty)
    assert(plan1.analyzed == plan1)
  }

  test("CopyIntoLocation - Analyzer") {
    val plan = CopyIntoLocation(null, child2)
    assert(plan.analyzed == plan)
    assert(plan.aliasMap.isEmpty)

    val plan1 = CopyIntoLocation(null, child1)
    assert(plan1.aliasMap == map2)
    assert(plan1.analyzed.toString == plan1.toString)
  }

  test("SnowflakeSampleNode - Analyzer") {
    val plan = SnowflakeSampleNode(Some(1.0), None, child2)
    assert(plan.analyzed == plan)
    assert(plan.aliasMap.isEmpty)

    val plan1 = SnowflakeSampleNode(Some(1.0), None, child1)
    assert(plan1.aliasMap == map2)
    assert(plan1.analyzed.toString == plan1.toString)
  }

  test("Sort - Analyzer") {
    val order = SortOrder(attr3, Ascending, NullsLast, Set.empty)
    val plan = Sort(Seq(order), child1)
    assert(plan.aliasMap == map2)
    assert(
      plan.analyzed.asInstanceOf[Sort].order.head.child.asInstanceOf[Attribute].name == "\"C\""
    )

    val plan1 = Sort(Seq(order), child2)
    assert(plan1.aliasMap.isEmpty)
    assert(plan1.analyzed.toString == plan1.toString)
  }

  test("LimitOnSort - Analyzer") {
    val order = SortOrder(attr3, Ascending, NullsLast, Set.empty)
    val plan = LimitOnSort(child1, attr3, Seq(order))
    assert(plan.aliasMap == map2)
    assert(
      plan.analyzed.asInstanceOf[LimitOnSort].order.head.child.asInstanceOf[Attribute].name
        == "\"C\""
    )
    assert(
      plan.analyzed.asInstanceOf[LimitOnSort].limitExpr.asInstanceOf[Attribute].name
        == "\"C\""
    )

    val plan1 = LimitOnSort(child2, attr3, Seq(order))
    assert(plan1.aliasMap.isEmpty)
    assert(plan1.analyzed.toString == plan1.toString)
  }

  test("Aggregate - Analyzer") {
    val plan = Aggregate(Seq(attr3), Seq.empty, child1)
    assert(plan.aliasMap == map2)
    assert(
      plan.analyzed
        .asInstanceOf[Aggregate]
        .groupingExpressions
        .head
        .asInstanceOf[Attribute]
        .name == "\"C\""
    )

    val plan1 = Aggregate(Seq.empty, Seq(attr3), child1)
    assert(plan1.aliasMap == map2)
    assert(plan1.analyzed.asInstanceOf[Aggregate].aggregateExpressions.head.name == "\"C\"")

    val plan2 = Aggregate(Seq(attr3), Seq.empty, child2)
    assert(plan2.aliasMap.isEmpty)
    assert(plan2.analyzed.toString == plan2.toString)
  }

  test("Pivot - Analyzer") {
    val plan = Pivot(attr3, Seq.empty, Seq.empty, child1)
    assert(plan.aliasMap == map2)
    assert(plan.analyzed.asInstanceOf[Pivot].pivotColumn.asInstanceOf[Attribute].name == "\"C\"")

    val plan1 = Pivot(attr1, Seq(attr3), Seq.empty, child1)
    assert(plan1.aliasMap == map2)
    assert(
      plan1.analyzed.asInstanceOf[Pivot].pivotColumn.asInstanceOf[Attribute].name == "\"COL1\""
    )
    assert(
      plan1.analyzed.asInstanceOf[Pivot].pivotValues.head.asInstanceOf[Attribute].name == "\"C\""
    )

    val plan2 = Pivot(attr1, Seq.empty, Seq(attr3), child1)
    assert(plan2.aliasMap == map2)
    assert(
      plan2.analyzed.asInstanceOf[Pivot].pivotColumn.asInstanceOf[Attribute].name == "\"COL1\""
    )
    assert(
      plan2.analyzed.asInstanceOf[Pivot].aggregates.head.asInstanceOf[Attribute].name == "\"C\""
    )

    val plan3 = Pivot(attr3, Seq.empty, Seq.empty, child2)
    assert(plan3.aliasMap.isEmpty)
    assert(
      plan3.analyzed.asInstanceOf[Pivot].pivotColumn.asInstanceOf[Attribute].name == "\"COL3\""
    )
  }

  test("Filter - Analyzer") {
    val plan = Filter(attr3, child1)
    assert(plan.aliasMap == map2)
    assert(plan.analyzed.asInstanceOf[Filter].condition.asInstanceOf[Attribute].name == "\"C\"")

    val plan1 = Filter(attr3, child2)
    assert(plan1.aliasMap.isEmpty)
    assert(plan1.analyzed.asInstanceOf[Filter].condition.asInstanceOf[Attribute].name == "\"COL3\"")
  }

  test("Project - Analyzer") {
    val plan = Project(Seq(attr3), child1)
    assert(plan.aliasMap == map2)
    assert(
      plan.analyzed
        .asInstanceOf[Project]
        .projectList
        .head
        .asInstanceOf[Attribute]
        .name == "\"C\""
    )

    val plan1 = Project(Seq(attr3), child2)
    assert(plan1.aliasMap.isEmpty)
    assert(
      plan1.analyzed
        .asInstanceOf[Project]
        .projectList
        .head
        .asInstanceOf[Attribute]
        .name == "\"COL3\""
    )
  }

  test("ProjectAndFilter - Analyzer") {
    val plan = ProjectAndFilter(Seq(attr3), attr3, child1)
    assert(plan.aliasMap == map2)
    assert(
      plan.analyzed
        .asInstanceOf[ProjectAndFilter]
        .projectList
        .head
        .asInstanceOf[Attribute]
        .name == "\"C\""
    )
    assert(
      plan.analyzed
        .asInstanceOf[ProjectAndFilter]
        .condition
        .asInstanceOf[Attribute]
        .name == "\"C\""
    )

    val plan1 = ProjectAndFilter(Seq(attr3), attr3, child2)
    assert(plan1.aliasMap.isEmpty)
    assert(
      plan1.analyzed
        .asInstanceOf[ProjectAndFilter]
        .projectList
        .head
        .asInstanceOf[Attribute]
        .name == "\"COL3\""
    )
    assert(
      plan1.analyzed
        .asInstanceOf[ProjectAndFilter]
        .condition
        .asInstanceOf[Attribute]
        .name == "\"COL3\""
    )
  }

  test("CreateViewCommand - Analyzer") {
    val plan = CreateViewCommand("a", child1, LocalTempView)
    assert(plan.aliasMap == map2)
    assert(plan.analyzed.toString == plan.toString)

    val plan1 = CreateViewCommand("a", child2, LocalTempView)
    assert(plan1.aliasMap.isEmpty)
    assert(plan1.analyzed.toString == plan1.toString)
  }

  test("Lateral - Analyzer") {
    import com.snowflake.snowpark.internal.analyzer.{TableFunction => TableFunc}
    val tf = TableFunc("dummy", Seq(attr3))
    val plan1 = Lateral(child1, tf)
    assert(plan1.aliasMap == map2)
    assert(
      plan1.analyzed
        .asInstanceOf[Lateral]
        .tableFunction
        .asInstanceOf[TableFunc]
        .args
        .head
        .asInstanceOf[Attribute]
        .name == "\"C\""
    )

    val plan2 = Lateral(child2, tf)
    assert(plan2.aliasMap.isEmpty)
    assert(
      plan2.analyzed
        .asInstanceOf[Lateral]
        .tableFunction
        .asInstanceOf[TableFunc]
        .args
        .head
        .asInstanceOf[Attribute]
        .name == "\"COL3\""
    )
  }

  test("Limit - Analyzer") {
    val plan = Limit(attr3, child1)
    assert(plan.aliasMap == map2)
    assert(
      plan.analyzed
        .asInstanceOf[Limit]
        .limitExpr
        .asInstanceOf[Attribute]
        .name == "\"C\""
    )

    val plan1 = Limit(attr3, child2)
    assert(plan1.aliasMap.isEmpty)
    assert(
      plan1.analyzed
        .asInstanceOf[Limit]
        .limitExpr
        .asInstanceOf[Attribute]
        .name == "\"COL3\""
    )
  }

  test("TableFunctionJoin - Analyzer") {
    import com.snowflake.snowpark.internal.analyzer.{TableFunction => TableFunc}
    val tf = TableFunc("dummy", Seq(attr3))
    val plan1 = TableFunctionJoin(child1, tf, None)
    assert(plan1.aliasMap == map2)
    assert(
      plan1.analyzed
        .asInstanceOf[TableFunctionJoin]
        .tableFunction
        .asInstanceOf[TableFunc]
        .args
        .head
        .asInstanceOf[Attribute]
        .name == "\"C\""
    )

    val plan2 = TableFunctionJoin(child2, tf, None)
    assert(plan2.aliasMap.isEmpty)
    assert(
      plan2.analyzed
        .asInstanceOf[TableFunctionJoin]
        .tableFunction
        .asInstanceOf[TableFunc]
        .args
        .head
        .asInstanceOf[Attribute]
        .name == "\"COL3\""
    )
  }

  test("TableMerge - Analyzer") {
    val me = DeleteMergeExpression(Some(attr3))
    val plan1 = TableMerge("dummy", child1, attr3, Seq(me))
    assert(plan1.aliasMap == map2)
    assert(
      plan1.analyzed
        .asInstanceOf[TableMerge]
        .joinExpr
        .asInstanceOf[Attribute]
        .name == "\"C\""
    )
    assert(
      plan1.analyzed
        .asInstanceOf[TableMerge]
        .clauses
        .head
        .asInstanceOf[DeleteMergeExpression]
        .condition
        .get
        .asInstanceOf[Attribute]
        .name == "\"C\""
    )

    val plan2 = TableMerge("dummy", child2, attr3, Seq(me))
    assert(plan2.aliasMap.isEmpty)
    assert(
      plan2.analyzed
        .asInstanceOf[TableMerge]
        .joinExpr
        .asInstanceOf[Attribute]
        .name == "\"COL3\""
    )
    assert(
      plan2.analyzed
        .asInstanceOf[TableMerge]
        .clauses
        .head
        .asInstanceOf[DeleteMergeExpression]
        .condition
        .get
        .asInstanceOf[Attribute]
        .name == "\"COL3\""
    )
  }

  test("SnowflakeCreateTable - Analyzer") {
    val plan1 = SnowflakeCreateTable("dummy", SaveMode.Append, None)
    assert(plan1.aliasMap.isEmpty)
    assert(plan1.analyzed == plan1)

    val plan2 = SnowflakeCreateTable("dummy", SaveMode.Append, Some(child1))
    assert(plan2.aliasMap == map2)
    assert(plan2.analyzed.toString == plan2.toString)
  }

  test("TableUpdate - Analyzer") {
    val plan1 = TableUpdate("dummy", Map(attr3 -> attr3), Some(attr3), Some(child1))
    assert(plan1.aliasMap == map2)
    assert(
      plan1.analyzed
        .asInstanceOf[TableUpdate]
        .condition
        .get
        .asInstanceOf[Attribute]
        .name == "\"C\""
    )
    plan1.analyzed.asInstanceOf[TableUpdate].assignments.foreach {
      case (key: Attribute, value: Attribute) =>
        assert(key.name == "\"C\"")
        assert(value.name == "\"C\"")
    }

    val plan2 = TableUpdate("dummy", Map(attr3 -> attr3), Some(attr3), None)
    assert(plan2.aliasMap.isEmpty)
    assert(
      plan2.analyzed
        .asInstanceOf[TableUpdate]
        .condition
        .get
        .asInstanceOf[Attribute]
        .name == "\"COL3\""
    )
    plan2.analyzed.asInstanceOf[TableUpdate].assignments.foreach {
      case (key: Attribute, value: Attribute) =>
        assert(key.name == "\"COL3\"")
        assert(value.name == "\"COL3\"")
    }
  }

  test("TableDelete - Analyzer") {
    val plan1 = TableDelete("dummy", Some(attr3), Some(child1))
    assert(plan1.aliasMap == map2)
    assert(
      plan1.analyzed
        .asInstanceOf[TableDelete]
        .condition
        .get
        .asInstanceOf[Attribute]
        .name == "\"C\""
    )

    val plan2 = TableDelete("dummy", Some(attr3), None)
    assert(plan2.aliasMap.isEmpty)
    assert(
      plan2.analyzed
        .asInstanceOf[TableDelete]
        .condition
        .get
        .asInstanceOf[Attribute]
        .name == "\"COL3\""
    )
  }

  def binaryNodeAnalyzerChecker(func: (LogicalPlan, LogicalPlan) => LogicalPlan): Unit = {
    val plan1 = func(child1, child4)
    assert(plan1.aliasMap == map4)
    assert(plan1.analyzed.toString == plan1.toString)

    val plan2 = func(child1, child2)
    assert(plan2.aliasMap == map2)
    assert(plan2.analyzed.toString == plan2.toString)

    val plan3 = func(child2, child3)
    assert(plan3.aliasMap.isEmpty)
    assert(plan3.analyzed.toString == plan3.toString)

    val plan4 = func(child3, child4)
    assert(plan4.aliasMap == map3)
    assert(plan4.analyzed.toString == plan4.toString)
  }

  test("BinaryNode - Analyzer") {
    binaryNodeAnalyzerChecker(Except)
    binaryNodeAnalyzerChecker(Intersect)
    binaryNodeAnalyzerChecker(Union)
    binaryNodeAnalyzerChecker(UnionAll)
    binaryNodeAnalyzerChecker(Except)
    binaryNodeAnalyzerChecker(Join(_, _, LeftOuter, None))

    val plan = Join(child1, child4, LeftOuter, Some(attr3))
    assert(plan.aliasMap == map4)
    assert(
      plan.analyzed
        .asInstanceOf[Join]
        .condition
        .get
        .asInstanceOf[Attribute]
        .name == "\"C\""
    )

    val plan1 = Join(child2, child3, LeftOuter, Some(attr3))
    assert(plan1.aliasMap.isEmpty)
    assert(
      plan1.analyzed
        .asInstanceOf[Join]
        .condition
        .get
        .asInstanceOf[Attribute]
        .name == "\"COL3\""
    )
  }

  // updateChildren, simplifier
  def simplifierChecker(dataSize: Int, func: Seq[LogicalPlan] => LogicalPlan): Unit = {
    val testData = (0 until dataSize).map(Range(1, _, 1))
    val testFunc = (plan: LogicalPlan) =>
      plan match {
        // small change for future verification
        case Range(_, end, _) => Range(end, 1, 1)
        case _                => plan
      }
    val plan = func(testData)
    val newPlan = plan.updateChildren(testFunc)
    assert(newPlan.children.zipWithIndex.forall {
      case (Range(start, _, _), i) if start == i => true
      case _                                     => false
    })
  }
  def leafSimplifierChecker(plan: LogicalPlan): Unit = {
    val newPlan = plan.updateChildren(_ => Project(Seq.empty, Range(1, 1, 1)))
    assert(newPlan == plan)
  }

  def unarySimplifierChecker(func: LogicalPlan => LogicalPlan): Unit =
    simplifierChecker(1, data => func(data.head))

  def binarySimplifierChecker(func: (LogicalPlan, LogicalPlan) => LogicalPlan): Unit =
    simplifierChecker(2, data => func(data.head, data(1)))

  test("simplifier") {
    val tableFunction = internal.analyzer.TableFunction("dummy", Seq.empty)
    leafSimplifierChecker(TableFunctionRelation(tableFunction))
    leafSimplifierChecker(Range(1, 1, 2))
    leafSimplifierChecker(Generator(Seq(tableFunction), 1))
    leafSimplifierChecker(UnresolvedRelation("a"))
    leafSimplifierChecker(StoredProcedureRelation("a", Seq.empty))
    leafSimplifierChecker(SnowflakeValues(Seq.empty, Seq.empty))
    leafSimplifierChecker(CopyIntoNode("a", Seq.empty, Seq.empty, Map.empty, null))

    unarySimplifierChecker(SnowflakeSampleNode(Some(1.0), None, _))
    unarySimplifierChecker(WithColumns(Seq.empty, _))
    unarySimplifierChecker(DropColumns(Seq.empty, _))
    unarySimplifierChecker(Sort(Seq.empty, _))
    unarySimplifierChecker(Aggregate(Seq.empty, Seq.empty, _))
    unarySimplifierChecker(Pivot(null, Seq.empty, Seq.empty, _))
    unarySimplifierChecker(Filter(null, _))
    unarySimplifierChecker(Project(Seq.empty, _))
    unarySimplifierChecker(ProjectAndFilter(Seq.empty, null, _))
    unarySimplifierChecker(CopyIntoLocation(null, _))
    unarySimplifierChecker(CreateViewCommand("a", _, LocalTempView))
    unarySimplifierChecker(Lateral(_, tableFunction))
    unarySimplifierChecker(Limit(tableFunction, _))
    unarySimplifierChecker(LimitOnSort(_, tableFunction, Seq.empty))
    unarySimplifierChecker(TableFunctionJoin(_, tableFunction, None))
    unarySimplifierChecker(TableMerge("a", _, tableFunction, Seq.empty))

    binarySimplifierChecker(Except)
    binarySimplifierChecker(Intersect)
    binarySimplifierChecker(Union)
    binarySimplifierChecker(UnionAll)
    binarySimplifierChecker(Join(_, _, LeftOuter, None))

    unarySimplifierChecker(x => TableUpdate("a", Map.empty, None, Some(x)))
    unarySimplifierChecker(x => TableDelete("a", None, Some(x)))
    unarySimplifierChecker(x => SnowflakeCreateTable("a", SaveMode.Append, Some(x)))
    leafSimplifierChecker(SnowflakePlan(Seq.empty, "222", session, None, supportAsyncMode = false))

  }
}
