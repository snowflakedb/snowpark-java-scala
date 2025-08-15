package com.snowflake.snowpark.internal.analyzer

private[snowpark] trait BinaryExpression extends Expression {

  def left: Expression
  def right: Expression
  def sqlOperator: String

  override def toString: String = s"($left $sqlOperator $right)"
  override final def children: Seq[Expression] = Seq(left, right)
  override def nullable: Boolean = left.nullable || right.nullable

  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    createAnalyzedBinary(analyzedChildren.head, analyzedChildren(1))

  protected val createAnalyzedBinary: (Expression, Expression) => Expression
}

trait BinaryArithmeticExpression extends BinaryExpression

case class EqualTo(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def sqlOperator: String = "="

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression = EqualTo
}

case class NotEqualTo(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def sqlOperator: String = "<>"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression = NotEqualTo
}

case class GreaterThan(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def sqlOperator: String = ">"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression =
    GreaterThan
}

case class LessThan(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def sqlOperator: String = "<"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression = LessThan
}

case class LessThanOrEqual(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def sqlOperator: String = "<="

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression =
    LessThanOrEqual
}

case class GreaterThanOrEqual(left: Expression, right: Expression)
    extends BinaryArithmeticExpression {
  override def sqlOperator: String = ">="

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression =
    GreaterThanOrEqual
}

case class EqualNullSafe(left: Expression, right: Expression) extends BinaryExpression {
  override def sqlOperator: String = "EQUAL_NULL"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression =
    EqualNullSafe
}

case class Or(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def sqlOperator: String = "OR"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression = Or
}

case class And(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def sqlOperator: String = "AND"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression = And
}

case class Add(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def sqlOperator: String = "+"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression = Add
}

case class Subtract(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def sqlOperator: String = "-"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression = Subtract
}

case class Multiply(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def sqlOperator: String = "*"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression = Multiply
}

case class Divide(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def sqlOperator: String = "/"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression = Divide
}

case class Remainder(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def sqlOperator: String = "%"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression = Remainder
}

case class BitwiseAnd(left: Expression, right: Expression) extends BinaryExpression {
  override def sqlOperator: String = "BITAND"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression = BitwiseAnd
}

case class BitwiseOr(left: Expression, right: Expression) extends BinaryExpression {
  override def sqlOperator: String = "BITOR"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression = BitwiseOr
}

case class BitwiseXor(left: Expression, right: Expression) extends BinaryExpression {
  override def sqlOperator: String = "BITXOR"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression = BitwiseXor
}

case class ShiftLeft(left: Expression, right: Expression) extends BinaryExpression {
  override def sqlOperator: String = "BITSHIFTLEFT"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression = ShiftLeft
}

case class ShiftRight(left: Expression, right: Expression) extends BinaryExpression {
  override def sqlOperator: String = "BITSHIFTRIGHT"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression = ShiftRight
}

case class Logarithm(left: Expression, right: Expression) extends BinaryExpression {
  override def sqlOperator: String = "LOG"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression = Logarithm
}

case class Atan2(left: Expression, right: Expression) extends BinaryExpression {
  override def sqlOperator: String = "ATAN2"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression = Atan2
}

case class Round(left: Expression, right: Expression) extends BinaryExpression {
  override def sqlOperator: String = "ROUND"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression = Round
}

case class Pow(left: Expression, right: Expression) extends BinaryExpression {
  override def sqlOperator: String = "POWER"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression = Pow
}

case class Sha2(left: Expression, right: Expression) extends BinaryExpression {
  override def sqlOperator: String = "SHA2"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression = Sha2
}

case class StringRepeat(left: Expression, right: Expression) extends BinaryExpression {
  override def sqlOperator: String = "REPEAT"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression =
    StringRepeat
}

case class AddMonths(left: Expression, right: Expression) extends BinaryExpression {
  override def sqlOperator: String = "ADD_MONTHS"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression = AddMonths
}

case class NextDay(left: Expression, right: Expression) extends BinaryExpression {
  override def sqlOperator: String = "NEXT_DAY"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression = NextDay
}

case class Trunc(left: Expression, right: Expression) extends BinaryExpression {
  override def sqlOperator: String = "TRUNC"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression = Trunc
}

case class DateTrunc(left: Expression, right: Expression) extends BinaryExpression {
  override def sqlOperator: String = "DATE_TRUNC"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression = DateTrunc
}

case class ArraysOverlap(left: Expression, right: Expression) extends BinaryExpression {
  override def sqlOperator: String = "ARRAYS_OVERLAP"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression =
    ArraysOverlap
}

case class ArrayIntersect(left: Expression, right: Expression) extends BinaryExpression {
  override def sqlOperator: String = "ARRAY_INTERSECTION"

  override protected val createAnalyzedBinary: (Expression, Expression) => Expression =
    ArrayIntersect
}
