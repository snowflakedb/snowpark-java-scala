package com.snowflake.snowpark

import com.snowflake.snowpark.internal.{ExprNode, NameIndices, SrcPositionInfo, StmtNode}
import com.snowflake.snowpark.types.{
  AtomicType,
  DataType,
  FractionalType,
  IntegralType,
  NumericType,
  StructType
}
import org.scalatest.funsuite.AnyFunSuite
import scalapb.GeneratedMessage

import scala.reflect.ClassTag
import scala.util.Random

trait CommonTestBase extends AnyFunSuite {

  def treeString(schema: StructType, layer: Int): String = schema.treeString(layer)
  def isAtomicType(tpe: DataType): Boolean = tpe.isInstanceOf[AtomicType]
  def isNumericType(tpe: DataType): Boolean = tpe.isInstanceOf[NumericType]
  def isIntegralType(tpe: DataType): Boolean = tpe.isInstanceOf[IntegralType]
  def isFractionalType(tpe: DataType): Boolean = tpe.isInstanceOf[FractionalType]

  def checkNameIndices(expected: Set[Int], actual: NameIndices): Unit = {
    assert(actual.nameIndices == expected)
  }

  def checkAst(
      expected: GeneratedMessage,
      actual: ExprNode,
      expectedNameIndices: Set[Int]): Unit = {
    checkAst(expected, actual.expr)
    checkNameIndices(expectedNameIndices, actual)
  }

  def checkAst(expected: GeneratedMessage, actual: ExprNode): Unit = {
    checkAst(expected, actual.expr)
  }

  def checkAst(expected: GeneratedMessage, actual: GeneratedMessage): Unit = {
    assert(actual == expected)
  }

  def checkException[T <: Throwable](msg: String)(f: => Any)(implicit
      classTag: ClassTag[T]): Unit = {
    val thrown = intercept[T](f)
    assert(thrown.getMessage.contains(msg))
  }

  def randomName: String = Random.alphanumeric.take(10).mkString
}
