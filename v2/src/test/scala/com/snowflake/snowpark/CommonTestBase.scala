package com.snowflake.snowpark

import com.snowflake.snowpark.types.{AtomicType, DataType, FractionalType, IntegralType, NumericType, StructType}
import org.scalatest.funsuite.AnyFunSuite

trait CommonTestBase extends AnyFunSuite{

  def treeString(schema: StructType, layer: Int): String = schema.treeString(layer)
  def isAtomicType(tpe: DataType): Boolean = tpe.isInstanceOf[AtomicType]
  def isNumericType(tpe: DataType): Boolean = tpe.isInstanceOf[NumericType]
  def isIntegralType(tpe: DataType): Boolean = tpe.isInstanceOf[IntegralType]
  def isFractionalType(tpe: DataType): Boolean = tpe.isInstanceOf[FractionalType]
}
