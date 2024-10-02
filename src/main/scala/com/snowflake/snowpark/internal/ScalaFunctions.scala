package com.snowflake.snowpark.internal

import com.snowflake.snowpark.{Session, StoredProcedure, UserDefinedFunction}
import com.snowflake.snowpark.types._
import com.snowflake.snowpark.udtf._
import com.snowflake.snowpark_java.udf._
import com.snowflake.snowpark_java.udtf._
import com.snowflake.snowpark_java.sproc._

import java.lang.reflect.Method
import scala.collection.JavaConverters._

case class UdfColumnSchema(dataType: DataType, isOption: Boolean = false)
case class UdfColumn(schema: UdfColumnSchema, name: String)

object ScalaFunctions {

  // scalastyle:off line.size.limit

  import scala.reflect.runtime.universe._

  private def baseType(tpe: `Type`): `Type` = {
    tpe.dealias match {
      case annotatedType: AnnotatedType => annotatedType.underlying
      case other => other
    }
  }
  private def typeOf[T: TypeTag]: `Type` = {
    scala.reflect.runtime.universe.typeOf[T]
  }

  private def isSupported(tpe: `Type`): Boolean = baseType(tpe) match {
    case t if t =:= typeOf[Option[Short]] => true
    case t if t =:= typeOf[Option[Int]] => true
    case t if t =:= typeOf[Option[Float]] => true
    case t if t =:= typeOf[Option[Double]] => true
    case t if t =:= typeOf[Option[Long]] => true
    case t if t =:= typeOf[Option[Boolean]] => true
    case t if t =:= typeOf[Short] => true
    case t if t =:= typeOf[Int] => true
    case t if t =:= typeOf[Float] => true
    case t if t =:= typeOf[Double] => true
    case t if t =:= typeOf[Long] => true
    case t if t =:= typeOf[Boolean] => true
    case t if t =:= typeOf[String] => true
    case t if t =:= typeOf[java.lang.String] => true
    case t if t =:= typeOf[java.math.BigDecimal] => true
    case t if t =:= typeOf[java.math.BigInteger] => true
    case t if t =:= typeOf[java.sql.Date] => true
    case t if t =:= typeOf[java.sql.Time] => true
    case t if t =:= typeOf[java.sql.Timestamp] => true
    case t if t =:= typeOf[Array[Byte]] => true
    case t if t =:= typeOf[Array[String]] => true
    case t if t =:= typeOf[Array[Variant]] => true
    case t if t =:= typeOf[scala.collection.mutable.Map[String, String]] => true
    case t if t =:= typeOf[scala.collection.mutable.Map[String, Variant]] => true
    case t if t =:= typeOf[Geography] => true
    case t if t =:= typeOf[Geometry] => true
    case t if t =:= typeOf[Variant] => true
    case t if t <:< typeOf[scala.collection.Iterable[_]] =>
      throw new UnsupportedOperationException(
        s"Unsupported type $t for Scala UDFs. Supported collection types are " +
          s"Array[Byte], Array[String] and mutable.Map[String, String]")
    case _ => throw new UnsupportedOperationException(s"Unsupported type $tpe")
  }

  private[snowpark] val SYSTEM_DEFAULT: DecimalType = DecimalType(38, 18)
  private[snowpark] val BigIntDecimal = DecimalType(38, 0)

  // This is a simplified version for ScalaReflection.schemaFor().
  // If more types need to be supported, that function is a good reference.
  private def schemaForWrapper[T: TypeTag]: UdfColumnSchema = baseType(typeOf[T]) match {
    case t if t =:= typeOf[Option[Short]] => UdfColumnSchema(ShortType, isOption = true)
    case t if t =:= typeOf[Option[Int]] => UdfColumnSchema(IntegerType, isOption = true)
    case t if t =:= typeOf[Option[Float]] => UdfColumnSchema(FloatType, isOption = true)
    case t if t =:= typeOf[Option[Double]] => UdfColumnSchema(DoubleType, isOption = true)
    case t if t =:= typeOf[Option[Long]] => UdfColumnSchema(LongType, isOption = true)
    case t if t =:= typeOf[Option[Boolean]] => UdfColumnSchema(BooleanType, isOption = true)
    case t if t =:= typeOf[Short] => UdfColumnSchema(ShortType)
    case t if t =:= typeOf[Int] => UdfColumnSchema(IntegerType)
    case t if t =:= typeOf[Float] => UdfColumnSchema(FloatType)
    case t if t =:= typeOf[Double] => UdfColumnSchema(DoubleType)
    case t if t =:= typeOf[Long] => UdfColumnSchema(LongType)
    case t if t =:= typeOf[Boolean] => UdfColumnSchema(BooleanType)
    case t if t =:= typeOf[String] => UdfColumnSchema(StringType)
    // This is the only case need test.
    case t if t =:= typeOf[java.lang.String] => UdfColumnSchema(StringType)
    case t if t =:= typeOf[java.math.BigDecimal] => UdfColumnSchema(SYSTEM_DEFAULT)
    case t if t =:= typeOf[java.math.BigInteger] => UdfColumnSchema(BigIntDecimal)
    case t if t =:= typeOf[java.sql.Date] => UdfColumnSchema(DateType)
    case t if t =:= typeOf[java.sql.Time] => UdfColumnSchema(TimeType)
    case t if t =:= typeOf[java.sql.Timestamp] => UdfColumnSchema(TimestampType)
    case t if t =:= typeOf[Array[Byte]] => UdfColumnSchema(BinaryType)
    case t if t =:= typeOf[Array[String]] => UdfColumnSchema(ArrayType(StringType))
    case t if t =:= typeOf[Array[Variant]] => UdfColumnSchema(ArrayType(VariantType))
    case t if t =:= typeOf[scala.collection.mutable.Map[String, String]] =>
      UdfColumnSchema(MapType(StringType, StringType))
    case t if t =:= typeOf[scala.collection.mutable.Map[String, Variant]] =>
      UdfColumnSchema(MapType(StringType, VariantType))
    case t if t =:= typeOf[Geography] => UdfColumnSchema(GeographyType)
    case t if t =:= typeOf[Geometry] => UdfColumnSchema(GeometryType)
    case t if t =:= typeOf[Variant] => UdfColumnSchema(VariantType)
    case t => throw new UnsupportedOperationException(s"Unsupported type $t")
  }

  /* Code below for _toSProc 0-21 generated by this script
   * (0 to 21).foreach { x =>
   *   val types = (1 to x).foldLeft("_")((i, _) => {s"$i, _"})
   *   val (input, args) = x match {
   *     case 0 => ("", "Nil")
   *     case 1 => (" input: DataType,","UdfColumnSchema(input) :: Nil")
   *     case _ => (" input: Array[DataType],","input.map(UdfColumnSchema(_))")
   *   }
   *   println(s"""
   *     |def _toSProc(func: JavaSProc$x[$types],$input output: DataType): StoredProcedure =
   *     |StoredProcedure(
   *     |  func,
   *     |  UdfColumnSchema(output),
   *     |  $args)""".stripMargin)
   * }
   * */
  def _toSProc(func: JavaSProc0[_], output: DataType): StoredProcedure =
    StoredProcedure(func, UdfColumnSchema(output), Nil)

  def _toSProc(func: JavaSProc1[_, _], input: DataType, output: DataType): StoredProcedure =
    StoredProcedure(func, UdfColumnSchema(output), UdfColumnSchema(input) :: Nil)

  def _toSProc(
      func: JavaSProc2[_, _, _],
      input: Array[DataType],
      output: DataType): StoredProcedure =
    StoredProcedure(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toSProc(
      func: JavaSProc3[_, _, _, _],
      input: Array[DataType],
      output: DataType): StoredProcedure =
    StoredProcedure(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toSProc(
      func: JavaSProc4[_, _, _, _, _],
      input: Array[DataType],
      output: DataType): StoredProcedure =
    StoredProcedure(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toSProc(
      func: JavaSProc5[_, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): StoredProcedure =
    StoredProcedure(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toSProc(
      func: JavaSProc6[_, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): StoredProcedure =
    StoredProcedure(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toSProc(
      func: JavaSProc7[_, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): StoredProcedure =
    StoredProcedure(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toSProc(
      func: JavaSProc8[_, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): StoredProcedure =
    StoredProcedure(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toSProc(
      func: JavaSProc9[_, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): StoredProcedure =
    StoredProcedure(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toSProc(
      func: JavaSProc10[_, _, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): StoredProcedure =
    StoredProcedure(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toSProc(
      func: JavaSProc11[_, _, _, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): StoredProcedure =
    StoredProcedure(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toSProc(
      func: JavaSProc12[_, _, _, _, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): StoredProcedure =
    StoredProcedure(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toSProc(
      func: JavaSProc13[_, _, _, _, _, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): StoredProcedure =
    StoredProcedure(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toSProc(
      func: JavaSProc14[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): StoredProcedure =
    StoredProcedure(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toSProc(
      func: JavaSProc15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): StoredProcedure =
    StoredProcedure(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toSProc(
      func: JavaSProc16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): StoredProcedure =
    StoredProcedure(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toSProc(
      func: JavaSProc17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): StoredProcedure =
    StoredProcedure(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toSProc(
      func: JavaSProc18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): StoredProcedure =
    StoredProcedure(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toSProc(
      func: JavaSProc19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): StoredProcedure =
    StoredProcedure(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toSProc(
      func: JavaSProc20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): StoredProcedure =
    StoredProcedure(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toSProc(
      func: JavaSProc21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): StoredProcedure =
    StoredProcedure(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  /* Code below for _toUdf 0-22 generated by this script
   * (0 to 22).foreach { x =>
   *  val types = (1 to x).foldLeft("_")((i, _) => {s"$i, _"})
   *  val (input, args) = x match {
   *    case 0 => ("", "Nil")
   *    case 1 => (" input: DataType,","UdfColumnSchema(input) :: Nil")
   *    case _ => (" input: Array[DataType],","input.map(UdfColumnSchema(_))")
   *  }
   *  println(s"""
   *    |def _toUdf(func: JavaUDF$x[$types],$input output: DataType): UserDefinedFunction =
   *    |UserDefinedFunction(
   *    |  func,
   *    |  UdfColumnSchema(output),
   *    |  $args)""".stripMargin)
   * }
   * */

  def _toUdf(func: JavaUDF0[_], output: DataType): UserDefinedFunction =
    UserDefinedFunction(func, UdfColumnSchema(output), Nil)

  def _toUdf(func: JavaUDF1[_, _], input: DataType, output: DataType): UserDefinedFunction =
    UserDefinedFunction(func, UdfColumnSchema(output), UdfColumnSchema(input) :: Nil)

  def _toUdf(
      func: JavaUDF2[_, _, _],
      input: Array[DataType],
      output: DataType): UserDefinedFunction =
    UserDefinedFunction(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toUdf(
      func: JavaUDF3[_, _, _, _],
      input: Array[DataType],
      output: DataType): UserDefinedFunction =
    UserDefinedFunction(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toUdf(
      func: JavaUDF4[_, _, _, _, _],
      input: Array[DataType],
      output: DataType): UserDefinedFunction =
    UserDefinedFunction(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toUdf(
      func: JavaUDF5[_, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): UserDefinedFunction =
    UserDefinedFunction(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toUdf(
      func: JavaUDF6[_, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): UserDefinedFunction =
    UserDefinedFunction(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toUdf(
      func: JavaUDF7[_, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): UserDefinedFunction =
    UserDefinedFunction(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toUdf(
      func: JavaUDF8[_, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): UserDefinedFunction =
    UserDefinedFunction(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toUdf(
      func: JavaUDF9[_, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): UserDefinedFunction =
    UserDefinedFunction(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toUdf(
      func: JavaUDF10[_, _, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): UserDefinedFunction =
    UserDefinedFunction(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toUdf(
      func: JavaUDF11[_, _, _, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): UserDefinedFunction =
    UserDefinedFunction(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toUdf(
      func: JavaUDF12[_, _, _, _, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): UserDefinedFunction =
    UserDefinedFunction(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toUdf(
      func: JavaUDF13[_, _, _, _, _, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): UserDefinedFunction =
    UserDefinedFunction(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toUdf(
      func: JavaUDF14[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): UserDefinedFunction =
    UserDefinedFunction(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toUdf(
      func: JavaUDF15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): UserDefinedFunction =
    UserDefinedFunction(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toUdf(
      func: JavaUDF16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): UserDefinedFunction =
    UserDefinedFunction(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toUdf(
      func: JavaUDF17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): UserDefinedFunction =
    UserDefinedFunction(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toUdf(
      func: JavaUDF18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): UserDefinedFunction =
    UserDefinedFunction(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toUdf(
      func: JavaUDF19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): UserDefinedFunction =
    UserDefinedFunction(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toUdf(
      func: JavaUDF20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): UserDefinedFunction =
    UserDefinedFunction(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toUdf(
      func: JavaUDF21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): UserDefinedFunction =
    UserDefinedFunction(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  def _toUdf(
      func: JavaUDF22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _],
      input: Array[DataType],
      output: DataType): UserDefinedFunction =
    UserDefinedFunction(func, UdfColumnSchema(output), input.map(UdfColumnSchema(_)))

  /* Code below for _toUdf 0-22 generated by this script
 (0 to 22).foreach { x =>
   val types = (1 to x).foldRight("RT")((i, s) => {s"A$i, $s"})
   val typeOfs = (1 to x).map(i => s"A$i").toSeq.map(t => s"typeOf[$t]")
   val typeTags = (1 to x).map(i => s"A$i: TypeTag").foldLeft("RT: TypeTag")(_ + ", " + _)
   val inputTypes = (1 to x).foldRight("Nil")((i, s) => {s"schemaForWrapper[A$i] :: $s"})
   println(s"""
     |/**
     | * Creates a Scala closure of $x arguments as user-defined function (UDF).
     | * @tparam RT return type of UDF.
     | */
     |def _toUdf[$typeTags](func: Function$x[$types]): UserDefinedFunction = {
     |  $typeOfs.foreach(isSupported(_))
     |  isSupported(typeOf[RT])
     |  val returnColumn = schemaForWrapper[RT]
     |  val inputColumns: Seq[UdfColumnSchema] = $inputTypes
     |  UserDefinedFunction(func, returnColumn, inputColumns)
     |}""".stripMargin)
 }
   */

  /**
   * Creates a Scala closure of 0 arguments as user-defined function (UDF).
   * @tparam RT
   *   return type of UDF.
   */
  def _toUdf[RT: TypeTag](func: Function0[RT]): UserDefinedFunction = {
    Vector().foreach(isSupported(_))
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] = Nil
    UserDefinedFunction(func, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 1 arguments as user-defined function (UDF).
   * @tparam RT
   *   return type of UDF.
   */
  def _toUdf[RT: TypeTag, A1: TypeTag](func: Function1[A1, RT]): UserDefinedFunction = {
    Vector(typeOf[A1]).foreach(isSupported(_))
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] = schemaForWrapper[A1] :: Nil
    UserDefinedFunction(func, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 2 arguments as user-defined function (UDF).
   * @tparam RT
   *   return type of UDF.
   */
  def _toUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag](
      func: Function2[A1, A2, RT]): UserDefinedFunction = {
    Vector(typeOf[A1], typeOf[A2]).foreach(isSupported(_))
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] = schemaForWrapper[A1] :: schemaForWrapper[A2] :: Nil
    UserDefinedFunction(func, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 3 arguments as user-defined function (UDF).
   * @tparam RT
   *   return type of UDF.
   */
  def _toUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag](
      func: Function3[A1, A2, A3, RT]): UserDefinedFunction = {
    Vector(typeOf[A1], typeOf[A2], typeOf[A3]).foreach(isSupported(_))
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] =
      schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[A3] :: Nil
    UserDefinedFunction(func, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 4 arguments as user-defined function (UDF).
   * @tparam RT
   *   return type of UDF.
   */
  def _toUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag](
      func: Function4[A1, A2, A3, A4, RT]): UserDefinedFunction = {
    Vector(typeOf[A1], typeOf[A2], typeOf[A3], typeOf[A4]).foreach(isSupported(_))
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] = schemaForWrapper[A1] :: schemaForWrapper[
      A2] :: schemaForWrapper[A3] :: schemaForWrapper[A4] :: Nil
    UserDefinedFunction(func, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 5 arguments as user-defined function (UDF).
   * @tparam RT
   *   return type of UDF.
   */
  def _toUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag](
      func: Function5[A1, A2, A3, A4, A5, RT]): UserDefinedFunction = {
    Vector(typeOf[A1], typeOf[A2], typeOf[A3], typeOf[A4], typeOf[A5]).foreach(isSupported(_))
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] = schemaForWrapper[A1] :: schemaForWrapper[
      A2] :: schemaForWrapper[A3] :: schemaForWrapper[A4] :: schemaForWrapper[A5] :: Nil
    UserDefinedFunction(func, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 6 arguments as user-defined function (UDF).
   * @tparam RT
   *   return type of UDF.
   */
  def _toUdf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag](func: Function6[A1, A2, A3, A4, A5, A6, RT]): UserDefinedFunction = {
    Vector(typeOf[A1], typeOf[A2], typeOf[A3], typeOf[A4], typeOf[A5], typeOf[A6])
      .foreach(isSupported(_))
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] =
      schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[A3] :: schemaForWrapper[
        A4] :: schemaForWrapper[A5] :: schemaForWrapper[A6] :: Nil
    UserDefinedFunction(func, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 7 arguments as user-defined function (UDF).
   * @tparam RT
   *   return type of UDF.
   */
  def _toUdf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag](func: Function7[A1, A2, A3, A4, A5, A6, A7, RT]): UserDefinedFunction = {
    Vector(typeOf[A1], typeOf[A2], typeOf[A3], typeOf[A4], typeOf[A5], typeOf[A6], typeOf[A7])
      .foreach(isSupported(_))
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] =
      schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[A3] :: schemaForWrapper[
        A4] :: schemaForWrapper[A5] :: schemaForWrapper[A6] :: schemaForWrapper[A7] :: Nil
    UserDefinedFunction(func, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 8 arguments as user-defined function (UDF).
   * @tparam RT
   *   return type of UDF.
   */
  def _toUdf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag](func: Function8[A1, A2, A3, A4, A5, A6, A7, A8, RT]): UserDefinedFunction = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8]).foreach(isSupported(_))
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] = schemaForWrapper[A1] :: schemaForWrapper[
      A2] :: schemaForWrapper[A3] :: schemaForWrapper[A4] :: schemaForWrapper[
      A5] :: schemaForWrapper[A6] :: schemaForWrapper[A7] :: schemaForWrapper[A8] :: Nil
    UserDefinedFunction(func, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 9 arguments as user-defined function (UDF).
   * @tparam RT
   *   return type of UDF.
   */
  def _toUdf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag](func: Function9[A1, A2, A3, A4, A5, A6, A7, A8, A9, RT]): UserDefinedFunction = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9]).foreach(isSupported(_))
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns
        : Seq[UdfColumnSchema] = schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[
      A3] :: schemaForWrapper[A4] :: schemaForWrapper[A5] :: schemaForWrapper[
      A6] :: schemaForWrapper[A7] :: schemaForWrapper[A8] :: schemaForWrapper[A9] :: Nil
    UserDefinedFunction(func, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 10 arguments as user-defined function (UDF).
   * @tparam RT
   *   return type of UDF.
   */
  def _toUdf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag](
      func: Function10[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT]): UserDefinedFunction = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9],
      typeOf[A10]).foreach(isSupported(_))
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] =
      schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[A3] :: schemaForWrapper[
        A4] :: schemaForWrapper[A5] :: schemaForWrapper[A6] :: schemaForWrapper[
        A7] :: schemaForWrapper[A8] :: schemaForWrapper[A9] :: schemaForWrapper[A10] :: Nil
    UserDefinedFunction(func, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 11 arguments as user-defined function (UDF).
   * @tparam RT
   *   return type of UDF.
   */
  def _toUdf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag](
      func: Function11[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, RT]): UserDefinedFunction = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9],
      typeOf[A10],
      typeOf[A11]).foreach(isSupported(_))
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] =
      schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[A3] :: schemaForWrapper[
        A4] :: schemaForWrapper[A5] :: schemaForWrapper[A6] :: schemaForWrapper[
        A7] :: schemaForWrapper[A8] :: schemaForWrapper[A9] :: schemaForWrapper[
        A10] :: schemaForWrapper[A11] :: Nil
    UserDefinedFunction(func, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 12 arguments as user-defined function (UDF).
   * @tparam RT
   *   return type of UDF.
   */
  def _toUdf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag](func: Function12[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, RT])
      : UserDefinedFunction = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9],
      typeOf[A10],
      typeOf[A11],
      typeOf[A12]).foreach(isSupported(_))
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns
        : Seq[UdfColumnSchema] = schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[
      A3] :: schemaForWrapper[A4] :: schemaForWrapper[A5] :: schemaForWrapper[
      A6] :: schemaForWrapper[A7] :: schemaForWrapper[A8] :: schemaForWrapper[
      A9] :: schemaForWrapper[A10] :: schemaForWrapper[A11] :: schemaForWrapper[A12] :: Nil
    UserDefinedFunction(func, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 13 arguments as user-defined function (UDF).
   * @tparam RT
   *   return type of UDF.
   */
  def _toUdf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag](func: Function13[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, RT])
      : UserDefinedFunction = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9],
      typeOf[A10],
      typeOf[A11],
      typeOf[A12],
      typeOf[A13]).foreach(isSupported(_))
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] =
      schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[A3] :: schemaForWrapper[
        A4] :: schemaForWrapper[A5] :: schemaForWrapper[A6] :: schemaForWrapper[
        A7] :: schemaForWrapper[A8] :: schemaForWrapper[A9] :: schemaForWrapper[
        A10] :: schemaForWrapper[A11] :: schemaForWrapper[A12] :: schemaForWrapper[A13] :: Nil
    UserDefinedFunction(func, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 14 arguments as user-defined function (UDF).
   * @tparam RT
   *   return type of UDF.
   */
  def _toUdf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag,
      A14: TypeTag](
      func: Function14[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, RT])
      : UserDefinedFunction = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9],
      typeOf[A10],
      typeOf[A11],
      typeOf[A12],
      typeOf[A13],
      typeOf[A14]).foreach(isSupported(_))
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] =
      schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[A3] :: schemaForWrapper[
        A4] :: schemaForWrapper[A5] :: schemaForWrapper[A6] :: schemaForWrapper[
        A7] :: schemaForWrapper[A8] :: schemaForWrapper[A9] :: schemaForWrapper[
        A10] :: schemaForWrapper[A11] :: schemaForWrapper[A12] :: schemaForWrapper[
        A13] :: schemaForWrapper[A14] :: Nil
    UserDefinedFunction(func, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 15 arguments as user-defined function (UDF).
   * @tparam RT
   *   return type of UDF.
   */
  def _toUdf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag,
      A14: TypeTag,
      A15: TypeTag](
      func: Function15[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, RT])
      : UserDefinedFunction = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9],
      typeOf[A10],
      typeOf[A11],
      typeOf[A12],
      typeOf[A13],
      typeOf[A14],
      typeOf[A15]).foreach(isSupported(_))
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns
        : Seq[UdfColumnSchema] = schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[
      A3] :: schemaForWrapper[A4] :: schemaForWrapper[A5] :: schemaForWrapper[
      A6] :: schemaForWrapper[A7] :: schemaForWrapper[A8] :: schemaForWrapper[
      A9] :: schemaForWrapper[A10] :: schemaForWrapper[A11] :: schemaForWrapper[
      A12] :: schemaForWrapper[A13] :: schemaForWrapper[A14] :: schemaForWrapper[A15] :: Nil
    UserDefinedFunction(func, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 16 arguments as user-defined function (UDF).
   * @tparam RT
   *   return type of UDF.
   */
  def _toUdf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag,
      A14: TypeTag,
      A15: TypeTag,
      A16: TypeTag](
      func: Function16[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, RT])
      : UserDefinedFunction = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9],
      typeOf[A10],
      typeOf[A11],
      typeOf[A12],
      typeOf[A13],
      typeOf[A14],
      typeOf[A15],
      typeOf[A16]).foreach(isSupported(_))
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] =
      schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[A3] :: schemaForWrapper[
        A4] :: schemaForWrapper[A5] :: schemaForWrapper[A6] :: schemaForWrapper[
        A7] :: schemaForWrapper[A8] :: schemaForWrapper[A9] :: schemaForWrapper[
        A10] :: schemaForWrapper[A11] :: schemaForWrapper[A12] :: schemaForWrapper[
        A13] :: schemaForWrapper[A14] :: schemaForWrapper[A15] :: schemaForWrapper[A16] :: Nil
    UserDefinedFunction(func, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 17 arguments as user-defined function (UDF).
   * @tparam RT
   *   return type of UDF.
   */
  def _toUdf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag,
      A14: TypeTag,
      A15: TypeTag,
      A16: TypeTag,
      A17: TypeTag](
      func: Function17[
        A1,
        A2,
        A3,
        A4,
        A5,
        A6,
        A7,
        A8,
        A9,
        A10,
        A11,
        A12,
        A13,
        A14,
        A15,
        A16,
        A17,
        RT]): UserDefinedFunction = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9],
      typeOf[A10],
      typeOf[A11],
      typeOf[A12],
      typeOf[A13],
      typeOf[A14],
      typeOf[A15],
      typeOf[A16],
      typeOf[A17]).foreach(isSupported(_))
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] =
      schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[A3] :: schemaForWrapper[
        A4] :: schemaForWrapper[A5] :: schemaForWrapper[A6] :: schemaForWrapper[
        A7] :: schemaForWrapper[A8] :: schemaForWrapper[A9] :: schemaForWrapper[
        A10] :: schemaForWrapper[A11] :: schemaForWrapper[A12] :: schemaForWrapper[
        A13] :: schemaForWrapper[A14] :: schemaForWrapper[A15] :: schemaForWrapper[
        A16] :: schemaForWrapper[A17] :: Nil
    UserDefinedFunction(func, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 18 arguments as user-defined function (UDF).
   * @tparam RT
   *   return type of UDF.
   */
  def _toUdf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag,
      A14: TypeTag,
      A15: TypeTag,
      A16: TypeTag,
      A17: TypeTag,
      A18: TypeTag](
      func: Function18[
        A1,
        A2,
        A3,
        A4,
        A5,
        A6,
        A7,
        A8,
        A9,
        A10,
        A11,
        A12,
        A13,
        A14,
        A15,
        A16,
        A17,
        A18,
        RT]): UserDefinedFunction = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9],
      typeOf[A10],
      typeOf[A11],
      typeOf[A12],
      typeOf[A13],
      typeOf[A14],
      typeOf[A15],
      typeOf[A16],
      typeOf[A17],
      typeOf[A18]).foreach(isSupported(_))
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns
        : Seq[UdfColumnSchema] = schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[
      A3] :: schemaForWrapper[A4] :: schemaForWrapper[A5] :: schemaForWrapper[
      A6] :: schemaForWrapper[A7] :: schemaForWrapper[A8] :: schemaForWrapper[
      A9] :: schemaForWrapper[A10] :: schemaForWrapper[A11] :: schemaForWrapper[
      A12] :: schemaForWrapper[A13] :: schemaForWrapper[A14] :: schemaForWrapper[
      A15] :: schemaForWrapper[A16] :: schemaForWrapper[A17] :: schemaForWrapper[A18] :: Nil
    UserDefinedFunction(func, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 19 arguments as user-defined function (UDF).
   * @tparam RT
   *   return type of UDF.
   */
  def _toUdf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag,
      A14: TypeTag,
      A15: TypeTag,
      A16: TypeTag,
      A17: TypeTag,
      A18: TypeTag,
      A19: TypeTag](
      func: Function19[
        A1,
        A2,
        A3,
        A4,
        A5,
        A6,
        A7,
        A8,
        A9,
        A10,
        A11,
        A12,
        A13,
        A14,
        A15,
        A16,
        A17,
        A18,
        A19,
        RT]): UserDefinedFunction = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9],
      typeOf[A10],
      typeOf[A11],
      typeOf[A12],
      typeOf[A13],
      typeOf[A14],
      typeOf[A15],
      typeOf[A16],
      typeOf[A17],
      typeOf[A18],
      typeOf[A19]).foreach(isSupported(_))
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] =
      schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[A3] :: schemaForWrapper[
        A4] :: schemaForWrapper[A5] :: schemaForWrapper[A6] :: schemaForWrapper[
        A7] :: schemaForWrapper[A8] :: schemaForWrapper[A9] :: schemaForWrapper[
        A10] :: schemaForWrapper[A11] :: schemaForWrapper[A12] :: schemaForWrapper[
        A13] :: schemaForWrapper[A14] :: schemaForWrapper[A15] :: schemaForWrapper[
        A16] :: schemaForWrapper[A17] :: schemaForWrapper[A18] :: schemaForWrapper[A19] :: Nil
    UserDefinedFunction(func, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 20 arguments as user-defined function (UDF).
   * @tparam RT
   *   return type of UDF.
   */
  def _toUdf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag,
      A14: TypeTag,
      A15: TypeTag,
      A16: TypeTag,
      A17: TypeTag,
      A18: TypeTag,
      A19: TypeTag,
      A20: TypeTag](
      func: Function20[
        A1,
        A2,
        A3,
        A4,
        A5,
        A6,
        A7,
        A8,
        A9,
        A10,
        A11,
        A12,
        A13,
        A14,
        A15,
        A16,
        A17,
        A18,
        A19,
        A20,
        RT]): UserDefinedFunction = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9],
      typeOf[A10],
      typeOf[A11],
      typeOf[A12],
      typeOf[A13],
      typeOf[A14],
      typeOf[A15],
      typeOf[A16],
      typeOf[A17],
      typeOf[A18],
      typeOf[A19],
      typeOf[A20]).foreach(isSupported(_))
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] =
      schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[A3] :: schemaForWrapper[
        A4] :: schemaForWrapper[A5] :: schemaForWrapper[A6] :: schemaForWrapper[
        A7] :: schemaForWrapper[A8] :: schemaForWrapper[A9] :: schemaForWrapper[
        A10] :: schemaForWrapper[A11] :: schemaForWrapper[A12] :: schemaForWrapper[
        A13] :: schemaForWrapper[A14] :: schemaForWrapper[A15] :: schemaForWrapper[
        A16] :: schemaForWrapper[A17] :: schemaForWrapper[A18] :: schemaForWrapper[
        A19] :: schemaForWrapper[A20] :: Nil
    UserDefinedFunction(func, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 21 arguments as user-defined function (UDF).
   * @tparam RT
   *   return type of UDF.
   */
  def _toUdf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag,
      A14: TypeTag,
      A15: TypeTag,
      A16: TypeTag,
      A17: TypeTag,
      A18: TypeTag,
      A19: TypeTag,
      A20: TypeTag,
      A21: TypeTag](
      func: Function21[
        A1,
        A2,
        A3,
        A4,
        A5,
        A6,
        A7,
        A8,
        A9,
        A10,
        A11,
        A12,
        A13,
        A14,
        A15,
        A16,
        A17,
        A18,
        A19,
        A20,
        A21,
        RT]): UserDefinedFunction = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9],
      typeOf[A10],
      typeOf[A11],
      typeOf[A12],
      typeOf[A13],
      typeOf[A14],
      typeOf[A15],
      typeOf[A16],
      typeOf[A17],
      typeOf[A18],
      typeOf[A19],
      typeOf[A20],
      typeOf[A21]).foreach(isSupported(_))
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns
        : Seq[UdfColumnSchema] = schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[
      A3] :: schemaForWrapper[A4] :: schemaForWrapper[A5] :: schemaForWrapper[
      A6] :: schemaForWrapper[A7] :: schemaForWrapper[A8] :: schemaForWrapper[
      A9] :: schemaForWrapper[A10] :: schemaForWrapper[A11] :: schemaForWrapper[
      A12] :: schemaForWrapper[A13] :: schemaForWrapper[A14] :: schemaForWrapper[
      A15] :: schemaForWrapper[A16] :: schemaForWrapper[A17] :: schemaForWrapper[
      A18] :: schemaForWrapper[A19] :: schemaForWrapper[A20] :: schemaForWrapper[A21] :: Nil
    UserDefinedFunction(func, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 22 arguments as user-defined function (UDF).
   * @tparam RT
   *   return type of UDF.
   */
  def _toUdf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag,
      A14: TypeTag,
      A15: TypeTag,
      A16: TypeTag,
      A17: TypeTag,
      A18: TypeTag,
      A19: TypeTag,
      A20: TypeTag,
      A21: TypeTag,
      A22: TypeTag](
      func: Function22[
        A1,
        A2,
        A3,
        A4,
        A5,
        A6,
        A7,
        A8,
        A9,
        A10,
        A11,
        A12,
        A13,
        A14,
        A15,
        A16,
        A17,
        A18,
        A19,
        A20,
        A21,
        A22,
        RT]): UserDefinedFunction = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9],
      typeOf[A10],
      typeOf[A11],
      typeOf[A12],
      typeOf[A13],
      typeOf[A14],
      typeOf[A15],
      typeOf[A16],
      typeOf[A17],
      typeOf[A18],
      typeOf[A19],
      typeOf[A20],
      typeOf[A21],
      typeOf[A22]).foreach(isSupported(_))
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] =
      schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[A3] :: schemaForWrapper[
        A4] :: schemaForWrapper[A5] :: schemaForWrapper[A6] :: schemaForWrapper[
        A7] :: schemaForWrapper[A8] :: schemaForWrapper[A9] :: schemaForWrapper[
        A10] :: schemaForWrapper[A11] :: schemaForWrapper[A12] :: schemaForWrapper[
        A13] :: schemaForWrapper[A14] :: schemaForWrapper[A15] :: schemaForWrapper[
        A16] :: schemaForWrapper[A17] :: schemaForWrapper[A18] :: schemaForWrapper[
        A19] :: schemaForWrapper[A20] :: schemaForWrapper[A21] :: schemaForWrapper[A22] :: Nil
    UserDefinedFunction(func, returnColumn, inputColumns)
  }

  private[snowpark] def getUDTFClassName(udtf: Any): String = {
    udtf match {
      case scalaUdtf: UDTF => getScalaUDTFClassName(scalaUdtf)
      case javaUdtf: JavaUDTF => getJavaUDTFClassName(javaUdtf)
    }
  }

  private def getScalaUDTFClassName(udtf: UDTF): String = {
    // Check udtf's class must inherit from UDTF[0-22]
    udtf match {
      case _: UDTF0 => "com.snowflake.snowpark.udtf.UDTF0"
      case _: UDTF1[_] => "com.snowflake.snowpark.udtf.UDTF1"
      case _: UDTF2[_, _] => "com.snowflake.snowpark.udtf.UDTF2"
      case _: UDTF3[_, _, _] => "com.snowflake.snowpark.udtf.UDTF3"
      case _: UDTF4[_, _, _, _] => "com.snowflake.snowpark.udtf.UDTF4"
      case _: UDTF5[_, _, _, _, _] => "com.snowflake.snowpark.udtf.UDTF5"
      case _: UDTF6[_, _, _, _, _, _] => "com.snowflake.snowpark.udtf.UDTF6"
      case _: UDTF7[_, _, _, _, _, _, _] => "com.snowflake.snowpark.udtf.UDTF7"
      case _: UDTF8[_, _, _, _, _, _, _, _] => "com.snowflake.snowpark.udtf.UDTF8"
      case _: UDTF9[_, _, _, _, _, _, _, _, _] => "com.snowflake.snowpark.udtf.UDTF9"
      case _: UDTF10[_, _, _, _, _, _, _, _, _, _] => "com.snowflake.snowpark.udtf.UDTF10"
      case _: UDTF11[_, _, _, _, _, _, _, _, _, _, _] => "com.snowflake.snowpark.udtf.UDTF11"
      case _: UDTF12[_, _, _, _, _, _, _, _, _, _, _, _] => "com.snowflake.snowpark.udtf.UDTF12"
      case _: UDTF13[_, _, _, _, _, _, _, _, _, _, _, _, _] =>
        "com.snowflake.snowpark.udtf.UDTF13"
      case _: UDTF14[_, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        "com.snowflake.snowpark.udtf.UDTF14"
      case _: UDTF15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        "com.snowflake.snowpark.udtf.UDTF15"
      case _: UDTF16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        "com.snowflake.snowpark.udtf.UDTF16"
      case _: UDTF17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        "com.snowflake.snowpark.udtf.UDTF17"
      case _: UDTF18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        "com.snowflake.snowpark.udtf.UDTF18"
      case _: UDTF19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        "com.snowflake.snowpark.udtf.UDTF19"
      case _: UDTF20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        "com.snowflake.snowpark.udtf.UDTF20"
      case _: UDTF21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        "com.snowflake.snowpark.udtf.UDTF21"
      case _: UDTF22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        "com.snowflake.snowpark.udtf.UDTF22"
      case _ =>
        throw new UnsupportedOperationException("internal error: UDTF doesn't inherit from UDTFX")
    }
  }

  def checkSupportedUdtf(udtf: UDTF): Unit = {
    // Check udtf's class must inherit from UDTF[0-22]
    getScalaUDTFClassName(udtf)
    // Check output column name
    udtf.outputSchema().foreach { column =>
      if (!Utils.isValidJavaIdentifier(column.name)) {
        throw ErrorMessage.UDF_INVALID_UDTF_COLUMN_NAME(column.name)
      }
    }
  }

  def schemaForUdfColumn[T: TypeTag](index: Int): UdfColumn = {
    isSupported(typeOf[T])
    UdfColumn(schemaForWrapper[T], s"arg$index")
  }

  private[snowpark] def checkSupportedJavaUdtf(javaUdtf: JavaUDTF): Unit = {
    getJavaUDTFClassName(javaUdtf)
    // Check output column name
    javaUdtf.outputSchema().asScala.foreach { column =>
      if (!Utils.isValidJavaIdentifier(column.name)) {
        throw ErrorMessage.UDF_INVALID_UDTF_COLUMN_NAME(column.name)
      }
    }
  }

  private[snowpark] def getJavaUDTFClassName(javaUDTF: JavaUDTF): String = {
    // Check udtf's class must inherit from JavaUDTF[0-22]
    javaUDTF match {
      case _: JavaUDTF0 =>
        "com.snowflake.snowpark_java.udtf.JavaUDTF0"
      case _: JavaUDTF1[_] =>
        "com.snowflake.snowpark_java.udtf.JavaUDTF1"
      case _: JavaUDTF2[_, _] =>
        "com.snowflake.snowpark_java.udtf.JavaUDTF2"
      case _: JavaUDTF3[_, _, _] =>
        "com.snowflake.snowpark_java.udtf.JavaUDTF3"
      case _: JavaUDTF4[_, _, _, _] =>
        "com.snowflake.snowpark_java.udtf.JavaUDTF4"
      case _: JavaUDTF5[_, _, _, _, _] =>
        "com.snowflake.snowpark_java.udtf.JavaUDTF5"
      case _: JavaUDTF6[_, _, _, _, _, _] =>
        "com.snowflake.snowpark_java.udtf.JavaUDTF6"
      case _: JavaUDTF7[_, _, _, _, _, _, _] =>
        "com.snowflake.snowpark_java.udtf.JavaUDTF7"
      case _: JavaUDTF8[_, _, _, _, _, _, _, _] =>
        "com.snowflake.snowpark_java.udtf.JavaUDTF8"
      case _: JavaUDTF9[_, _, _, _, _, _, _, _, _] =>
        "com.snowflake.snowpark_java.udtf.JavaUDTF9"
      case _: JavaUDTF10[_, _, _, _, _, _, _, _, _, _] =>
        "com.snowflake.snowpark_java.udtf.JavaUDTF10"
      case _: JavaUDTF11[_, _, _, _, _, _, _, _, _, _, _] =>
        "com.snowflake.snowpark_java.udtf.JavaUDTF11"
      case _: JavaUDTF12[_, _, _, _, _, _, _, _, _, _, _, _] =>
        "com.snowflake.snowpark_java.udtf.JavaUDTF12"
      case _: JavaUDTF13[_, _, _, _, _, _, _, _, _, _, _, _, _] =>
        "com.snowflake.snowpark_java.udtf.JavaUDTF13"
      case _: JavaUDTF14[_, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        "com.snowflake.snowpark_java.udtf.JavaUDTF14"
      case _: JavaUDTF15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        "com.snowflake.snowpark_java.udtf.JavaUDTF15"
      case _: JavaUDTF16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        "com.snowflake.snowpark_java.udtf.JavaUDTF16"
      case _: JavaUDTF17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        "com.snowflake.snowpark_java.udtf.JavaUDTF17"
      case _: JavaUDTF18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        "com.snowflake.snowpark_java.udtf.JavaUDTF18"
      case _: JavaUDTF19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        "com.snowflake.snowpark_java.udtf.JavaUDTF19"
      case _: JavaUDTF20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        "com.snowflake.snowpark_java.udtf.JavaUDTF20"
      case _: JavaUDTF21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        "com.snowflake.snowpark_java.udtf.JavaUDTF21"
      case _: JavaUDTF22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        "com.snowflake.snowpark_java.udtf.JavaUDTF22"
      case _ =>
        throw new UnsupportedOperationException(
          "internal error: Java UDTF doesn't inherit from JavaUDTFX")
    }
  }

  private[snowpark] def getJavaUTFInputColumns(javaUDTF: JavaUDTF): Array[UdfColumn] = {
    // Check udtf's class must inherit from JavaUDTF[0-22]
    javaUDTF match {
      case _: JavaUDTF0 =>
        getUDFColumns(javaUDTF, 0)
      case _: JavaUDTF1[_] =>
        getUDFColumns(javaUDTF, 1)
      case _: JavaUDTF2[_, _] =>
        getUDFColumns(javaUDTF, 2)
      case _: JavaUDTF3[_, _, _] =>
        getUDFColumns(javaUDTF, 3)
      case _: JavaUDTF4[_, _, _, _] =>
        getUDFColumns(javaUDTF, 4)
      case _: JavaUDTF5[_, _, _, _, _] =>
        getUDFColumns(javaUDTF, 5)
      case _: JavaUDTF6[_, _, _, _, _, _] =>
        getUDFColumns(javaUDTF, 6)
      case _: JavaUDTF7[_, _, _, _, _, _, _] =>
        getUDFColumns(javaUDTF, 7)
      case _: JavaUDTF8[_, _, _, _, _, _, _, _] =>
        getUDFColumns(javaUDTF, 8)
      case _: JavaUDTF9[_, _, _, _, _, _, _, _, _] =>
        getUDFColumns(javaUDTF, 9)
      case _: JavaUDTF10[_, _, _, _, _, _, _, _, _, _] =>
        getUDFColumns(javaUDTF, 10)
      case _: JavaUDTF11[_, _, _, _, _, _, _, _, _, _, _] =>
        getUDFColumns(javaUDTF, 11)
      case _: JavaUDTF12[_, _, _, _, _, _, _, _, _, _, _, _] =>
        getUDFColumns(javaUDTF, 12)
      case _: JavaUDTF13[_, _, _, _, _, _, _, _, _, _, _, _, _] =>
        getUDFColumns(javaUDTF, 13)
      case _: JavaUDTF14[_, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        getUDFColumns(javaUDTF, 14)
      case _: JavaUDTF15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        getUDFColumns(javaUDTF, 15)
      case _: JavaUDTF16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        getUDFColumns(javaUDTF, 16)
      case _: JavaUDTF17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        getUDFColumns(javaUDTF, 17)
      case _: JavaUDTF18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        getUDFColumns(javaUDTF, 18)
      case _: JavaUDTF19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        getUDFColumns(javaUDTF, 19)
      case _: JavaUDTF20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        getUDFColumns(javaUDTF, 20)
      case _: JavaUDTF21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        getUDFColumns(javaUDTF, 21)
      case _: JavaUDTF22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
        getUDFColumns(javaUDTF, 22)
      case _ =>
        throw new UnsupportedOperationException(
          "internal error: Java UDTF doesn't inherit from JavaUDTFX")
    }
  }

  /* Code below for _toSP 0-21 generated by this script
   * (0 to 21).foreach { x =>
   * val types = (1 to x).foldRight("RT")((i, s) => {s"A$i, $s"})
   * val typeOfs = (1 to x).map(i => s"A$i").toSeq.map(t => s"typeOf[$t]")
   * val typeTags = (1 to x).map(i => s"A$i: TypeTag").foldLeft("RT: TypeTag")(_ + ", " + _)
   * val inputTypes = (1 to x).foldRight("Nil")((i, s) => {s"schemaForWrapper[A$i] :: $s"})
   * println(s"""
   *   |/**
   *   | * Creates a Scala closure of $x arguments as Stored Procedure function (SProc).
   *   | * @tparam RT return type of UDF.
   *   | */
   *   |def _toSP[$typeTags](sp: Function${x + 1}[Session, $types]): StoredProcedure = {
   *   |  $typeOfs.foreach(isSupported)
   *   |  isSupported(typeOf[RT])
   *   |  val returnColumn = schemaForWrapper[RT]
   *   |  val inputColumns: Seq[UdfColumnSchema] = $inputTypes
   *   |  StoredProcedure(sp, returnColumn, inputColumns)
   *   |}""".stripMargin)
   * }
   */

  /**
   * Creates a Scala closure of 0 arguments as Stored Procedure function (SProc).
   * @tparam RT
   *   return type of UDF.
   */
  def _toSP[RT: TypeTag](sp: Function1[Session, RT]): StoredProcedure = {
    Vector().foreach(isSupported)
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] = Nil
    StoredProcedure(sp, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 1 arguments as Stored Procedure function (SProc).
   * @tparam RT
   *   return type of UDF.
   */
  def _toSP[RT: TypeTag, A1: TypeTag](sp: Function2[Session, A1, RT]): StoredProcedure = {
    Vector(typeOf[A1]).foreach(isSupported)
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] = schemaForWrapper[A1] :: Nil
    StoredProcedure(sp, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 2 arguments as Stored Procedure function (SProc).
   * @tparam RT
   *   return type of UDF.
   */
  def _toSP[RT: TypeTag, A1: TypeTag, A2: TypeTag](
      sp: Function3[Session, A1, A2, RT]): StoredProcedure = {
    Vector(typeOf[A1], typeOf[A2]).foreach(isSupported)
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] = schemaForWrapper[A1] :: schemaForWrapper[A2] :: Nil
    StoredProcedure(sp, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 3 arguments as Stored Procedure function (SProc).
   * @tparam RT
   *   return type of UDF.
   */
  def _toSP[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag](
      sp: Function4[Session, A1, A2, A3, RT]): StoredProcedure = {
    Vector(typeOf[A1], typeOf[A2], typeOf[A3]).foreach(isSupported)
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] =
      schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[A3] :: Nil
    StoredProcedure(sp, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 4 arguments as Stored Procedure function (SProc).
   * @tparam RT
   *   return type of UDF.
   */
  def _toSP[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag](
      sp: Function5[Session, A1, A2, A3, A4, RT]): StoredProcedure = {
    Vector(typeOf[A1], typeOf[A2], typeOf[A3], typeOf[A4]).foreach(isSupported)
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] = schemaForWrapper[A1] :: schemaForWrapper[
      A2] :: schemaForWrapper[A3] :: schemaForWrapper[A4] :: Nil
    StoredProcedure(sp, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 5 arguments as Stored Procedure function (SProc).
   * @tparam RT
   *   return type of UDF.
   */
  def _toSP[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag](
      sp: Function6[Session, A1, A2, A3, A4, A5, RT]): StoredProcedure = {
    Vector(typeOf[A1], typeOf[A2], typeOf[A3], typeOf[A4], typeOf[A5]).foreach(isSupported)
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] = schemaForWrapper[A1] :: schemaForWrapper[
      A2] :: schemaForWrapper[A3] :: schemaForWrapper[A4] :: schemaForWrapper[A5] :: Nil
    StoredProcedure(sp, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 6 arguments as Stored Procedure function (SProc).
   * @tparam RT
   *   return type of UDF.
   */
  def _toSP[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag](sp: Function7[Session, A1, A2, A3, A4, A5, A6, RT]): StoredProcedure = {
    Vector(typeOf[A1], typeOf[A2], typeOf[A3], typeOf[A4], typeOf[A5], typeOf[A6])
      .foreach(isSupported)
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] =
      schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[A3] :: schemaForWrapper[
        A4] :: schemaForWrapper[A5] :: schemaForWrapper[A6] :: Nil
    StoredProcedure(sp, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 7 arguments as Stored Procedure function (SProc).
   * @tparam RT
   *   return type of UDF.
   */
  def _toSP[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag](sp: Function8[Session, A1, A2, A3, A4, A5, A6, A7, RT]): StoredProcedure = {
    Vector(typeOf[A1], typeOf[A2], typeOf[A3], typeOf[A4], typeOf[A5], typeOf[A6], typeOf[A7])
      .foreach(isSupported)
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] =
      schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[A3] :: schemaForWrapper[
        A4] :: schemaForWrapper[A5] :: schemaForWrapper[A6] :: schemaForWrapper[A7] :: Nil
    StoredProcedure(sp, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 8 arguments as Stored Procedure function (SProc).
   * @tparam RT
   *   return type of UDF.
   */
  def _toSP[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag](sp: Function9[Session, A1, A2, A3, A4, A5, A6, A7, A8, RT]): StoredProcedure = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8]).foreach(isSupported)
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] = schemaForWrapper[A1] :: schemaForWrapper[
      A2] :: schemaForWrapper[A3] :: schemaForWrapper[A4] :: schemaForWrapper[
      A5] :: schemaForWrapper[A6] :: schemaForWrapper[A7] :: schemaForWrapper[A8] :: Nil
    StoredProcedure(sp, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 9 arguments as Stored Procedure function (SProc).
   * @tparam RT
   *   return type of UDF.
   */
  def _toSP[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag](
      sp: Function10[Session, A1, A2, A3, A4, A5, A6, A7, A8, A9, RT]): StoredProcedure = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9]).foreach(isSupported)
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns
        : Seq[UdfColumnSchema] = schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[
      A3] :: schemaForWrapper[A4] :: schemaForWrapper[A5] :: schemaForWrapper[
      A6] :: schemaForWrapper[A7] :: schemaForWrapper[A8] :: schemaForWrapper[A9] :: Nil
    StoredProcedure(sp, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 10 arguments as Stored Procedure function (SProc).
   * @tparam RT
   *   return type of UDF.
   */
  def _toSP[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag](
      sp: Function11[Session, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT]): StoredProcedure = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9],
      typeOf[A10]).foreach(isSupported)
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] =
      schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[A3] :: schemaForWrapper[
        A4] :: schemaForWrapper[A5] :: schemaForWrapper[A6] :: schemaForWrapper[
        A7] :: schemaForWrapper[A8] :: schemaForWrapper[A9] :: schemaForWrapper[A10] :: Nil
    StoredProcedure(sp, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 11 arguments as Stored Procedure function (SProc).
   * @tparam RT
   *   return type of UDF.
   */
  def _toSP[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag](sp: Function12[Session, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, RT])
      : StoredProcedure = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9],
      typeOf[A10],
      typeOf[A11]).foreach(isSupported)
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] =
      schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[A3] :: schemaForWrapper[
        A4] :: schemaForWrapper[A5] :: schemaForWrapper[A6] :: schemaForWrapper[
        A7] :: schemaForWrapper[A8] :: schemaForWrapper[A9] :: schemaForWrapper[
        A10] :: schemaForWrapper[A11] :: Nil
    StoredProcedure(sp, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 12 arguments as Stored Procedure function (SProc).
   * @tparam RT
   *   return type of UDF.
   */
  def _toSP[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag](sp: Function13[Session, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, RT])
      : StoredProcedure = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9],
      typeOf[A10],
      typeOf[A11],
      typeOf[A12]).foreach(isSupported)
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns
        : Seq[UdfColumnSchema] = schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[
      A3] :: schemaForWrapper[A4] :: schemaForWrapper[A5] :: schemaForWrapper[
      A6] :: schemaForWrapper[A7] :: schemaForWrapper[A8] :: schemaForWrapper[
      A9] :: schemaForWrapper[A10] :: schemaForWrapper[A11] :: schemaForWrapper[A12] :: Nil
    StoredProcedure(sp, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 13 arguments as Stored Procedure function (SProc).
   * @tparam RT
   *   return type of UDF.
   */
  def _toSP[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag](
      sp: Function14[Session, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, RT])
      : StoredProcedure = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9],
      typeOf[A10],
      typeOf[A11],
      typeOf[A12],
      typeOf[A13]).foreach(isSupported)
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] =
      schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[A3] :: schemaForWrapper[
        A4] :: schemaForWrapper[A5] :: schemaForWrapper[A6] :: schemaForWrapper[
        A7] :: schemaForWrapper[A8] :: schemaForWrapper[A9] :: schemaForWrapper[
        A10] :: schemaForWrapper[A11] :: schemaForWrapper[A12] :: schemaForWrapper[A13] :: Nil
    StoredProcedure(sp, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 14 arguments as Stored Procedure function (SProc).
   * @tparam RT
   *   return type of UDF.
   */
  def _toSP[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag,
      A14: TypeTag](
      sp: Function15[Session, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, RT])
      : StoredProcedure = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9],
      typeOf[A10],
      typeOf[A11],
      typeOf[A12],
      typeOf[A13],
      typeOf[A14]).foreach(isSupported)
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] =
      schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[A3] :: schemaForWrapper[
        A4] :: schemaForWrapper[A5] :: schemaForWrapper[A6] :: schemaForWrapper[
        A7] :: schemaForWrapper[A8] :: schemaForWrapper[A9] :: schemaForWrapper[
        A10] :: schemaForWrapper[A11] :: schemaForWrapper[A12] :: schemaForWrapper[
        A13] :: schemaForWrapper[A14] :: Nil
    StoredProcedure(sp, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 15 arguments as Stored Procedure function (SProc).
   * @tparam RT
   *   return type of UDF.
   */
  def _toSP[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag,
      A14: TypeTag,
      A15: TypeTag](
      sp: Function16[Session, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, RT])
      : StoredProcedure = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9],
      typeOf[A10],
      typeOf[A11],
      typeOf[A12],
      typeOf[A13],
      typeOf[A14],
      typeOf[A15]).foreach(isSupported)
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns
        : Seq[UdfColumnSchema] = schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[
      A3] :: schemaForWrapper[A4] :: schemaForWrapper[A5] :: schemaForWrapper[
      A6] :: schemaForWrapper[A7] :: schemaForWrapper[A8] :: schemaForWrapper[
      A9] :: schemaForWrapper[A10] :: schemaForWrapper[A11] :: schemaForWrapper[
      A12] :: schemaForWrapper[A13] :: schemaForWrapper[A14] :: schemaForWrapper[A15] :: Nil
    StoredProcedure(sp, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 16 arguments as Stored Procedure function (SProc).
   * @tparam RT
   *   return type of UDF.
   */
  def _toSP[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag,
      A14: TypeTag,
      A15: TypeTag,
      A16: TypeTag](
      sp: Function17[
        Session,
        A1,
        A2,
        A3,
        A4,
        A5,
        A6,
        A7,
        A8,
        A9,
        A10,
        A11,
        A12,
        A13,
        A14,
        A15,
        A16,
        RT]): StoredProcedure = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9],
      typeOf[A10],
      typeOf[A11],
      typeOf[A12],
      typeOf[A13],
      typeOf[A14],
      typeOf[A15],
      typeOf[A16]).foreach(isSupported)
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] =
      schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[A3] :: schemaForWrapper[
        A4] :: schemaForWrapper[A5] :: schemaForWrapper[A6] :: schemaForWrapper[
        A7] :: schemaForWrapper[A8] :: schemaForWrapper[A9] :: schemaForWrapper[
        A10] :: schemaForWrapper[A11] :: schemaForWrapper[A12] :: schemaForWrapper[
        A13] :: schemaForWrapper[A14] :: schemaForWrapper[A15] :: schemaForWrapper[A16] :: Nil
    StoredProcedure(sp, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 17 arguments as Stored Procedure function (SProc).
   * @tparam RT
   *   return type of UDF.
   */
  def _toSP[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag,
      A14: TypeTag,
      A15: TypeTag,
      A16: TypeTag,
      A17: TypeTag](
      sp: Function18[
        Session,
        A1,
        A2,
        A3,
        A4,
        A5,
        A6,
        A7,
        A8,
        A9,
        A10,
        A11,
        A12,
        A13,
        A14,
        A15,
        A16,
        A17,
        RT]): StoredProcedure = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9],
      typeOf[A10],
      typeOf[A11],
      typeOf[A12],
      typeOf[A13],
      typeOf[A14],
      typeOf[A15],
      typeOf[A16],
      typeOf[A17]).foreach(isSupported)
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] =
      schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[A3] :: schemaForWrapper[
        A4] :: schemaForWrapper[A5] :: schemaForWrapper[A6] :: schemaForWrapper[
        A7] :: schemaForWrapper[A8] :: schemaForWrapper[A9] :: schemaForWrapper[
        A10] :: schemaForWrapper[A11] :: schemaForWrapper[A12] :: schemaForWrapper[
        A13] :: schemaForWrapper[A14] :: schemaForWrapper[A15] :: schemaForWrapper[
        A16] :: schemaForWrapper[A17] :: Nil
    StoredProcedure(sp, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 18 arguments as Stored Procedure function (SProc).
   * @tparam RT
   *   return type of UDF.
   */
  def _toSP[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag,
      A14: TypeTag,
      A15: TypeTag,
      A16: TypeTag,
      A17: TypeTag,
      A18: TypeTag](
      sp: Function19[
        Session,
        A1,
        A2,
        A3,
        A4,
        A5,
        A6,
        A7,
        A8,
        A9,
        A10,
        A11,
        A12,
        A13,
        A14,
        A15,
        A16,
        A17,
        A18,
        RT]): StoredProcedure = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9],
      typeOf[A10],
      typeOf[A11],
      typeOf[A12],
      typeOf[A13],
      typeOf[A14],
      typeOf[A15],
      typeOf[A16],
      typeOf[A17],
      typeOf[A18]).foreach(isSupported)
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns
        : Seq[UdfColumnSchema] = schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[
      A3] :: schemaForWrapper[A4] :: schemaForWrapper[A5] :: schemaForWrapper[
      A6] :: schemaForWrapper[A7] :: schemaForWrapper[A8] :: schemaForWrapper[
      A9] :: schemaForWrapper[A10] :: schemaForWrapper[A11] :: schemaForWrapper[
      A12] :: schemaForWrapper[A13] :: schemaForWrapper[A14] :: schemaForWrapper[
      A15] :: schemaForWrapper[A16] :: schemaForWrapper[A17] :: schemaForWrapper[A18] :: Nil
    StoredProcedure(sp, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 19 arguments as Stored Procedure function (SProc).
   * @tparam RT
   *   return type of UDF.
   */
  def _toSP[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag,
      A14: TypeTag,
      A15: TypeTag,
      A16: TypeTag,
      A17: TypeTag,
      A18: TypeTag,
      A19: TypeTag](
      sp: Function20[
        Session,
        A1,
        A2,
        A3,
        A4,
        A5,
        A6,
        A7,
        A8,
        A9,
        A10,
        A11,
        A12,
        A13,
        A14,
        A15,
        A16,
        A17,
        A18,
        A19,
        RT]): StoredProcedure = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9],
      typeOf[A10],
      typeOf[A11],
      typeOf[A12],
      typeOf[A13],
      typeOf[A14],
      typeOf[A15],
      typeOf[A16],
      typeOf[A17],
      typeOf[A18],
      typeOf[A19]).foreach(isSupported)
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] =
      schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[A3] :: schemaForWrapper[
        A4] :: schemaForWrapper[A5] :: schemaForWrapper[A6] :: schemaForWrapper[
        A7] :: schemaForWrapper[A8] :: schemaForWrapper[A9] :: schemaForWrapper[
        A10] :: schemaForWrapper[A11] :: schemaForWrapper[A12] :: schemaForWrapper[
        A13] :: schemaForWrapper[A14] :: schemaForWrapper[A15] :: schemaForWrapper[
        A16] :: schemaForWrapper[A17] :: schemaForWrapper[A18] :: schemaForWrapper[A19] :: Nil
    StoredProcedure(sp, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 20 arguments as Stored Procedure function (SProc).
   * @tparam RT
   *   return type of UDF.
   */
  def _toSP[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag,
      A14: TypeTag,
      A15: TypeTag,
      A16: TypeTag,
      A17: TypeTag,
      A18: TypeTag,
      A19: TypeTag,
      A20: TypeTag](
      sp: Function21[
        Session,
        A1,
        A2,
        A3,
        A4,
        A5,
        A6,
        A7,
        A8,
        A9,
        A10,
        A11,
        A12,
        A13,
        A14,
        A15,
        A16,
        A17,
        A18,
        A19,
        A20,
        RT]): StoredProcedure = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9],
      typeOf[A10],
      typeOf[A11],
      typeOf[A12],
      typeOf[A13],
      typeOf[A14],
      typeOf[A15],
      typeOf[A16],
      typeOf[A17],
      typeOf[A18],
      typeOf[A19],
      typeOf[A20]).foreach(isSupported)
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns: Seq[UdfColumnSchema] =
      schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[A3] :: schemaForWrapper[
        A4] :: schemaForWrapper[A5] :: schemaForWrapper[A6] :: schemaForWrapper[
        A7] :: schemaForWrapper[A8] :: schemaForWrapper[A9] :: schemaForWrapper[
        A10] :: schemaForWrapper[A11] :: schemaForWrapper[A12] :: schemaForWrapper[
        A13] :: schemaForWrapper[A14] :: schemaForWrapper[A15] :: schemaForWrapper[
        A16] :: schemaForWrapper[A17] :: schemaForWrapper[A18] :: schemaForWrapper[
        A19] :: schemaForWrapper[A20] :: Nil
    StoredProcedure(sp, returnColumn, inputColumns)
  }

  /**
   * Creates a Scala closure of 21 arguments as Stored Procedure function (SProc).
   * @tparam RT
   *   return type of UDF.
   */
  def _toSP[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag,
      A14: TypeTag,
      A15: TypeTag,
      A16: TypeTag,
      A17: TypeTag,
      A18: TypeTag,
      A19: TypeTag,
      A20: TypeTag,
      A21: TypeTag](
      sp: Function22[
        Session,
        A1,
        A2,
        A3,
        A4,
        A5,
        A6,
        A7,
        A8,
        A9,
        A10,
        A11,
        A12,
        A13,
        A14,
        A15,
        A16,
        A17,
        A18,
        A19,
        A20,
        A21,
        RT]): StoredProcedure = {
    Vector(
      typeOf[A1],
      typeOf[A2],
      typeOf[A3],
      typeOf[A4],
      typeOf[A5],
      typeOf[A6],
      typeOf[A7],
      typeOf[A8],
      typeOf[A9],
      typeOf[A10],
      typeOf[A11],
      typeOf[A12],
      typeOf[A13],
      typeOf[A14],
      typeOf[A15],
      typeOf[A16],
      typeOf[A17],
      typeOf[A18],
      typeOf[A19],
      typeOf[A20],
      typeOf[A21]).foreach(isSupported)
    isSupported(typeOf[RT])
    val returnColumn = schemaForWrapper[RT]
    val inputColumns
        : Seq[UdfColumnSchema] = schemaForWrapper[A1] :: schemaForWrapper[A2] :: schemaForWrapper[
      A3] :: schemaForWrapper[A4] :: schemaForWrapper[A5] :: schemaForWrapper[
      A6] :: schemaForWrapper[A7] :: schemaForWrapper[A8] :: schemaForWrapper[
      A9] :: schemaForWrapper[A10] :: schemaForWrapper[A11] :: schemaForWrapper[
      A12] :: schemaForWrapper[A13] :: schemaForWrapper[A14] :: schemaForWrapper[
      A15] :: schemaForWrapper[A16] :: schemaForWrapper[A17] :: schemaForWrapper[
      A18] :: schemaForWrapper[A19] :: schemaForWrapper[A20] :: schemaForWrapper[A21] :: Nil
    StoredProcedure(sp, returnColumn, inputColumns)
  }

  private def getUDFColumns(udtf: JavaUDTF, argCount: Int): Array[UdfColumn] = {
    val argNames = (1 to argCount).map(i => s"arg$i")
    getProcessMethod(udtf, argCount).getParameters
      .map(_.getType)
      .map(javaTypeToDataType)
      .zip(argNames)
      .map(entry => UdfColumn(UdfColumnSchema(entry._1), entry._2))
  }

  // For JavaUDTF, use reflection to load the process() function.
  // Because JavaUDTFx is generic interface, there will 2 process() methods:
  // One is all parameters to be Object, the other has real parameter type class.
  // This function is to load the process() with real parameters type.
  private def getProcessMethod(udtf: JavaUDTF, argCount: Int): Method = {
    val processFuncName = "process"
    val methods = udtf.getClass.getDeclaredMethods.filter(m =>
      if (argCount == 0) {
        m.getName.equals(processFuncName) && m.getParameterCount == argCount
      } else {
        m.getName.equals(processFuncName) && m.getParameterCount == argCount &&
        m.getParameterTypes.map(_.getCanonicalName).exists(!_.equals("java.lang.Object"))
      })
    if (methods.length != 1) {
      throw ErrorMessage.UDF_CANNOT_INFER_MULTIPLE_PROCESS(argCount)
    }
    methods.head
  }
}
