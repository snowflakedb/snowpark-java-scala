package com.snowflake.snowpark.udtf

import com.snowflake.snowpark.Row
import com.snowflake.snowpark.internal.{ScalaFunctions, UdfColumn}
import com.snowflake.snowpark.types.StructType

import scala.reflect.runtime.universe.TypeTag

/** The Scala UDTF (user-defined table function) trait.
  * @since 1.2.0
  */
sealed trait UDTF extends java.io.Serializable {

  /** A StructType that describes the data types of the fields in the rows returned by the process()
    * and endPartition() methods.
    *
    * For example, if a UDTF returns rows that contain a StringType and IntegerType field, the
    * outputSchema() method should construct and return the following StructType object: {{ override
    * def outputSchema(): StructType = StructType(StructField("word", StringType),
    * StructField("count", IntegerType)) }}
    *
    * Since: 1.2.0
    */
  def outputSchema(): StructType

  /** This method can be used to generate output rows that are based on any state information
    * aggregated in process(). This method is invoked once for each partition, after all rows in
    * that partition have been passed to process().
    *
    * The rows returned in this method must match the StructType defined in [[outputSchema]]
    *
    * Since: 1.2.0
    */
  def endPartition(): Iterable[Row]

  // Below are internal private functions
  private[snowpark] def inputColumns: Seq[UdfColumn]
}

/** The Scala UDTF (user-defined table function) abstract class that has no argument.
  * @since 1.2.0
  */
abstract class UDTF0 extends UDTF {

  /** This method is invoked once for each row in the input partition. The arguments passed to the
    * registered UDTF are passed to process().
    *
    * The rows returned in this method must match the StructType defined in [[outputSchema]]
    *
    * Since: 1.2.0
    */
  def process(): Iterable[Row]

  override private[snowpark] def inputColumns: Seq[UdfColumn] = Seq.empty
}

// scalastyle:off
/*
  test("generate UDTF classes 1 - 22") {
    (1 to 22).foreach { x =>
      val className = s"UDTF$x"
      val types = (0 until x).map(i => s"A$i: TypeTag").mkString(", ")
      val processArgs = (0 until x).map(i => s"arg$i: A$i").mkString(", ")
      val inputColumns =
        (0 until x).map(i => s"ScalaFunctions.schemaForUdfColumn[A$i](${i + 1})").mkString(", ")
      val s = if (x > 1) "s" else ""
      val code =
        s"""
           |/**
           | * The Scala UDTF (user-defined table function) abstract class that has $x argument$s.
           | *
           | * @since 1.2.0
           | */
           |abstract class $className[$types] extends UDTF {
           |
           |  /**
           |   * This method is invoked once for each row in the input partition.
           |   * The arguments passed to the registered UDTF are passed to process().
           |   *
           |   * The rows returned in this method must match the StructType defined in [[outputSchema]]
           |   *
           |   * Since: 1.2.0
           |   */
           |  // scalastyle:off
           |  def process($processArgs): Iterable[Row]
           |  // scalastyle:on
           |
           |  override private[snowpark] def inputColumns: Seq[UdfColumn] =
           |    Seq($inputColumns)
           |}""".stripMargin
      println(code)
    }
  }
 */
// scalastyle:on

/** The Scala UDTF (user-defined table function) abstract class that has 1 argument.
  *
  * @since 1.2.0
  */
abstract class UDTF1[A0: TypeTag] extends UDTF {

  /** This method is invoked once for each row in the input partition. The arguments passed to the
    * registered UDTF are passed to process().
    *
    * The rows returned in this method must match the StructType defined in [[outputSchema]]
    *
    * Since: 1.2.0
    */
  def process(arg0: A0): Iterable[Row]

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(ScalaFunctions.schemaForUdfColumn[A0](1))
}

/** The Scala UDTF (user-defined table function) abstract class that has 2 arguments.
  *
  * @since 1.2.0
  */
abstract class UDTF2[A0: TypeTag, A1: TypeTag] extends UDTF {

  /** This method is invoked once for each row in the input partition. The arguments passed to the
    * registered UDTF are passed to process().
    *
    * The rows returned in this method must match the StructType defined in [[outputSchema]]
    *
    * Since: 1.2.0
    */
  def process(arg0: A0, arg1: A1): Iterable[Row]

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(ScalaFunctions.schemaForUdfColumn[A0](1), ScalaFunctions.schemaForUdfColumn[A1](2))
}

/** The Scala UDTF (user-defined table function) abstract class that has 3 arguments.
  *
  * @since 1.2.0
  */
abstract class UDTF3[A0: TypeTag, A1: TypeTag, A2: TypeTag] extends UDTF {

  /** This method is invoked once for each row in the input partition. The arguments passed to the
    * registered UDTF are passed to process().
    *
    * The rows returned in this method must match the StructType defined in [[outputSchema]]
    *
    * Since: 1.2.0
    */
  def process(arg0: A0, arg1: A1, arg2: A2): Iterable[Row]

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1),
      ScalaFunctions.schemaForUdfColumn[A1](2),
      ScalaFunctions.schemaForUdfColumn[A2](3))
}

/** The Scala UDTF (user-defined table function) abstract class that has 4 arguments.
  *
  * @since 1.2.0
  */
abstract class UDTF4[A0: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag] extends UDTF {

  /** This method is invoked once for each row in the input partition. The arguments passed to the
    * registered UDTF are passed to process().
    *
    * The rows returned in this method must match the StructType defined in [[outputSchema]]
    *
    * Since: 1.2.0
    */
  def process(arg0: A0, arg1: A1, arg2: A2, arg3: A3): Iterable[Row]

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1),
      ScalaFunctions.schemaForUdfColumn[A1](2),
      ScalaFunctions.schemaForUdfColumn[A2](3),
      ScalaFunctions.schemaForUdfColumn[A3](4))
}

/** The Scala UDTF (user-defined table function) abstract class that has 5 arguments.
  *
  * @since 1.2.0
  */
abstract class UDTF5[A0: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag] extends UDTF {

  /** This method is invoked once for each row in the input partition. The arguments passed to the
    * registered UDTF are passed to process().
    *
    * The rows returned in this method must match the StructType defined in [[outputSchema]]
    *
    * Since: 1.2.0
    */
  def process(arg0: A0, arg1: A1, arg2: A2, arg3: A3, arg4: A4): Iterable[Row]

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1),
      ScalaFunctions.schemaForUdfColumn[A1](2),
      ScalaFunctions.schemaForUdfColumn[A2](3),
      ScalaFunctions.schemaForUdfColumn[A3](4),
      ScalaFunctions.schemaForUdfColumn[A4](5))
}

/** The Scala UDTF (user-defined table function) abstract class that has 6 arguments.
  *
  * @since 1.2.0
  */
abstract class UDTF6[A0: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag]
    extends UDTF {

  /** This method is invoked once for each row in the input partition. The arguments passed to the
    * registered UDTF are passed to process().
    *
    * The rows returned in this method must match the StructType defined in [[outputSchema]]
    *
    * Since: 1.2.0
    */
  def process(arg0: A0, arg1: A1, arg2: A2, arg3: A3, arg4: A4, arg5: A5): Iterable[Row]

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1),
      ScalaFunctions.schemaForUdfColumn[A1](2),
      ScalaFunctions.schemaForUdfColumn[A2](3),
      ScalaFunctions.schemaForUdfColumn[A3](4),
      ScalaFunctions.schemaForUdfColumn[A4](5),
      ScalaFunctions.schemaForUdfColumn[A5](6))
}

/** The Scala UDTF (user-defined table function) abstract class that has 7 arguments.
  *
  * @since 1.2.0
  */
abstract class UDTF7[
    A0: TypeTag,
    A1: TypeTag,
    A2: TypeTag,
    A3: TypeTag,
    A4: TypeTag,
    A5: TypeTag,
    A6: TypeTag]
    extends UDTF {

  /** This method is invoked once for each row in the input partition. The arguments passed to the
    * registered UDTF are passed to process().
    *
    * The rows returned in this method must match the StructType defined in [[outputSchema]]
    *
    * Since: 1.2.0
    */
  def process(arg0: A0, arg1: A1, arg2: A2, arg3: A3, arg4: A4, arg5: A5, arg6: A6): Iterable[Row]

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1),
      ScalaFunctions.schemaForUdfColumn[A1](2),
      ScalaFunctions.schemaForUdfColumn[A2](3),
      ScalaFunctions.schemaForUdfColumn[A3](4),
      ScalaFunctions.schemaForUdfColumn[A4](5),
      ScalaFunctions.schemaForUdfColumn[A5](6),
      ScalaFunctions.schemaForUdfColumn[A6](7))
}

/** The Scala UDTF (user-defined table function) abstract class that has 8 arguments.
  *
  * @since 1.2.0
  */
abstract class UDTF8[
    A0: TypeTag,
    A1: TypeTag,
    A2: TypeTag,
    A3: TypeTag,
    A4: TypeTag,
    A5: TypeTag,
    A6: TypeTag,
    A7: TypeTag]
    extends UDTF {

  /** This method is invoked once for each row in the input partition. The arguments passed to the
    * registered UDTF are passed to process().
    *
    * The rows returned in this method must match the StructType defined in [[outputSchema]]
    *
    * Since: 1.2.0
    */
  def process(arg0: A0, arg1: A1, arg2: A2, arg3: A3, arg4: A4, arg5: A5, arg6: A6, arg7: A7)
      : Iterable[Row]

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1),
      ScalaFunctions.schemaForUdfColumn[A1](2),
      ScalaFunctions.schemaForUdfColumn[A2](3),
      ScalaFunctions.schemaForUdfColumn[A3](4),
      ScalaFunctions.schemaForUdfColumn[A4](5),
      ScalaFunctions.schemaForUdfColumn[A5](6),
      ScalaFunctions.schemaForUdfColumn[A6](7),
      ScalaFunctions.schemaForUdfColumn[A7](8))
}

/** The Scala UDTF (user-defined table function) abstract class that has 9 arguments.
  *
  * @since 1.2.0
  */
abstract class UDTF9[
    A0: TypeTag,
    A1: TypeTag,
    A2: TypeTag,
    A3: TypeTag,
    A4: TypeTag,
    A5: TypeTag,
    A6: TypeTag,
    A7: TypeTag,
    A8: TypeTag]
    extends UDTF {

  /** This method is invoked once for each row in the input partition. The arguments passed to the
    * registered UDTF are passed to process().
    *
    * The rows returned in this method must match the StructType defined in [[outputSchema]]
    *
    * Since: 1.2.0
    */
  def process(
      arg0: A0,
      arg1: A1,
      arg2: A2,
      arg3: A3,
      arg4: A4,
      arg5: A5,
      arg6: A6,
      arg7: A7,
      arg8: A8): Iterable[Row]

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1),
      ScalaFunctions.schemaForUdfColumn[A1](2),
      ScalaFunctions.schemaForUdfColumn[A2](3),
      ScalaFunctions.schemaForUdfColumn[A3](4),
      ScalaFunctions.schemaForUdfColumn[A4](5),
      ScalaFunctions.schemaForUdfColumn[A5](6),
      ScalaFunctions.schemaForUdfColumn[A6](7),
      ScalaFunctions.schemaForUdfColumn[A7](8),
      ScalaFunctions.schemaForUdfColumn[A8](9))
}

/** The Scala UDTF (user-defined table function) abstract class that has 10 arguments.
  *
  * @since 1.2.0
  */
abstract class UDTF10[
    A0: TypeTag,
    A1: TypeTag,
    A2: TypeTag,
    A3: TypeTag,
    A4: TypeTag,
    A5: TypeTag,
    A6: TypeTag,
    A7: TypeTag,
    A8: TypeTag,
    A9: TypeTag]
    extends UDTF {

  /** This method is invoked once for each row in the input partition. The arguments passed to the
    * registered UDTF are passed to process().
    *
    * The rows returned in this method must match the StructType defined in [[outputSchema]]
    *
    * Since: 1.2.0
    */
  def process(
      arg0: A0,
      arg1: A1,
      arg2: A2,
      arg3: A3,
      arg4: A4,
      arg5: A5,
      arg6: A6,
      arg7: A7,
      arg8: A8,
      arg9: A9): Iterable[Row]

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1),
      ScalaFunctions.schemaForUdfColumn[A1](2),
      ScalaFunctions.schemaForUdfColumn[A2](3),
      ScalaFunctions.schemaForUdfColumn[A3](4),
      ScalaFunctions.schemaForUdfColumn[A4](5),
      ScalaFunctions.schemaForUdfColumn[A5](6),
      ScalaFunctions.schemaForUdfColumn[A6](7),
      ScalaFunctions.schemaForUdfColumn[A7](8),
      ScalaFunctions.schemaForUdfColumn[A8](9),
      ScalaFunctions.schemaForUdfColumn[A9](10))
}

/** The Scala UDTF (user-defined table function) abstract class that has 11 arguments.
  *
  * @since 1.2.0
  */
abstract class UDTF11[
    A0: TypeTag,
    A1: TypeTag,
    A2: TypeTag,
    A3: TypeTag,
    A4: TypeTag,
    A5: TypeTag,
    A6: TypeTag,
    A7: TypeTag,
    A8: TypeTag,
    A9: TypeTag,
    A10: TypeTag]
    extends UDTF {

  /** This method is invoked once for each row in the input partition. The arguments passed to the
    * registered UDTF are passed to process().
    *
    * The rows returned in this method must match the StructType defined in [[outputSchema]]
    *
    * Since: 1.2.0
    */
  // scalastyle:off
  def process(
      arg0: A0,
      arg1: A1,
      arg2: A2,
      arg3: A3,
      arg4: A4,
      arg5: A5,
      arg6: A6,
      arg7: A7,
      arg8: A8,
      arg9: A9,
      arg10: A10): Iterable[Row]
  // scalastyle:on

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1),
      ScalaFunctions.schemaForUdfColumn[A1](2),
      ScalaFunctions.schemaForUdfColumn[A2](3),
      ScalaFunctions.schemaForUdfColumn[A3](4),
      ScalaFunctions.schemaForUdfColumn[A4](5),
      ScalaFunctions.schemaForUdfColumn[A5](6),
      ScalaFunctions.schemaForUdfColumn[A6](7),
      ScalaFunctions.schemaForUdfColumn[A7](8),
      ScalaFunctions.schemaForUdfColumn[A8](9),
      ScalaFunctions.schemaForUdfColumn[A9](10),
      ScalaFunctions.schemaForUdfColumn[A10](11))
}

/** The Scala UDTF (user-defined table function) abstract class that has 12 arguments.
  *
  * @since 1.2.0
  */
abstract class UDTF12[
    A0: TypeTag,
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
    A11: TypeTag]
    extends UDTF {

  /** This method is invoked once for each row in the input partition. The arguments passed to the
    * registered UDTF are passed to process().
    *
    * The rows returned in this method must match the StructType defined in [[outputSchema]]
    *
    * Since: 1.2.0
    */
  // scalastyle:off
  def process(
      arg0: A0,
      arg1: A1,
      arg2: A2,
      arg3: A3,
      arg4: A4,
      arg5: A5,
      arg6: A6,
      arg7: A7,
      arg8: A8,
      arg9: A9,
      arg10: A10,
      arg11: A11): Iterable[Row]
  // scalastyle:on

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1),
      ScalaFunctions.schemaForUdfColumn[A1](2),
      ScalaFunctions.schemaForUdfColumn[A2](3),
      ScalaFunctions.schemaForUdfColumn[A3](4),
      ScalaFunctions.schemaForUdfColumn[A4](5),
      ScalaFunctions.schemaForUdfColumn[A5](6),
      ScalaFunctions.schemaForUdfColumn[A6](7),
      ScalaFunctions.schemaForUdfColumn[A7](8),
      ScalaFunctions.schemaForUdfColumn[A8](9),
      ScalaFunctions.schemaForUdfColumn[A9](10),
      ScalaFunctions.schemaForUdfColumn[A10](11),
      ScalaFunctions.schemaForUdfColumn[A11](12))
}

/** The Scala UDTF (user-defined table function) abstract class that has 13 arguments.
  *
  * @since 1.2.0
  */
abstract class UDTF13[
    A0: TypeTag,
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
    A12: TypeTag]
    extends UDTF {

  /** This method is invoked once for each row in the input partition. The arguments passed to the
    * registered UDTF are passed to process().
    *
    * The rows returned in this method must match the StructType defined in [[outputSchema]]
    *
    * Since: 1.2.0
    */
  // scalastyle:off
  def process(
      arg0: A0,
      arg1: A1,
      arg2: A2,
      arg3: A3,
      arg4: A4,
      arg5: A5,
      arg6: A6,
      arg7: A7,
      arg8: A8,
      arg9: A9,
      arg10: A10,
      arg11: A11,
      arg12: A12): Iterable[Row]
  // scalastyle:on

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1),
      ScalaFunctions.schemaForUdfColumn[A1](2),
      ScalaFunctions.schemaForUdfColumn[A2](3),
      ScalaFunctions.schemaForUdfColumn[A3](4),
      ScalaFunctions.schemaForUdfColumn[A4](5),
      ScalaFunctions.schemaForUdfColumn[A5](6),
      ScalaFunctions.schemaForUdfColumn[A6](7),
      ScalaFunctions.schemaForUdfColumn[A7](8),
      ScalaFunctions.schemaForUdfColumn[A8](9),
      ScalaFunctions.schemaForUdfColumn[A9](10),
      ScalaFunctions.schemaForUdfColumn[A10](11),
      ScalaFunctions.schemaForUdfColumn[A11](12),
      ScalaFunctions.schemaForUdfColumn[A12](13))
}

/** The Scala UDTF (user-defined table function) abstract class that has 14 arguments.
  *
  * @since 1.2.0
  */
abstract class UDTF14[
    A0: TypeTag,
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
    A13: TypeTag]
    extends UDTF {

  /** This method is invoked once for each row in the input partition. The arguments passed to the
    * registered UDTF are passed to process().
    *
    * The rows returned in this method must match the StructType defined in [[outputSchema]]
    *
    * Since: 1.2.0
    */
  // scalastyle:off
  def process(
      arg0: A0,
      arg1: A1,
      arg2: A2,
      arg3: A3,
      arg4: A4,
      arg5: A5,
      arg6: A6,
      arg7: A7,
      arg8: A8,
      arg9: A9,
      arg10: A10,
      arg11: A11,
      arg12: A12,
      arg13: A13): Iterable[Row]
  // scalastyle:on

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1),
      ScalaFunctions.schemaForUdfColumn[A1](2),
      ScalaFunctions.schemaForUdfColumn[A2](3),
      ScalaFunctions.schemaForUdfColumn[A3](4),
      ScalaFunctions.schemaForUdfColumn[A4](5),
      ScalaFunctions.schemaForUdfColumn[A5](6),
      ScalaFunctions.schemaForUdfColumn[A6](7),
      ScalaFunctions.schemaForUdfColumn[A7](8),
      ScalaFunctions.schemaForUdfColumn[A8](9),
      ScalaFunctions.schemaForUdfColumn[A9](10),
      ScalaFunctions.schemaForUdfColumn[A10](11),
      ScalaFunctions.schemaForUdfColumn[A11](12),
      ScalaFunctions.schemaForUdfColumn[A12](13),
      ScalaFunctions.schemaForUdfColumn[A13](14))
}

/** The Scala UDTF (user-defined table function) abstract class that has 15 arguments.
  *
  * @since 1.2.0
  */
abstract class UDTF15[
    A0: TypeTag,
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
    A14: TypeTag]
    extends UDTF {

  /** This method is invoked once for each row in the input partition. The arguments passed to the
    * registered UDTF are passed to process().
    *
    * The rows returned in this method must match the StructType defined in [[outputSchema]]
    *
    * Since: 1.2.0
    */
  // scalastyle:off
  def process(
      arg0: A0,
      arg1: A1,
      arg2: A2,
      arg3: A3,
      arg4: A4,
      arg5: A5,
      arg6: A6,
      arg7: A7,
      arg8: A8,
      arg9: A9,
      arg10: A10,
      arg11: A11,
      arg12: A12,
      arg13: A13,
      arg14: A14): Iterable[Row]
  // scalastyle:on

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1),
      ScalaFunctions.schemaForUdfColumn[A1](2),
      ScalaFunctions.schemaForUdfColumn[A2](3),
      ScalaFunctions.schemaForUdfColumn[A3](4),
      ScalaFunctions.schemaForUdfColumn[A4](5),
      ScalaFunctions.schemaForUdfColumn[A5](6),
      ScalaFunctions.schemaForUdfColumn[A6](7),
      ScalaFunctions.schemaForUdfColumn[A7](8),
      ScalaFunctions.schemaForUdfColumn[A8](9),
      ScalaFunctions.schemaForUdfColumn[A9](10),
      ScalaFunctions.schemaForUdfColumn[A10](11),
      ScalaFunctions.schemaForUdfColumn[A11](12),
      ScalaFunctions.schemaForUdfColumn[A12](13),
      ScalaFunctions.schemaForUdfColumn[A13](14),
      ScalaFunctions.schemaForUdfColumn[A14](15))
}

/** The Scala UDTF (user-defined table function) abstract class that has 16 arguments.
  *
  * @since 1.2.0
  */
abstract class UDTF16[
    A0: TypeTag,
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
    A15: TypeTag]
    extends UDTF {

  /** This method is invoked once for each row in the input partition. The arguments passed to the
    * registered UDTF are passed to process().
    *
    * The rows returned in this method must match the StructType defined in [[outputSchema]]
    *
    * Since: 1.2.0
    */
  // scalastyle:off
  def process(
      arg0: A0,
      arg1: A1,
      arg2: A2,
      arg3: A3,
      arg4: A4,
      arg5: A5,
      arg6: A6,
      arg7: A7,
      arg8: A8,
      arg9: A9,
      arg10: A10,
      arg11: A11,
      arg12: A12,
      arg13: A13,
      arg14: A14,
      arg15: A15): Iterable[Row]
  // scalastyle:on

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1),
      ScalaFunctions.schemaForUdfColumn[A1](2),
      ScalaFunctions.schemaForUdfColumn[A2](3),
      ScalaFunctions.schemaForUdfColumn[A3](4),
      ScalaFunctions.schemaForUdfColumn[A4](5),
      ScalaFunctions.schemaForUdfColumn[A5](6),
      ScalaFunctions.schemaForUdfColumn[A6](7),
      ScalaFunctions.schemaForUdfColumn[A7](8),
      ScalaFunctions.schemaForUdfColumn[A8](9),
      ScalaFunctions.schemaForUdfColumn[A9](10),
      ScalaFunctions.schemaForUdfColumn[A10](11),
      ScalaFunctions.schemaForUdfColumn[A11](12),
      ScalaFunctions.schemaForUdfColumn[A12](13),
      ScalaFunctions.schemaForUdfColumn[A13](14),
      ScalaFunctions.schemaForUdfColumn[A14](15),
      ScalaFunctions.schemaForUdfColumn[A15](16))
}

/** The Scala UDTF (user-defined table function) abstract class that has 17 arguments.
  *
  * @since 1.2.0
  */
abstract class UDTF17[
    A0: TypeTag,
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
    A16: TypeTag]
    extends UDTF {

  /** This method is invoked once for each row in the input partition. The arguments passed to the
    * registered UDTF are passed to process().
    *
    * The rows returned in this method must match the StructType defined in [[outputSchema]]
    *
    * Since: 1.2.0
    */
  // scalastyle:off
  def process(
      arg0: A0,
      arg1: A1,
      arg2: A2,
      arg3: A3,
      arg4: A4,
      arg5: A5,
      arg6: A6,
      arg7: A7,
      arg8: A8,
      arg9: A9,
      arg10: A10,
      arg11: A11,
      arg12: A12,
      arg13: A13,
      arg14: A14,
      arg15: A15,
      arg16: A16): Iterable[Row]
  // scalastyle:on

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1),
      ScalaFunctions.schemaForUdfColumn[A1](2),
      ScalaFunctions.schemaForUdfColumn[A2](3),
      ScalaFunctions.schemaForUdfColumn[A3](4),
      ScalaFunctions.schemaForUdfColumn[A4](5),
      ScalaFunctions.schemaForUdfColumn[A5](6),
      ScalaFunctions.schemaForUdfColumn[A6](7),
      ScalaFunctions.schemaForUdfColumn[A7](8),
      ScalaFunctions.schemaForUdfColumn[A8](9),
      ScalaFunctions.schemaForUdfColumn[A9](10),
      ScalaFunctions.schemaForUdfColumn[A10](11),
      ScalaFunctions.schemaForUdfColumn[A11](12),
      ScalaFunctions.schemaForUdfColumn[A12](13),
      ScalaFunctions.schemaForUdfColumn[A13](14),
      ScalaFunctions.schemaForUdfColumn[A14](15),
      ScalaFunctions.schemaForUdfColumn[A15](16),
      ScalaFunctions.schemaForUdfColumn[A16](17))
}

/** The Scala UDTF (user-defined table function) abstract class that has 18 arguments.
  *
  * @since 1.2.0
  */
abstract class UDTF18[
    A0: TypeTag,
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
    A17: TypeTag]
    extends UDTF {

  /** This method is invoked once for each row in the input partition. The arguments passed to the
    * registered UDTF are passed to process().
    *
    * The rows returned in this method must match the StructType defined in [[outputSchema]]
    *
    * Since: 1.2.0
    */
  // scalastyle:off
  def process(
      arg0: A0,
      arg1: A1,
      arg2: A2,
      arg3: A3,
      arg4: A4,
      arg5: A5,
      arg6: A6,
      arg7: A7,
      arg8: A8,
      arg9: A9,
      arg10: A10,
      arg11: A11,
      arg12: A12,
      arg13: A13,
      arg14: A14,
      arg15: A15,
      arg16: A16,
      arg17: A17): Iterable[Row]
  // scalastyle:on

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1),
      ScalaFunctions.schemaForUdfColumn[A1](2),
      ScalaFunctions.schemaForUdfColumn[A2](3),
      ScalaFunctions.schemaForUdfColumn[A3](4),
      ScalaFunctions.schemaForUdfColumn[A4](5),
      ScalaFunctions.schemaForUdfColumn[A5](6),
      ScalaFunctions.schemaForUdfColumn[A6](7),
      ScalaFunctions.schemaForUdfColumn[A7](8),
      ScalaFunctions.schemaForUdfColumn[A8](9),
      ScalaFunctions.schemaForUdfColumn[A9](10),
      ScalaFunctions.schemaForUdfColumn[A10](11),
      ScalaFunctions.schemaForUdfColumn[A11](12),
      ScalaFunctions.schemaForUdfColumn[A12](13),
      ScalaFunctions.schemaForUdfColumn[A13](14),
      ScalaFunctions.schemaForUdfColumn[A14](15),
      ScalaFunctions.schemaForUdfColumn[A15](16),
      ScalaFunctions.schemaForUdfColumn[A16](17),
      ScalaFunctions.schemaForUdfColumn[A17](18))
}

/** The Scala UDTF (user-defined table function) abstract class that has 19 arguments.
  *
  * @since 1.2.0
  */
abstract class UDTF19[
    A0: TypeTag,
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
    A18: TypeTag]
    extends UDTF {

  /** This method is invoked once for each row in the input partition. The arguments passed to the
    * registered UDTF are passed to process().
    *
    * The rows returned in this method must match the StructType defined in [[outputSchema]]
    *
    * Since: 1.2.0
    */
  // scalastyle:off
  def process(
      arg0: A0,
      arg1: A1,
      arg2: A2,
      arg3: A3,
      arg4: A4,
      arg5: A5,
      arg6: A6,
      arg7: A7,
      arg8: A8,
      arg9: A9,
      arg10: A10,
      arg11: A11,
      arg12: A12,
      arg13: A13,
      arg14: A14,
      arg15: A15,
      arg16: A16,
      arg17: A17,
      arg18: A18): Iterable[Row]
  // scalastyle:on

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1),
      ScalaFunctions.schemaForUdfColumn[A1](2),
      ScalaFunctions.schemaForUdfColumn[A2](3),
      ScalaFunctions.schemaForUdfColumn[A3](4),
      ScalaFunctions.schemaForUdfColumn[A4](5),
      ScalaFunctions.schemaForUdfColumn[A5](6),
      ScalaFunctions.schemaForUdfColumn[A6](7),
      ScalaFunctions.schemaForUdfColumn[A7](8),
      ScalaFunctions.schemaForUdfColumn[A8](9),
      ScalaFunctions.schemaForUdfColumn[A9](10),
      ScalaFunctions.schemaForUdfColumn[A10](11),
      ScalaFunctions.schemaForUdfColumn[A11](12),
      ScalaFunctions.schemaForUdfColumn[A12](13),
      ScalaFunctions.schemaForUdfColumn[A13](14),
      ScalaFunctions.schemaForUdfColumn[A14](15),
      ScalaFunctions.schemaForUdfColumn[A15](16),
      ScalaFunctions.schemaForUdfColumn[A16](17),
      ScalaFunctions.schemaForUdfColumn[A17](18),
      ScalaFunctions.schemaForUdfColumn[A18](19))
}

/** The Scala UDTF (user-defined table function) abstract class that has 20 arguments.
  *
  * @since 1.2.0
  */
abstract class UDTF20[
    A0: TypeTag,
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
    A19: TypeTag]
    extends UDTF {

  /** This method is invoked once for each row in the input partition. The arguments passed to the
    * registered UDTF are passed to process().
    *
    * The rows returned in this method must match the StructType defined in [[outputSchema]]
    *
    * Since: 1.2.0
    */
  // scalastyle:off
  def process(
      arg0: A0,
      arg1: A1,
      arg2: A2,
      arg3: A3,
      arg4: A4,
      arg5: A5,
      arg6: A6,
      arg7: A7,
      arg8: A8,
      arg9: A9,
      arg10: A10,
      arg11: A11,
      arg12: A12,
      arg13: A13,
      arg14: A14,
      arg15: A15,
      arg16: A16,
      arg17: A17,
      arg18: A18,
      arg19: A19): Iterable[Row]
  // scalastyle:on

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1),
      ScalaFunctions.schemaForUdfColumn[A1](2),
      ScalaFunctions.schemaForUdfColumn[A2](3),
      ScalaFunctions.schemaForUdfColumn[A3](4),
      ScalaFunctions.schemaForUdfColumn[A4](5),
      ScalaFunctions.schemaForUdfColumn[A5](6),
      ScalaFunctions.schemaForUdfColumn[A6](7),
      ScalaFunctions.schemaForUdfColumn[A7](8),
      ScalaFunctions.schemaForUdfColumn[A8](9),
      ScalaFunctions.schemaForUdfColumn[A9](10),
      ScalaFunctions.schemaForUdfColumn[A10](11),
      ScalaFunctions.schemaForUdfColumn[A11](12),
      ScalaFunctions.schemaForUdfColumn[A12](13),
      ScalaFunctions.schemaForUdfColumn[A13](14),
      ScalaFunctions.schemaForUdfColumn[A14](15),
      ScalaFunctions.schemaForUdfColumn[A15](16),
      ScalaFunctions.schemaForUdfColumn[A16](17),
      ScalaFunctions.schemaForUdfColumn[A17](18),
      ScalaFunctions.schemaForUdfColumn[A18](19),
      ScalaFunctions.schemaForUdfColumn[A19](20))
}

/** The Scala UDTF (user-defined table function) abstract class that has 21 arguments.
  *
  * @since 1.2.0
  */
abstract class UDTF21[
    A0: TypeTag,
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
    A20: TypeTag]
    extends UDTF {

  /** This method is invoked once for each row in the input partition. The arguments passed to the
    * registered UDTF are passed to process().
    *
    * The rows returned in this method must match the StructType defined in [[outputSchema]]
    *
    * Since: 1.2.0
    */
  // scalastyle:off
  def process(
      arg0: A0,
      arg1: A1,
      arg2: A2,
      arg3: A3,
      arg4: A4,
      arg5: A5,
      arg6: A6,
      arg7: A7,
      arg8: A8,
      arg9: A9,
      arg10: A10,
      arg11: A11,
      arg12: A12,
      arg13: A13,
      arg14: A14,
      arg15: A15,
      arg16: A16,
      arg17: A17,
      arg18: A18,
      arg19: A19,
      arg20: A20): Iterable[Row]
  // scalastyle:on

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1),
      ScalaFunctions.schemaForUdfColumn[A1](2),
      ScalaFunctions.schemaForUdfColumn[A2](3),
      ScalaFunctions.schemaForUdfColumn[A3](4),
      ScalaFunctions.schemaForUdfColumn[A4](5),
      ScalaFunctions.schemaForUdfColumn[A5](6),
      ScalaFunctions.schemaForUdfColumn[A6](7),
      ScalaFunctions.schemaForUdfColumn[A7](8),
      ScalaFunctions.schemaForUdfColumn[A8](9),
      ScalaFunctions.schemaForUdfColumn[A9](10),
      ScalaFunctions.schemaForUdfColumn[A10](11),
      ScalaFunctions.schemaForUdfColumn[A11](12),
      ScalaFunctions.schemaForUdfColumn[A12](13),
      ScalaFunctions.schemaForUdfColumn[A13](14),
      ScalaFunctions.schemaForUdfColumn[A14](15),
      ScalaFunctions.schemaForUdfColumn[A15](16),
      ScalaFunctions.schemaForUdfColumn[A16](17),
      ScalaFunctions.schemaForUdfColumn[A17](18),
      ScalaFunctions.schemaForUdfColumn[A18](19),
      ScalaFunctions.schemaForUdfColumn[A19](20),
      ScalaFunctions.schemaForUdfColumn[A20](21))
}

/** The Scala UDTF (user-defined table function) abstract class that has 22 arguments.
  *
  * @since 1.2.0
  */
abstract class UDTF22[
    A0: TypeTag,
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
    A21: TypeTag]
    extends UDTF {

  /** This method is invoked once for each row in the input partition. The arguments passed to the
    * registered UDTF are passed to process().
    *
    * The rows returned in this method must match the StructType defined in [[outputSchema]]
    *
    * Since: 1.2.0
    */
  // scalastyle:off
  def process(
      arg0: A0,
      arg1: A1,
      arg2: A2,
      arg3: A3,
      arg4: A4,
      arg5: A5,
      arg6: A6,
      arg7: A7,
      arg8: A8,
      arg9: A9,
      arg10: A10,
      arg11: A11,
      arg12: A12,
      arg13: A13,
      arg14: A14,
      arg15: A15,
      arg16: A16,
      arg17: A17,
      arg18: A18,
      arg19: A19,
      arg20: A20,
      arg21: A21): Iterable[Row]
  // scalastyle:on

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1),
      ScalaFunctions.schemaForUdfColumn[A1](2),
      ScalaFunctions.schemaForUdfColumn[A2](3),
      ScalaFunctions.schemaForUdfColumn[A3](4),
      ScalaFunctions.schemaForUdfColumn[A4](5),
      ScalaFunctions.schemaForUdfColumn[A5](6),
      ScalaFunctions.schemaForUdfColumn[A6](7),
      ScalaFunctions.schemaForUdfColumn[A7](8),
      ScalaFunctions.schemaForUdfColumn[A8](9),
      ScalaFunctions.schemaForUdfColumn[A9](10),
      ScalaFunctions.schemaForUdfColumn[A10](11),
      ScalaFunctions.schemaForUdfColumn[A11](12),
      ScalaFunctions.schemaForUdfColumn[A12](13),
      ScalaFunctions.schemaForUdfColumn[A13](14),
      ScalaFunctions.schemaForUdfColumn[A14](15),
      ScalaFunctions.schemaForUdfColumn[A15](16),
      ScalaFunctions.schemaForUdfColumn[A16](17),
      ScalaFunctions.schemaForUdfColumn[A17](18),
      ScalaFunctions.schemaForUdfColumn[A18](19),
      ScalaFunctions.schemaForUdfColumn[A19](20),
      ScalaFunctions.schemaForUdfColumn[A20](21),
      ScalaFunctions.schemaForUdfColumn[A21](22))
}
