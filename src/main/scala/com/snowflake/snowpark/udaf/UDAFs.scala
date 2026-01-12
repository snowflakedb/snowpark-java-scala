package com.snowflake.snowpark.udaf

import com.snowflake.snowpark.internal.{ScalaFunctions, UdfColumn}
import com.snowflake.snowpark.types.{DataType, StructType}

import scala.reflect.runtime.universe.TypeTag

/**
 * The Scala UDAF (user-defined aggregate function) trait.
 * @since 1.19.0
 */
sealed trait UDAF extends java.io.Serializable {

  /**
   * A DataType that describes the type of the value returned by the terminate() method.
   *
   * For example, if a UDAF returns an integer value, outputType() should return IntegerType: {{
   * override def outputType(): DataType = IntegerType }}
   *
   * Since: 1.19.0
   */
  def outputType(): DataType

  /**
   * A StructType that describes the input schema of the aggregate function. By default, returns an
   * empty StructType.
   *
   * Since: 1.19.0
   */
  def inputSchema(): StructType = StructType()

  // Below are internal private functions
  private[snowpark] def inputColumns: Seq[UdfColumn]
}

/**
 * The Scala UDAF (user-defined aggregate function) abstract class that has no argument.
 *
 * @tparam S
 *   The type of the aggregation state.
 * @tparam O
 *   The type of the output value.
 * @since 1.19.0
 */
abstract class UDAF0[S, O] extends UDAF {

  /**
   * Returns the initial state of the aggregation.
   *
   * Since: 1.19.0
   */
  def initialize(): S

  /**
   * Updates the aggregation state with a new input row (no arguments).
   *
   * @param state
   *   The current aggregation state.
   *
   * Since: 1.19.0
   */
  def accumulate(state: S): S

  /**
   * Merges two aggregation states into one.
   *
   * @param state1
   *   The first aggregation state.
   * @param state2
   *   The second aggregation state.
   * @return
   *   The merged aggregation state.
   *
   * Since: 1.19.0
   */
  def merge(state1: S, state2: S): S

  /**
   * Produces the final output value from the aggregation state.
   *
   * @param state
   *   The final aggregation state.
   * @return
   *   The output value.
   *
   * Since: 1.19.0
   */
  def terminate(state: S): O

  override private[snowpark] def inputColumns: Seq[UdfColumn] = Seq.empty
}

// scalastyle:off
/*
  test("generate UDAF classes 1 - 22") {
    (1 to 22).foreach { x =>
      val className = s"UDAF$x"
      val argTypes = (0 until x).map(i => s"A$i: TypeTag").mkString(", ")
      val accumulateArgs = (0 until x).map(i => s"arg$i: A$i").mkString(", ")
      val inputColumns =
        (0 until x).map(i => s"ScalaFunctions.schemaForUdfColumn[A$i](${i + 1})").mkString(", ")
      val s = if (x > 1) "s" else ""
      val code =
        s"""
           |/**
           | * The Scala UDAF (user-defined aggregate function) abstract class that has $x argument$s.
           | *
           | * @tparam S The type of the aggregation state.
           | * @tparam O The type of the output value.
           | * @since 1.19.0
           | */
           |abstract class $className[S, O, $argTypes] extends UDAF {
           |
           |  /**
           |   * Returns the initial state of the aggregation.
           |   *
           |   * Since: 1.19.0
           |   */
           |  def initialize(): S
           |
           |  /**
           |   * Updates the aggregation state with a new input row.
           |   *
           |   * @param state The current aggregation state.
           |   *
           |   * Since: 1.19.0
           |   */
           |  // scalastyle:off
           |  def accumulate(state: S, $accumulateArgs): S
           |  // scalastyle:on
           |
           |  /**
           |   * Merges two aggregation states into one.
           |   *
           |   * @param state1 The first aggregation state.
           |   * @param state2 The second aggregation state.
           |   * @return The merged aggregation state.
           |   *
           |   * Since: 1.19.0
           |   */
           |  def merge(state1: S, state2: S): S
           |
           |  /**
           |   * Produces the final output value from the aggregation state.
           |   *
           |   * @param state The final aggregation state.
           |   * @return The output value.
           |   *
           |   * Since: 1.19.0
           |   */
           |  def terminate(state: S): O
           |
           |  override private[snowpark] def inputColumns: Seq[UdfColumn] =
           |    Seq($inputColumns)
           |}""".stripMargin
      println(code)
    }
  }
 */
// scalastyle:on

/**
 * The Scala UDAF (user-defined aggregate function) abstract class that has 1 argument.
 *
 * @tparam S
 *   The type of the aggregation state.
 * @tparam O
 *   The type of the output value.
 * @since 1.19.0
 */
abstract class UDAF1[S, O, A0](implicit @transient private val a0Tag: TypeTag[A0]) extends UDAF {

  /**
   * Returns the initial state of the aggregation.
   *
   * Since: 1.19.0
   */
  def initialize(): S

  /**
   * Updates the aggregation state with a new input row.
   *
   * @param state
   *   The current aggregation state.
   *
   * Since: 1.19.0
   */
  def accumulate(state: S, arg0: A0): S

  /**
   * Merges two aggregation states into one.
   *
   * @param state1
   *   The first aggregation state.
   * @param state2
   *   The second aggregation state.
   * @return
   *   The merged aggregation state.
   *
   * Since: 1.19.0
   */
  def merge(state1: S, state2: S): S

  /**
   * Produces the final output value from the aggregation state.
   *
   * @param state
   *   The final aggregation state.
   * @return
   *   The output value.
   *
   * Since: 1.19.0
   */
  def terminate(state: S): O

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(ScalaFunctions.schemaForUdfColumn[A0](1)(a0Tag))
}

/**
 * The Scala UDAF (user-defined aggregate function) abstract class that has 2 arguments.
 *
 * @tparam S
 *   The type of the aggregation state.
 * @tparam O
 *   The type of the output value.
 * @since 1.19.0
 */
abstract class UDAF2[S, O, A0, A1](implicit
    @transient private val a0Tag: TypeTag[A0],
    @transient private val a1Tag: TypeTag[A1])
    extends UDAF {

  /**
   * Returns the initial state of the aggregation.
   *
   * Since: 1.19.0
   */
  def initialize(): S

  /**
   * Updates the aggregation state with a new input row.
   *
   * @param state
   *   The current aggregation state.
   *
   * Since: 1.19.0
   */
  def accumulate(state: S, arg0: A0, arg1: A1): S

  /**
   * Merges two aggregation states into one.
   *
   * @param state1
   *   The first aggregation state.
   * @param state2
   *   The second aggregation state.
   * @return
   *   The merged aggregation state.
   *
   * Since: 1.19.0
   */
  def merge(state1: S, state2: S): S

  /**
   * Produces the final output value from the aggregation state.
   *
   * @param state
   *   The final aggregation state.
   * @return
   *   The output value.
   *
   * Since: 1.19.0
   */
  def terminate(state: S): O

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1)(a0Tag),
      ScalaFunctions.schemaForUdfColumn[A1](2)(a1Tag))
}

/**
 * The Scala UDAF (user-defined aggregate function) abstract class that has 3 arguments.
 *
 * @tparam S
 *   The type of the aggregation state.
 * @tparam O
 *   The type of the output value.
 * @since 1.19.0
 */
abstract class UDAF3[S, O, A0, A1, A2](implicit
    @transient private val a0Tag: TypeTag[A0],
    @transient private val a1Tag: TypeTag[A1],
    @transient private val a2Tag: TypeTag[A2])
    extends UDAF {

  /**
   * Returns the initial state of the aggregation.
   *
   * Since: 1.19.0
   */
  def initialize(): S

  /**
   * Updates the aggregation state with a new input row.
   *
   * @param state
   *   The current aggregation state.
   *
   * Since: 1.19.0
   */
  def accumulate(state: S, arg0: A0, arg1: A1, arg2: A2): S

  /**
   * Merges two aggregation states into one.
   *
   * @param state1
   *   The first aggregation state.
   * @param state2
   *   The second aggregation state.
   * @return
   *   The merged aggregation state.
   *
   * Since: 1.19.0
   */
  def merge(state1: S, state2: S): S

  /**
   * Produces the final output value from the aggregation state.
   *
   * @param state
   *   The final aggregation state.
   * @return
   *   The output value.
   *
   * Since: 1.19.0
   */
  def terminate(state: S): O

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1)(a0Tag),
      ScalaFunctions.schemaForUdfColumn[A1](2)(a1Tag),
      ScalaFunctions.schemaForUdfColumn[A2](3)(a2Tag))
}

/**
 * The Scala UDAF (user-defined aggregate function) abstract class that has 4 arguments.
 *
 * @tparam S
 *   The type of the aggregation state.
 * @tparam O
 *   The type of the output value.
 * @since 1.19.0
 */
abstract class UDAF4[S, O, A0, A1, A2, A3](implicit
    @transient private val a0Tag: TypeTag[A0],
    @transient private val a1Tag: TypeTag[A1],
    @transient private val a2Tag: TypeTag[A2],
    @transient private val a3Tag: TypeTag[A3])
    extends UDAF {

  /**
   * Returns the initial state of the aggregation.
   *
   * Since: 1.19.0
   */
  def initialize(): S

  /**
   * Updates the aggregation state with a new input row.
   *
   * @param state
   *   The current aggregation state.
   *
   * Since: 1.19.0
   */
  def accumulate(state: S, arg0: A0, arg1: A1, arg2: A2, arg3: A3): S

  /**
   * Merges two aggregation states into one.
   *
   * @param state1
   *   The first aggregation state.
   * @param state2
   *   The second aggregation state.
   * @return
   *   The merged aggregation state.
   *
   * Since: 1.19.0
   */
  def merge(state1: S, state2: S): S

  /**
   * Produces the final output value from the aggregation state.
   *
   * @param state
   *   The final aggregation state.
   * @return
   *   The output value.
   *
   * Since: 1.19.0
   */
  def terminate(state: S): O

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1)(a0Tag),
      ScalaFunctions.schemaForUdfColumn[A1](2)(a1Tag),
      ScalaFunctions.schemaForUdfColumn[A2](3)(a2Tag),
      ScalaFunctions.schemaForUdfColumn[A3](4)(a3Tag))
}

/**
 * The Scala UDAF (user-defined aggregate function) abstract class that has 5 arguments.
 *
 * @tparam S
 *   The type of the aggregation state.
 * @tparam O
 *   The type of the output value.
 * @since 1.19.0
 */
abstract class UDAF5[S, O, A0, A1, A2, A3, A4](implicit
    @transient private val a0Tag: TypeTag[A0],
    @transient private val a1Tag: TypeTag[A1],
    @transient private val a2Tag: TypeTag[A2],
    @transient private val a3Tag: TypeTag[A3],
    @transient private val a4Tag: TypeTag[A4])
    extends UDAF {

  /**
   * Returns the initial state of the aggregation.
   *
   * Since: 1.19.0
   */
  def initialize(): S

  /**
   * Updates the aggregation state with a new input row.
   *
   * @param state
   *   The current aggregation state.
   *
   * Since: 1.19.0
   */
  def accumulate(state: S, arg0: A0, arg1: A1, arg2: A2, arg3: A3, arg4: A4): S

  /**
   * Merges two aggregation states into one.
   *
   * @param state1
   *   The first aggregation state.
   * @param state2
   *   The second aggregation state.
   * @return
   *   The merged aggregation state.
   *
   * Since: 1.19.0
   */
  def merge(state1: S, state2: S): S

  /**
   * Produces the final output value from the aggregation state.
   *
   * @param state
   *   The final aggregation state.
   * @return
   *   The output value.
   *
   * Since: 1.19.0
   */
  def terminate(state: S): O

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1)(a0Tag),
      ScalaFunctions.schemaForUdfColumn[A1](2)(a1Tag),
      ScalaFunctions.schemaForUdfColumn[A2](3)(a2Tag),
      ScalaFunctions.schemaForUdfColumn[A3](4)(a3Tag),
      ScalaFunctions.schemaForUdfColumn[A4](5)(a4Tag))
}

/**
 * The Scala UDAF (user-defined aggregate function) abstract class that has 6 arguments.
 *
 * @tparam S
 *   The type of the aggregation state.
 * @tparam O
 *   The type of the output value.
 * @since 1.19.0
 */
abstract class UDAF6[S, O, A0, A1, A2, A3, A4, A5](implicit
    @transient private val a0Tag: TypeTag[A0],
    @transient private val a1Tag: TypeTag[A1],
    @transient private val a2Tag: TypeTag[A2],
    @transient private val a3Tag: TypeTag[A3],
    @transient private val a4Tag: TypeTag[A4],
    @transient private val a5Tag: TypeTag[A5])
    extends UDAF {

  /**
   * Returns the initial state of the aggregation.
   *
   * Since: 1.19.0
   */
  def initialize(): S

  /**
   * Updates the aggregation state with a new input row.
   *
   * @param state
   *   The current aggregation state.
   *
   * Since: 1.19.0
   */
  def accumulate(state: S, arg0: A0, arg1: A1, arg2: A2, arg3: A3, arg4: A4, arg5: A5): S

  /**
   * Merges two aggregation states into one.
   *
   * @param state1
   *   The first aggregation state.
   * @param state2
   *   The second aggregation state.
   * @return
   *   The merged aggregation state.
   *
   * Since: 1.19.0
   */
  def merge(state1: S, state2: S): S

  /**
   * Produces the final output value from the aggregation state.
   *
   * @param state
   *   The final aggregation state.
   * @return
   *   The output value.
   *
   * Since: 1.19.0
   */
  def terminate(state: S): O

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1)(a0Tag),
      ScalaFunctions.schemaForUdfColumn[A1](2)(a1Tag),
      ScalaFunctions.schemaForUdfColumn[A2](3)(a2Tag),
      ScalaFunctions.schemaForUdfColumn[A3](4)(a3Tag),
      ScalaFunctions.schemaForUdfColumn[A4](5)(a4Tag),
      ScalaFunctions.schemaForUdfColumn[A5](6)(a5Tag))
}

/**
 * The Scala UDAF (user-defined aggregate function) abstract class that has 7 arguments.
 *
 * @tparam S
 *   The type of the aggregation state.
 * @tparam O
 *   The type of the output value.
 * @since 1.19.0
 */
abstract class UDAF7[S, O, A0, A1, A2, A3, A4, A5, A6](implicit
    @transient private val a0Tag: TypeTag[A0],
    @transient private val a1Tag: TypeTag[A1],
    @transient private val a2Tag: TypeTag[A2],
    @transient private val a3Tag: TypeTag[A3],
    @transient private val a4Tag: TypeTag[A4],
    @transient private val a5Tag: TypeTag[A5],
    @transient private val a6Tag: TypeTag[A6])
    extends UDAF {

  /**
   * Returns the initial state of the aggregation.
   *
   * Since: 1.19.0
   */
  def initialize(): S

  /**
   * Updates the aggregation state with a new input row.
   *
   * @param state
   *   The current aggregation state.
   *
   * Since: 1.19.0
   */
  def accumulate(state: S, arg0: A0, arg1: A1, arg2: A2, arg3: A3, arg4: A4, arg5: A5, arg6: A6): S

  /**
   * Merges two aggregation states into one.
   *
   * @param state1
   *   The first aggregation state.
   * @param state2
   *   The second aggregation state.
   * @return
   *   The merged aggregation state.
   *
   * Since: 1.19.0
   */
  def merge(state1: S, state2: S): S

  /**
   * Produces the final output value from the aggregation state.
   *
   * @param state
   *   The final aggregation state.
   * @return
   *   The output value.
   *
   * Since: 1.19.0
   */
  def terminate(state: S): O

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1)(a0Tag),
      ScalaFunctions.schemaForUdfColumn[A1](2)(a1Tag),
      ScalaFunctions.schemaForUdfColumn[A2](3)(a2Tag),
      ScalaFunctions.schemaForUdfColumn[A3](4)(a3Tag),
      ScalaFunctions.schemaForUdfColumn[A4](5)(a4Tag),
      ScalaFunctions.schemaForUdfColumn[A5](6)(a5Tag),
      ScalaFunctions.schemaForUdfColumn[A6](7)(a6Tag))
}

/**
 * The Scala UDAF (user-defined aggregate function) abstract class that has 8 arguments.
 *
 * @tparam S
 *   The type of the aggregation state.
 * @tparam O
 *   The type of the output value.
 * @since 1.19.0
 */
abstract class UDAF8[S, O, A0, A1, A2, A3, A4, A5, A6, A7](implicit
    @transient private val a0Tag: TypeTag[A0],
    @transient private val a1Tag: TypeTag[A1],
    @transient private val a2Tag: TypeTag[A2],
    @transient private val a3Tag: TypeTag[A3],
    @transient private val a4Tag: TypeTag[A4],
    @transient private val a5Tag: TypeTag[A5],
    @transient private val a6Tag: TypeTag[A6],
    @transient private val a7Tag: TypeTag[A7])
    extends UDAF {

  /**
   * Returns the initial state of the aggregation.
   *
   * Since: 1.19.0
   */
  def initialize(): S

  /**
   * Updates the aggregation state with a new input row.
   *
   * @param state
   *   The current aggregation state.
   *
   * Since: 1.19.0
   */
  def accumulate(
      state: S,
      arg0: A0,
      arg1: A1,
      arg2: A2,
      arg3: A3,
      arg4: A4,
      arg5: A5,
      arg6: A6,
      arg7: A7): S

  /**
   * Merges two aggregation states into one.
   *
   * @param state1
   *   The first aggregation state.
   * @param state2
   *   The second aggregation state.
   * @return
   *   The merged aggregation state.
   *
   * Since: 1.19.0
   */
  def merge(state1: S, state2: S): S

  /**
   * Produces the final output value from the aggregation state.
   *
   * @param state
   *   The final aggregation state.
   * @return
   *   The output value.
   *
   * Since: 1.19.0
   */
  def terminate(state: S): O

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1)(a0Tag),
      ScalaFunctions.schemaForUdfColumn[A1](2)(a1Tag),
      ScalaFunctions.schemaForUdfColumn[A2](3)(a2Tag),
      ScalaFunctions.schemaForUdfColumn[A3](4)(a3Tag),
      ScalaFunctions.schemaForUdfColumn[A4](5)(a4Tag),
      ScalaFunctions.schemaForUdfColumn[A5](6)(a5Tag),
      ScalaFunctions.schemaForUdfColumn[A6](7)(a6Tag),
      ScalaFunctions.schemaForUdfColumn[A7](8)(a7Tag))
}

/**
 * The Scala UDAF (user-defined aggregate function) abstract class that has 9 arguments.
 *
 * @tparam S
 *   The type of the aggregation state.
 * @tparam O
 *   The type of the output value.
 * @since 1.19.0
 */
abstract class UDAF9[S, O, A0, A1, A2, A3, A4, A5, A6, A7, A8](implicit
    @transient private val a0Tag: TypeTag[A0],
    @transient private val a1Tag: TypeTag[A1],
    @transient private val a2Tag: TypeTag[A2],
    @transient private val a3Tag: TypeTag[A3],
    @transient private val a4Tag: TypeTag[A4],
    @transient private val a5Tag: TypeTag[A5],
    @transient private val a6Tag: TypeTag[A6],
    @transient private val a7Tag: TypeTag[A7],
    @transient private val a8Tag: TypeTag[A8])
    extends UDAF {

  /**
   * Returns the initial state of the aggregation.
   *
   * Since: 1.19.0
   */
  def initialize(): S

  /**
   * Updates the aggregation state with a new input row.
   *
   * @param state
   *   The current aggregation state.
   *
   * Since: 1.19.0
   */
  // scalastyle:off
  def accumulate(
      state: S,
      arg0: A0,
      arg1: A1,
      arg2: A2,
      arg3: A3,
      arg4: A4,
      arg5: A5,
      arg6: A6,
      arg7: A7,
      arg8: A8): S
  // scalastyle:on

  /**
   * Merges two aggregation states into one.
   *
   * @param state1
   *   The first aggregation state.
   * @param state2
   *   The second aggregation state.
   * @return
   *   The merged aggregation state.
   *
   * Since: 1.19.0
   */
  def merge(state1: S, state2: S): S

  /**
   * Produces the final output value from the aggregation state.
   *
   * @param state
   *   The final aggregation state.
   * @return
   *   The output value.
   *
   * Since: 1.19.0
   */
  def terminate(state: S): O

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1)(a0Tag),
      ScalaFunctions.schemaForUdfColumn[A1](2)(a1Tag),
      ScalaFunctions.schemaForUdfColumn[A2](3)(a2Tag),
      ScalaFunctions.schemaForUdfColumn[A3](4)(a3Tag),
      ScalaFunctions.schemaForUdfColumn[A4](5)(a4Tag),
      ScalaFunctions.schemaForUdfColumn[A5](6)(a5Tag),
      ScalaFunctions.schemaForUdfColumn[A6](7)(a6Tag),
      ScalaFunctions.schemaForUdfColumn[A7](8)(a7Tag),
      ScalaFunctions.schemaForUdfColumn[A8](9)(a8Tag))
}

/**
 * The Scala UDAF (user-defined aggregate function) abstract class that has 10 arguments.
 *
 * @tparam S
 *   The type of the aggregation state.
 * @tparam O
 *   The type of the output value.
 * @since 1.19.0
 */
abstract class UDAF10[S, O, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9](implicit
    @transient private val a0Tag: TypeTag[A0],
    @transient private val a1Tag: TypeTag[A1],
    @transient private val a2Tag: TypeTag[A2],
    @transient private val a3Tag: TypeTag[A3],
    @transient private val a4Tag: TypeTag[A4],
    @transient private val a5Tag: TypeTag[A5],
    @transient private val a6Tag: TypeTag[A6],
    @transient private val a7Tag: TypeTag[A7],
    @transient private val a8Tag: TypeTag[A8],
    @transient private val a9Tag: TypeTag[A9])
    extends UDAF {

  /**
   * Returns the initial state of the aggregation.
   *
   * Since: 1.19.0
   */
  def initialize(): S

  /**
   * Updates the aggregation state with a new input row.
   *
   * @param state
   *   The current aggregation state.
   *
   * Since: 1.19.0
   */
  // scalastyle:off
  def accumulate(
      state: S,
      arg0: A0,
      arg1: A1,
      arg2: A2,
      arg3: A3,
      arg4: A4,
      arg5: A5,
      arg6: A6,
      arg7: A7,
      arg8: A8,
      arg9: A9): S
  // scalastyle:on

  /**
   * Merges two aggregation states into one.
   *
   * @param state1
   *   The first aggregation state.
   * @param state2
   *   The second aggregation state.
   * @return
   *   The merged aggregation state.
   *
   * Since: 1.19.0
   */
  def merge(state1: S, state2: S): S

  /**
   * Produces the final output value from the aggregation state.
   *
   * @param state
   *   The final aggregation state.
   * @return
   *   The output value.
   *
   * Since: 1.19.0
   */
  def terminate(state: S): O

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1)(a0Tag),
      ScalaFunctions.schemaForUdfColumn[A1](2)(a1Tag),
      ScalaFunctions.schemaForUdfColumn[A2](3)(a2Tag),
      ScalaFunctions.schemaForUdfColumn[A3](4)(a3Tag),
      ScalaFunctions.schemaForUdfColumn[A4](5)(a4Tag),
      ScalaFunctions.schemaForUdfColumn[A5](6)(a5Tag),
      ScalaFunctions.schemaForUdfColumn[A6](7)(a6Tag),
      ScalaFunctions.schemaForUdfColumn[A7](8)(a7Tag),
      ScalaFunctions.schemaForUdfColumn[A8](9)(a8Tag),
      ScalaFunctions.schemaForUdfColumn[A9](10)(a9Tag))
}

/**
 * The Scala UDAF (user-defined aggregate function) abstract class that has 11 arguments.
 *
 * @tparam S
 *   The type of the aggregation state.
 * @tparam O
 *   The type of the output value.
 * @since 1.19.0
 */
abstract class UDAF11[S, O, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10](implicit
    @transient private val a0Tag: TypeTag[A0],
    @transient private val a1Tag: TypeTag[A1],
    @transient private val a2Tag: TypeTag[A2],
    @transient private val a3Tag: TypeTag[A3],
    @transient private val a4Tag: TypeTag[A4],
    @transient private val a5Tag: TypeTag[A5],
    @transient private val a6Tag: TypeTag[A6],
    @transient private val a7Tag: TypeTag[A7],
    @transient private val a8Tag: TypeTag[A8],
    @transient private val a9Tag: TypeTag[A9],
    @transient private val a10Tag: TypeTag[A10])
    extends UDAF {

  /**
   * Returns the initial state of the aggregation.
   *
   * Since: 1.19.0
   */
  def initialize(): S

  /**
   * Updates the aggregation state with a new input row.
   *
   * @param state
   *   The current aggregation state.
   *
   * Since: 1.19.0
   */
  // scalastyle:off
  def accumulate(
      state: S,
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
      arg10: A10): S
  // scalastyle:on

  /**
   * Merges two aggregation states into one.
   *
   * @param state1
   *   The first aggregation state.
   * @param state2
   *   The second aggregation state.
   * @return
   *   The merged aggregation state.
   *
   * Since: 1.19.0
   */
  def merge(state1: S, state2: S): S

  /**
   * Produces the final output value from the aggregation state.
   *
   * @param state
   *   The final aggregation state.
   * @return
   *   The output value.
   *
   * Since: 1.19.0
   */
  def terminate(state: S): O

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1)(a0Tag),
      ScalaFunctions.schemaForUdfColumn[A1](2)(a1Tag),
      ScalaFunctions.schemaForUdfColumn[A2](3)(a2Tag),
      ScalaFunctions.schemaForUdfColumn[A3](4)(a3Tag),
      ScalaFunctions.schemaForUdfColumn[A4](5)(a4Tag),
      ScalaFunctions.schemaForUdfColumn[A5](6)(a5Tag),
      ScalaFunctions.schemaForUdfColumn[A6](7)(a6Tag),
      ScalaFunctions.schemaForUdfColumn[A7](8)(a7Tag),
      ScalaFunctions.schemaForUdfColumn[A8](9)(a8Tag),
      ScalaFunctions.schemaForUdfColumn[A9](10)(a9Tag),
      ScalaFunctions.schemaForUdfColumn[A10](11)(a10Tag))
}

/**
 * The Scala UDAF (user-defined aggregate function) abstract class that has 12 arguments.
 *
 * @tparam S
 *   The type of the aggregation state.
 * @tparam O
 *   The type of the output value.
 * @since 1.19.0
 */
abstract class UDAF12[S, O, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11](implicit
    @transient private val a0Tag: TypeTag[A0],
    @transient private val a1Tag: TypeTag[A1],
    @transient private val a2Tag: TypeTag[A2],
    @transient private val a3Tag: TypeTag[A3],
    @transient private val a4Tag: TypeTag[A4],
    @transient private val a5Tag: TypeTag[A5],
    @transient private val a6Tag: TypeTag[A6],
    @transient private val a7Tag: TypeTag[A7],
    @transient private val a8Tag: TypeTag[A8],
    @transient private val a9Tag: TypeTag[A9],
    @transient private val a10Tag: TypeTag[A10],
    @transient private val a11Tag: TypeTag[A11])
    extends UDAF {

  /**
   * Returns the initial state of the aggregation.
   *
   * Since: 1.19.0
   */
  def initialize(): S

  /**
   * Updates the aggregation state with a new input row.
   *
   * @param state
   *   The current aggregation state.
   *
   * Since: 1.19.0
   */
  // scalastyle:off
  def accumulate(
      state: S,
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
      arg11: A11): S
  // scalastyle:on

  /**
   * Merges two aggregation states into one.
   *
   * @param state1
   *   The first aggregation state.
   * @param state2
   *   The second aggregation state.
   * @return
   *   The merged aggregation state.
   *
   * Since: 1.19.0
   */
  def merge(state1: S, state2: S): S

  /**
   * Produces the final output value from the aggregation state.
   *
   * @param state
   *   The final aggregation state.
   * @return
   *   The output value.
   *
   * Since: 1.19.0
   */
  def terminate(state: S): O

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1)(a0Tag),
      ScalaFunctions.schemaForUdfColumn[A1](2)(a1Tag),
      ScalaFunctions.schemaForUdfColumn[A2](3)(a2Tag),
      ScalaFunctions.schemaForUdfColumn[A3](4)(a3Tag),
      ScalaFunctions.schemaForUdfColumn[A4](5)(a4Tag),
      ScalaFunctions.schemaForUdfColumn[A5](6)(a5Tag),
      ScalaFunctions.schemaForUdfColumn[A6](7)(a6Tag),
      ScalaFunctions.schemaForUdfColumn[A7](8)(a7Tag),
      ScalaFunctions.schemaForUdfColumn[A8](9)(a8Tag),
      ScalaFunctions.schemaForUdfColumn[A9](10)(a9Tag),
      ScalaFunctions.schemaForUdfColumn[A10](11)(a10Tag),
      ScalaFunctions.schemaForUdfColumn[A11](12)(a11Tag))
}

/**
 * The Scala UDAF (user-defined aggregate function) abstract class that has 13 arguments.
 *
 * @tparam S
 *   The type of the aggregation state.
 * @tparam O
 *   The type of the output value.
 * @since 1.19.0
 */
abstract class UDAF13[S, O, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12](implicit
    @transient private val a0Tag: TypeTag[A0],
    @transient private val a1Tag: TypeTag[A1],
    @transient private val a2Tag: TypeTag[A2],
    @transient private val a3Tag: TypeTag[A3],
    @transient private val a4Tag: TypeTag[A4],
    @transient private val a5Tag: TypeTag[A5],
    @transient private val a6Tag: TypeTag[A6],
    @transient private val a7Tag: TypeTag[A7],
    @transient private val a8Tag: TypeTag[A8],
    @transient private val a9Tag: TypeTag[A9],
    @transient private val a10Tag: TypeTag[A10],
    @transient private val a11Tag: TypeTag[A11],
    @transient private val a12Tag: TypeTag[A12])
    extends UDAF {

  /**
   * Returns the initial state of the aggregation.
   *
   * Since: 1.19.0
   */
  def initialize(): S

  /**
   * Updates the aggregation state with a new input row.
   *
   * @param state
   *   The current aggregation state.
   *
   * Since: 1.19.0
   */
  // scalastyle:off
  def accumulate(
      state: S,
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
      arg12: A12): S
  // scalastyle:on

  /**
   * Merges two aggregation states into one.
   *
   * @param state1
   *   The first aggregation state.
   * @param state2
   *   The second aggregation state.
   * @return
   *   The merged aggregation state.
   *
   * Since: 1.19.0
   */
  def merge(state1: S, state2: S): S

  /**
   * Produces the final output value from the aggregation state.
   *
   * @param state
   *   The final aggregation state.
   * @return
   *   The output value.
   *
   * Since: 1.19.0
   */
  def terminate(state: S): O

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1)(a0Tag),
      ScalaFunctions.schemaForUdfColumn[A1](2)(a1Tag),
      ScalaFunctions.schemaForUdfColumn[A2](3)(a2Tag),
      ScalaFunctions.schemaForUdfColumn[A3](4)(a3Tag),
      ScalaFunctions.schemaForUdfColumn[A4](5)(a4Tag),
      ScalaFunctions.schemaForUdfColumn[A5](6)(a5Tag),
      ScalaFunctions.schemaForUdfColumn[A6](7)(a6Tag),
      ScalaFunctions.schemaForUdfColumn[A7](8)(a7Tag),
      ScalaFunctions.schemaForUdfColumn[A8](9)(a8Tag),
      ScalaFunctions.schemaForUdfColumn[A9](10)(a9Tag),
      ScalaFunctions.schemaForUdfColumn[A10](11)(a10Tag),
      ScalaFunctions.schemaForUdfColumn[A11](12)(a11Tag),
      ScalaFunctions.schemaForUdfColumn[A12](13)(a12Tag))
}

/**
 * The Scala UDAF (user-defined aggregate function) abstract class that has 14 arguments.
 *
 * @tparam S
 *   The type of the aggregation state.
 * @tparam O
 *   The type of the output value.
 * @since 1.19.0
 */
abstract class UDAF14[S, O, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13](implicit
    @transient private val a0Tag: TypeTag[A0],
    @transient private val a1Tag: TypeTag[A1],
    @transient private val a2Tag: TypeTag[A2],
    @transient private val a3Tag: TypeTag[A3],
    @transient private val a4Tag: TypeTag[A4],
    @transient private val a5Tag: TypeTag[A5],
    @transient private val a6Tag: TypeTag[A6],
    @transient private val a7Tag: TypeTag[A7],
    @transient private val a8Tag: TypeTag[A8],
    @transient private val a9Tag: TypeTag[A9],
    @transient private val a10Tag: TypeTag[A10],
    @transient private val a11Tag: TypeTag[A11],
    @transient private val a12Tag: TypeTag[A12],
    @transient private val a13Tag: TypeTag[A13])
    extends UDAF {

  /**
   * Returns the initial state of the aggregation.
   *
   * Since: 1.19.0
   */
  def initialize(): S

  /**
   * Updates the aggregation state with a new input row.
   *
   * @param state
   *   The current aggregation state.
   *
   * Since: 1.19.0
   */
  // scalastyle:off
  def accumulate(
      state: S,
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
      arg13: A13): S
  // scalastyle:on

  /**
   * Merges two aggregation states into one.
   *
   * @param state1
   *   The first aggregation state.
   * @param state2
   *   The second aggregation state.
   * @return
   *   The merged aggregation state.
   *
   * Since: 1.19.0
   */
  def merge(state1: S, state2: S): S

  /**
   * Produces the final output value from the aggregation state.
   *
   * @param state
   *   The final aggregation state.
   * @return
   *   The output value.
   *
   * Since: 1.19.0
   */
  def terminate(state: S): O

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1)(a0Tag),
      ScalaFunctions.schemaForUdfColumn[A1](2)(a1Tag),
      ScalaFunctions.schemaForUdfColumn[A2](3)(a2Tag),
      ScalaFunctions.schemaForUdfColumn[A3](4)(a3Tag),
      ScalaFunctions.schemaForUdfColumn[A4](5)(a4Tag),
      ScalaFunctions.schemaForUdfColumn[A5](6)(a5Tag),
      ScalaFunctions.schemaForUdfColumn[A6](7)(a6Tag),
      ScalaFunctions.schemaForUdfColumn[A7](8)(a7Tag),
      ScalaFunctions.schemaForUdfColumn[A8](9)(a8Tag),
      ScalaFunctions.schemaForUdfColumn[A9](10)(a9Tag),
      ScalaFunctions.schemaForUdfColumn[A10](11)(a10Tag),
      ScalaFunctions.schemaForUdfColumn[A11](12)(a11Tag),
      ScalaFunctions.schemaForUdfColumn[A12](13)(a12Tag),
      ScalaFunctions.schemaForUdfColumn[A13](14)(a13Tag))
}

/**
 * The Scala UDAF (user-defined aggregate function) abstract class that has 15 arguments.
 *
 * @tparam S
 *   The type of the aggregation state.
 * @tparam O
 *   The type of the output value.
 * @since 1.19.0
 */
abstract class UDAF15[S, O, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14](
    implicit
    @transient private val a0Tag: TypeTag[A0],
    @transient private val a1Tag: TypeTag[A1],
    @transient private val a2Tag: TypeTag[A2],
    @transient private val a3Tag: TypeTag[A3],
    @transient private val a4Tag: TypeTag[A4],
    @transient private val a5Tag: TypeTag[A5],
    @transient private val a6Tag: TypeTag[A6],
    @transient private val a7Tag: TypeTag[A7],
    @transient private val a8Tag: TypeTag[A8],
    @transient private val a9Tag: TypeTag[A9],
    @transient private val a10Tag: TypeTag[A10],
    @transient private val a11Tag: TypeTag[A11],
    @transient private val a12Tag: TypeTag[A12],
    @transient private val a13Tag: TypeTag[A13],
    @transient private val a14Tag: TypeTag[A14])
    extends UDAF {

  /**
   * Returns the initial state of the aggregation.
   *
   * Since: 1.19.0
   */
  def initialize(): S

  /**
   * Updates the aggregation state with a new input row.
   *
   * @param state
   *   The current aggregation state.
   *
   * Since: 1.19.0
   */
  // scalastyle:off
  def accumulate(
      state: S,
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
      arg14: A14): S
  // scalastyle:on

  /**
   * Merges two aggregation states into one.
   *
   * @param state1
   *   The first aggregation state.
   * @param state2
   *   The second aggregation state.
   * @return
   *   The merged aggregation state.
   *
   * Since: 1.19.0
   */
  def merge(state1: S, state2: S): S

  /**
   * Produces the final output value from the aggregation state.
   *
   * @param state
   *   The final aggregation state.
   * @return
   *   The output value.
   *
   * Since: 1.19.0
   */
  def terminate(state: S): O

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1)(a0Tag),
      ScalaFunctions.schemaForUdfColumn[A1](2)(a1Tag),
      ScalaFunctions.schemaForUdfColumn[A2](3)(a2Tag),
      ScalaFunctions.schemaForUdfColumn[A3](4)(a3Tag),
      ScalaFunctions.schemaForUdfColumn[A4](5)(a4Tag),
      ScalaFunctions.schemaForUdfColumn[A5](6)(a5Tag),
      ScalaFunctions.schemaForUdfColumn[A6](7)(a6Tag),
      ScalaFunctions.schemaForUdfColumn[A7](8)(a7Tag),
      ScalaFunctions.schemaForUdfColumn[A8](9)(a8Tag),
      ScalaFunctions.schemaForUdfColumn[A9](10)(a9Tag),
      ScalaFunctions.schemaForUdfColumn[A10](11)(a10Tag),
      ScalaFunctions.schemaForUdfColumn[A11](12)(a11Tag),
      ScalaFunctions.schemaForUdfColumn[A12](13)(a12Tag),
      ScalaFunctions.schemaForUdfColumn[A13](14)(a13Tag),
      ScalaFunctions.schemaForUdfColumn[A14](15)(a14Tag))
}

/**
 * The Scala UDAF (user-defined aggregate function) abstract class that has 16 arguments.
 *
 * @tparam S
 *   The type of the aggregation state.
 * @tparam O
 *   The type of the output value.
 * @since 1.19.0
 */
abstract class UDAF16[S, O, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15](
    implicit
    @transient private val a0Tag: TypeTag[A0],
    @transient private val a1Tag: TypeTag[A1],
    @transient private val a2Tag: TypeTag[A2],
    @transient private val a3Tag: TypeTag[A3],
    @transient private val a4Tag: TypeTag[A4],
    @transient private val a5Tag: TypeTag[A5],
    @transient private val a6Tag: TypeTag[A6],
    @transient private val a7Tag: TypeTag[A7],
    @transient private val a8Tag: TypeTag[A8],
    @transient private val a9Tag: TypeTag[A9],
    @transient private val a10Tag: TypeTag[A10],
    @transient private val a11Tag: TypeTag[A11],
    @transient private val a12Tag: TypeTag[A12],
    @transient private val a13Tag: TypeTag[A13],
    @transient private val a14Tag: TypeTag[A14],
    @transient private val a15Tag: TypeTag[A15])
    extends UDAF {

  /**
   * Returns the initial state of the aggregation.
   *
   * Since: 1.19.0
   */
  def initialize(): S

  /**
   * Updates the aggregation state with a new input row.
   *
   * @param state
   *   The current aggregation state.
   *
   * Since: 1.19.0
   */
  // scalastyle:off
  def accumulate(
      state: S,
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
      arg15: A15): S
  // scalastyle:on

  /**
   * Merges two aggregation states into one.
   *
   * @param state1
   *   The first aggregation state.
   * @param state2
   *   The second aggregation state.
   * @return
   *   The merged aggregation state.
   *
   * Since: 1.19.0
   */
  def merge(state1: S, state2: S): S

  /**
   * Produces the final output value from the aggregation state.
   *
   * @param state
   *   The final aggregation state.
   * @return
   *   The output value.
   *
   * Since: 1.19.0
   */
  def terminate(state: S): O

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1)(a0Tag),
      ScalaFunctions.schemaForUdfColumn[A1](2)(a1Tag),
      ScalaFunctions.schemaForUdfColumn[A2](3)(a2Tag),
      ScalaFunctions.schemaForUdfColumn[A3](4)(a3Tag),
      ScalaFunctions.schemaForUdfColumn[A4](5)(a4Tag),
      ScalaFunctions.schemaForUdfColumn[A5](6)(a5Tag),
      ScalaFunctions.schemaForUdfColumn[A6](7)(a6Tag),
      ScalaFunctions.schemaForUdfColumn[A7](8)(a7Tag),
      ScalaFunctions.schemaForUdfColumn[A8](9)(a8Tag),
      ScalaFunctions.schemaForUdfColumn[A9](10)(a9Tag),
      ScalaFunctions.schemaForUdfColumn[A10](11)(a10Tag),
      ScalaFunctions.schemaForUdfColumn[A11](12)(a11Tag),
      ScalaFunctions.schemaForUdfColumn[A12](13)(a12Tag),
      ScalaFunctions.schemaForUdfColumn[A13](14)(a13Tag),
      ScalaFunctions.schemaForUdfColumn[A14](15)(a14Tag),
      ScalaFunctions.schemaForUdfColumn[A15](16)(a15Tag))
}

/**
 * The Scala UDAF (user-defined aggregate function) abstract class that has 17 arguments.
 *
 * @tparam S
 *   The type of the aggregation state.
 * @tparam O
 *   The type of the output value.
 * @since 1.19.0
 */
abstract class UDAF17[
    S,
    O,
    A0,
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
    A16](implicit
    @transient private val a0Tag: TypeTag[A0],
    @transient private val a1Tag: TypeTag[A1],
    @transient private val a2Tag: TypeTag[A2],
    @transient private val a3Tag: TypeTag[A3],
    @transient private val a4Tag: TypeTag[A4],
    @transient private val a5Tag: TypeTag[A5],
    @transient private val a6Tag: TypeTag[A6],
    @transient private val a7Tag: TypeTag[A7],
    @transient private val a8Tag: TypeTag[A8],
    @transient private val a9Tag: TypeTag[A9],
    @transient private val a10Tag: TypeTag[A10],
    @transient private val a11Tag: TypeTag[A11],
    @transient private val a12Tag: TypeTag[A12],
    @transient private val a13Tag: TypeTag[A13],
    @transient private val a14Tag: TypeTag[A14],
    @transient private val a15Tag: TypeTag[A15],
    @transient private val a16Tag: TypeTag[A16])
    extends UDAF {

  /**
   * Returns the initial state of the aggregation.
   *
   * Since: 1.19.0
   */
  def initialize(): S

  /**
   * Updates the aggregation state with a new input row.
   *
   * @param state
   *   The current aggregation state.
   *
   * Since: 1.19.0
   */
  // scalastyle:off
  def accumulate(
      state: S,
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
      arg16: A16): S
  // scalastyle:on

  /**
   * Merges two aggregation states into one.
   *
   * @param state1
   *   The first aggregation state.
   * @param state2
   *   The second aggregation state.
   * @return
   *   The merged aggregation state.
   *
   * Since: 1.19.0
   */
  def merge(state1: S, state2: S): S

  /**
   * Produces the final output value from the aggregation state.
   *
   * @param state
   *   The final aggregation state.
   * @return
   *   The output value.
   *
   * Since: 1.19.0
   */
  def terminate(state: S): O

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1)(a0Tag),
      ScalaFunctions.schemaForUdfColumn[A1](2)(a1Tag),
      ScalaFunctions.schemaForUdfColumn[A2](3)(a2Tag),
      ScalaFunctions.schemaForUdfColumn[A3](4)(a3Tag),
      ScalaFunctions.schemaForUdfColumn[A4](5)(a4Tag),
      ScalaFunctions.schemaForUdfColumn[A5](6)(a5Tag),
      ScalaFunctions.schemaForUdfColumn[A6](7)(a6Tag),
      ScalaFunctions.schemaForUdfColumn[A7](8)(a7Tag),
      ScalaFunctions.schemaForUdfColumn[A8](9)(a8Tag),
      ScalaFunctions.schemaForUdfColumn[A9](10)(a9Tag),
      ScalaFunctions.schemaForUdfColumn[A10](11)(a10Tag),
      ScalaFunctions.schemaForUdfColumn[A11](12)(a11Tag),
      ScalaFunctions.schemaForUdfColumn[A12](13)(a12Tag),
      ScalaFunctions.schemaForUdfColumn[A13](14)(a13Tag),
      ScalaFunctions.schemaForUdfColumn[A14](15)(a14Tag),
      ScalaFunctions.schemaForUdfColumn[A15](16)(a15Tag),
      ScalaFunctions.schemaForUdfColumn[A16](17)(a16Tag))
}

/**
 * The Scala UDAF (user-defined aggregate function) abstract class that has 18 arguments.
 *
 * @tparam S
 *   The type of the aggregation state.
 * @tparam O
 *   The type of the output value.
 * @since 1.19.0
 */
abstract class UDAF18[
    S,
    O,
    A0,
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
    A17](implicit
    @transient private val a0Tag: TypeTag[A0],
    @transient private val a1Tag: TypeTag[A1],
    @transient private val a2Tag: TypeTag[A2],
    @transient private val a3Tag: TypeTag[A3],
    @transient private val a4Tag: TypeTag[A4],
    @transient private val a5Tag: TypeTag[A5],
    @transient private val a6Tag: TypeTag[A6],
    @transient private val a7Tag: TypeTag[A7],
    @transient private val a8Tag: TypeTag[A8],
    @transient private val a9Tag: TypeTag[A9],
    @transient private val a10Tag: TypeTag[A10],
    @transient private val a11Tag: TypeTag[A11],
    @transient private val a12Tag: TypeTag[A12],
    @transient private val a13Tag: TypeTag[A13],
    @transient private val a14Tag: TypeTag[A14],
    @transient private val a15Tag: TypeTag[A15],
    @transient private val a16Tag: TypeTag[A16],
    @transient private val a17Tag: TypeTag[A17])
    extends UDAF {

  /**
   * Returns the initial state of the aggregation.
   *
   * Since: 1.19.0
   */
  def initialize(): S

  /**
   * Updates the aggregation state with a new input row.
   *
   * @param state
   *   The current aggregation state.
   *
   * Since: 1.19.0
   */
  // scalastyle:off
  def accumulate(
      state: S,
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
      arg17: A17): S
  // scalastyle:on

  /**
   * Merges two aggregation states into one.
   *
   * @param state1
   *   The first aggregation state.
   * @param state2
   *   The second aggregation state.
   * @return
   *   The merged aggregation state.
   *
   * Since: 1.19.0
   */
  def merge(state1: S, state2: S): S

  /**
   * Produces the final output value from the aggregation state.
   *
   * @param state
   *   The final aggregation state.
   * @return
   *   The output value.
   *
   * Since: 1.19.0
   */
  def terminate(state: S): O

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1)(a0Tag),
      ScalaFunctions.schemaForUdfColumn[A1](2)(a1Tag),
      ScalaFunctions.schemaForUdfColumn[A2](3)(a2Tag),
      ScalaFunctions.schemaForUdfColumn[A3](4)(a3Tag),
      ScalaFunctions.schemaForUdfColumn[A4](5)(a4Tag),
      ScalaFunctions.schemaForUdfColumn[A5](6)(a5Tag),
      ScalaFunctions.schemaForUdfColumn[A6](7)(a6Tag),
      ScalaFunctions.schemaForUdfColumn[A7](8)(a7Tag),
      ScalaFunctions.schemaForUdfColumn[A8](9)(a8Tag),
      ScalaFunctions.schemaForUdfColumn[A9](10)(a9Tag),
      ScalaFunctions.schemaForUdfColumn[A10](11)(a10Tag),
      ScalaFunctions.schemaForUdfColumn[A11](12)(a11Tag),
      ScalaFunctions.schemaForUdfColumn[A12](13)(a12Tag),
      ScalaFunctions.schemaForUdfColumn[A13](14)(a13Tag),
      ScalaFunctions.schemaForUdfColumn[A14](15)(a14Tag),
      ScalaFunctions.schemaForUdfColumn[A15](16)(a15Tag),
      ScalaFunctions.schemaForUdfColumn[A16](17)(a16Tag),
      ScalaFunctions.schemaForUdfColumn[A17](18)(a17Tag))
}

/**
 * The Scala UDAF (user-defined aggregate function) abstract class that has 19 arguments.
 *
 * @tparam S
 *   The type of the aggregation state.
 * @tparam O
 *   The type of the output value.
 * @since 1.19.0
 */
abstract class UDAF19[
    S,
    O,
    A0,
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
    A18](implicit
    @transient private val a0Tag: TypeTag[A0],
    @transient private val a1Tag: TypeTag[A1],
    @transient private val a2Tag: TypeTag[A2],
    @transient private val a3Tag: TypeTag[A3],
    @transient private val a4Tag: TypeTag[A4],
    @transient private val a5Tag: TypeTag[A5],
    @transient private val a6Tag: TypeTag[A6],
    @transient private val a7Tag: TypeTag[A7],
    @transient private val a8Tag: TypeTag[A8],
    @transient private val a9Tag: TypeTag[A9],
    @transient private val a10Tag: TypeTag[A10],
    @transient private val a11Tag: TypeTag[A11],
    @transient private val a12Tag: TypeTag[A12],
    @transient private val a13Tag: TypeTag[A13],
    @transient private val a14Tag: TypeTag[A14],
    @transient private val a15Tag: TypeTag[A15],
    @transient private val a16Tag: TypeTag[A16],
    @transient private val a17Tag: TypeTag[A17],
    @transient private val a18Tag: TypeTag[A18])
    extends UDAF {

  /**
   * Returns the initial state of the aggregation.
   *
   * Since: 1.19.0
   */
  def initialize(): S

  /**
   * Updates the aggregation state with a new input row.
   *
   * @param state
   *   The current aggregation state.
   *
   * Since: 1.19.0
   */
  // scalastyle:off
  def accumulate(
      state: S,
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
      arg18: A18): S
  // scalastyle:on

  /**
   * Merges two aggregation states into one.
   *
   * @param state1
   *   The first aggregation state.
   * @param state2
   *   The second aggregation state.
   * @return
   *   The merged aggregation state.
   *
   * Since: 1.19.0
   */
  def merge(state1: S, state2: S): S

  /**
   * Produces the final output value from the aggregation state.
   *
   * @param state
   *   The final aggregation state.
   * @return
   *   The output value.
   *
   * Since: 1.19.0
   */
  def terminate(state: S): O

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1)(a0Tag),
      ScalaFunctions.schemaForUdfColumn[A1](2)(a1Tag),
      ScalaFunctions.schemaForUdfColumn[A2](3)(a2Tag),
      ScalaFunctions.schemaForUdfColumn[A3](4)(a3Tag),
      ScalaFunctions.schemaForUdfColumn[A4](5)(a4Tag),
      ScalaFunctions.schemaForUdfColumn[A5](6)(a5Tag),
      ScalaFunctions.schemaForUdfColumn[A6](7)(a6Tag),
      ScalaFunctions.schemaForUdfColumn[A7](8)(a7Tag),
      ScalaFunctions.schemaForUdfColumn[A8](9)(a8Tag),
      ScalaFunctions.schemaForUdfColumn[A9](10)(a9Tag),
      ScalaFunctions.schemaForUdfColumn[A10](11)(a10Tag),
      ScalaFunctions.schemaForUdfColumn[A11](12)(a11Tag),
      ScalaFunctions.schemaForUdfColumn[A12](13)(a12Tag),
      ScalaFunctions.schemaForUdfColumn[A13](14)(a13Tag),
      ScalaFunctions.schemaForUdfColumn[A14](15)(a14Tag),
      ScalaFunctions.schemaForUdfColumn[A15](16)(a15Tag),
      ScalaFunctions.schemaForUdfColumn[A16](17)(a16Tag),
      ScalaFunctions.schemaForUdfColumn[A17](18)(a17Tag),
      ScalaFunctions.schemaForUdfColumn[A18](19)(a18Tag))
}

/**
 * The Scala UDAF (user-defined aggregate function) abstract class that has 20 arguments.
 *
 * @tparam S
 *   The type of the aggregation state.
 * @tparam O
 *   The type of the output value.
 * @since 1.19.0
 */
abstract class UDAF20[
    S,
    O,
    A0,
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
    A19](implicit
    @transient private val a0Tag: TypeTag[A0],
    @transient private val a1Tag: TypeTag[A1],
    @transient private val a2Tag: TypeTag[A2],
    @transient private val a3Tag: TypeTag[A3],
    @transient private val a4Tag: TypeTag[A4],
    @transient private val a5Tag: TypeTag[A5],
    @transient private val a6Tag: TypeTag[A6],
    @transient private val a7Tag: TypeTag[A7],
    @transient private val a8Tag: TypeTag[A8],
    @transient private val a9Tag: TypeTag[A9],
    @transient private val a10Tag: TypeTag[A10],
    @transient private val a11Tag: TypeTag[A11],
    @transient private val a12Tag: TypeTag[A12],
    @transient private val a13Tag: TypeTag[A13],
    @transient private val a14Tag: TypeTag[A14],
    @transient private val a15Tag: TypeTag[A15],
    @transient private val a16Tag: TypeTag[A16],
    @transient private val a17Tag: TypeTag[A17],
    @transient private val a18Tag: TypeTag[A18],
    @transient private val a19Tag: TypeTag[A19])
    extends UDAF {

  /**
   * Returns the initial state of the aggregation.
   *
   * Since: 1.19.0
   */
  def initialize(): S

  /**
   * Updates the aggregation state with a new input row.
   *
   * @param state
   *   The current aggregation state.
   *
   * Since: 1.19.0
   */
  // scalastyle:off
  def accumulate(
      state: S,
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
      arg19: A19): S
  // scalastyle:on

  /**
   * Merges two aggregation states into one.
   *
   * @param state1
   *   The first aggregation state.
   * @param state2
   *   The second aggregation state.
   * @return
   *   The merged aggregation state.
   *
   * Since: 1.19.0
   */
  def merge(state1: S, state2: S): S

  /**
   * Produces the final output value from the aggregation state.
   *
   * @param state
   *   The final aggregation state.
   * @return
   *   The output value.
   *
   * Since: 1.19.0
   */
  def terminate(state: S): O

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1)(a0Tag),
      ScalaFunctions.schemaForUdfColumn[A1](2)(a1Tag),
      ScalaFunctions.schemaForUdfColumn[A2](3)(a2Tag),
      ScalaFunctions.schemaForUdfColumn[A3](4)(a3Tag),
      ScalaFunctions.schemaForUdfColumn[A4](5)(a4Tag),
      ScalaFunctions.schemaForUdfColumn[A5](6)(a5Tag),
      ScalaFunctions.schemaForUdfColumn[A6](7)(a6Tag),
      ScalaFunctions.schemaForUdfColumn[A7](8)(a7Tag),
      ScalaFunctions.schemaForUdfColumn[A8](9)(a8Tag),
      ScalaFunctions.schemaForUdfColumn[A9](10)(a9Tag),
      ScalaFunctions.schemaForUdfColumn[A10](11)(a10Tag),
      ScalaFunctions.schemaForUdfColumn[A11](12)(a11Tag),
      ScalaFunctions.schemaForUdfColumn[A12](13)(a12Tag),
      ScalaFunctions.schemaForUdfColumn[A13](14)(a13Tag),
      ScalaFunctions.schemaForUdfColumn[A14](15)(a14Tag),
      ScalaFunctions.schemaForUdfColumn[A15](16)(a15Tag),
      ScalaFunctions.schemaForUdfColumn[A16](17)(a16Tag),
      ScalaFunctions.schemaForUdfColumn[A17](18)(a17Tag),
      ScalaFunctions.schemaForUdfColumn[A18](19)(a18Tag),
      ScalaFunctions.schemaForUdfColumn[A19](20)(a19Tag))
}

/**
 * The Scala UDAF (user-defined aggregate function) abstract class that has 21 arguments.
 *
 * @tparam S
 *   The type of the aggregation state.
 * @tparam O
 *   The type of the output value.
 * @since 1.19.0
 */
abstract class UDAF21[
    S,
    O,
    A0,
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
    A20](implicit
    @transient private val a0Tag: TypeTag[A0],
    @transient private val a1Tag: TypeTag[A1],
    @transient private val a2Tag: TypeTag[A2],
    @transient private val a3Tag: TypeTag[A3],
    @transient private val a4Tag: TypeTag[A4],
    @transient private val a5Tag: TypeTag[A5],
    @transient private val a6Tag: TypeTag[A6],
    @transient private val a7Tag: TypeTag[A7],
    @transient private val a8Tag: TypeTag[A8],
    @transient private val a9Tag: TypeTag[A9],
    @transient private val a10Tag: TypeTag[A10],
    @transient private val a11Tag: TypeTag[A11],
    @transient private val a12Tag: TypeTag[A12],
    @transient private val a13Tag: TypeTag[A13],
    @transient private val a14Tag: TypeTag[A14],
    @transient private val a15Tag: TypeTag[A15],
    @transient private val a16Tag: TypeTag[A16],
    @transient private val a17Tag: TypeTag[A17],
    @transient private val a18Tag: TypeTag[A18],
    @transient private val a19Tag: TypeTag[A19],
    @transient private val a20Tag: TypeTag[A20])
    extends UDAF {

  /**
   * Returns the initial state of the aggregation.
   *
   * Since: 1.19.0
   */
  def initialize(): S

  /**
   * Updates the aggregation state with a new input row.
   *
   * @param state
   *   The current aggregation state.
   *
   * Since: 1.19.0
   */
  // scalastyle:off
  def accumulate(
      state: S,
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
      arg20: A20): S
  // scalastyle:on

  /**
   * Merges two aggregation states into one.
   *
   * @param state1
   *   The first aggregation state.
   * @param state2
   *   The second aggregation state.
   * @return
   *   The merged aggregation state.
   *
   * Since: 1.19.0
   */
  def merge(state1: S, state2: S): S

  /**
   * Produces the final output value from the aggregation state.
   *
   * @param state
   *   The final aggregation state.
   * @return
   *   The output value.
   *
   * Since: 1.19.0
   */
  def terminate(state: S): O

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1)(a0Tag),
      ScalaFunctions.schemaForUdfColumn[A1](2)(a1Tag),
      ScalaFunctions.schemaForUdfColumn[A2](3)(a2Tag),
      ScalaFunctions.schemaForUdfColumn[A3](4)(a3Tag),
      ScalaFunctions.schemaForUdfColumn[A4](5)(a4Tag),
      ScalaFunctions.schemaForUdfColumn[A5](6)(a5Tag),
      ScalaFunctions.schemaForUdfColumn[A6](7)(a6Tag),
      ScalaFunctions.schemaForUdfColumn[A7](8)(a7Tag),
      ScalaFunctions.schemaForUdfColumn[A8](9)(a8Tag),
      ScalaFunctions.schemaForUdfColumn[A9](10)(a9Tag),
      ScalaFunctions.schemaForUdfColumn[A10](11)(a10Tag),
      ScalaFunctions.schemaForUdfColumn[A11](12)(a11Tag),
      ScalaFunctions.schemaForUdfColumn[A12](13)(a12Tag),
      ScalaFunctions.schemaForUdfColumn[A13](14)(a13Tag),
      ScalaFunctions.schemaForUdfColumn[A14](15)(a14Tag),
      ScalaFunctions.schemaForUdfColumn[A15](16)(a15Tag),
      ScalaFunctions.schemaForUdfColumn[A16](17)(a16Tag),
      ScalaFunctions.schemaForUdfColumn[A17](18)(a17Tag),
      ScalaFunctions.schemaForUdfColumn[A18](19)(a18Tag),
      ScalaFunctions.schemaForUdfColumn[A19](20)(a19Tag),
      ScalaFunctions.schemaForUdfColumn[A20](21)(a20Tag))
}

/**
 * The Scala UDAF (user-defined aggregate function) abstract class that has 22 arguments.
 *
 * @tparam S
 *   The type of the aggregation state.
 * @tparam O
 *   The type of the output value.
 * @since 1.19.0
 */
abstract class UDAF22[
    S,
    O,
    A0,
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
    A21](implicit
    @transient private val a0Tag: TypeTag[A0],
    @transient private val a1Tag: TypeTag[A1],
    @transient private val a2Tag: TypeTag[A2],
    @transient private val a3Tag: TypeTag[A3],
    @transient private val a4Tag: TypeTag[A4],
    @transient private val a5Tag: TypeTag[A5],
    @transient private val a6Tag: TypeTag[A6],
    @transient private val a7Tag: TypeTag[A7],
    @transient private val a8Tag: TypeTag[A8],
    @transient private val a9Tag: TypeTag[A9],
    @transient private val a10Tag: TypeTag[A10],
    @transient private val a11Tag: TypeTag[A11],
    @transient private val a12Tag: TypeTag[A12],
    @transient private val a13Tag: TypeTag[A13],
    @transient private val a14Tag: TypeTag[A14],
    @transient private val a15Tag: TypeTag[A15],
    @transient private val a16Tag: TypeTag[A16],
    @transient private val a17Tag: TypeTag[A17],
    @transient private val a18Tag: TypeTag[A18],
    @transient private val a19Tag: TypeTag[A19],
    @transient private val a20Tag: TypeTag[A20],
    @transient private val a21Tag: TypeTag[A21])
    extends UDAF {

  /**
   * Returns the initial state of the aggregation.
   *
   * Since: 1.19.0
   */
  def initialize(): S

  /**
   * Updates the aggregation state with a new input row.
   *
   * @param state
   *   The current aggregation state.
   *
   * Since: 1.19.0
   */
  // scalastyle:off
  def accumulate(
      state: S,
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
      arg21: A21): S
  // scalastyle:on

  /**
   * Merges two aggregation states into one.
   *
   * @param state1
   *   The first aggregation state.
   * @param state2
   *   The second aggregation state.
   * @return
   *   The merged aggregation state.
   *
   * Since: 1.19.0
   */
  def merge(state1: S, state2: S): S

  /**
   * Produces the final output value from the aggregation state.
   *
   * @param state
   *   The final aggregation state.
   * @return
   *   The output value.
   *
   * Since: 1.19.0
   */
  def terminate(state: S): O

  override private[snowpark] def inputColumns: Seq[UdfColumn] =
    Seq(
      ScalaFunctions.schemaForUdfColumn[A0](1)(a0Tag),
      ScalaFunctions.schemaForUdfColumn[A1](2)(a1Tag),
      ScalaFunctions.schemaForUdfColumn[A2](3)(a2Tag),
      ScalaFunctions.schemaForUdfColumn[A3](4)(a3Tag),
      ScalaFunctions.schemaForUdfColumn[A4](5)(a4Tag),
      ScalaFunctions.schemaForUdfColumn[A5](6)(a5Tag),
      ScalaFunctions.schemaForUdfColumn[A6](7)(a6Tag),
      ScalaFunctions.schemaForUdfColumn[A7](8)(a7Tag),
      ScalaFunctions.schemaForUdfColumn[A8](9)(a8Tag),
      ScalaFunctions.schemaForUdfColumn[A9](10)(a9Tag),
      ScalaFunctions.schemaForUdfColumn[A10](11)(a10Tag),
      ScalaFunctions.schemaForUdfColumn[A11](12)(a11Tag),
      ScalaFunctions.schemaForUdfColumn[A12](13)(a12Tag),
      ScalaFunctions.schemaForUdfColumn[A13](14)(a13Tag),
      ScalaFunctions.schemaForUdfColumn[A14](15)(a14Tag),
      ScalaFunctions.schemaForUdfColumn[A15](16)(a15Tag),
      ScalaFunctions.schemaForUdfColumn[A16](17)(a16Tag),
      ScalaFunctions.schemaForUdfColumn[A17](18)(a17Tag),
      ScalaFunctions.schemaForUdfColumn[A18](19)(a18Tag),
      ScalaFunctions.schemaForUdfColumn[A19](20)(a19Tag),
      ScalaFunctions.schemaForUdfColumn[A20](21)(a20Tag),
      ScalaFunctions.schemaForUdfColumn[A21](22)(a21Tag))
}
