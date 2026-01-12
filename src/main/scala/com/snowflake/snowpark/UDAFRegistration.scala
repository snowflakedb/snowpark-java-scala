package com.snowflake.snowpark

import com.snowflake.snowpark.internal.{Logging, OpenTelemetry, UDXRegistrationHandler}
import com.snowflake.snowpark.udaf.UDAF
import com.snowflake.snowpark_java.udaf.JavaUDAF

// scalastyle:off
/**
 * Provides methods to register a UDAF (user-defined aggregate function) in the Snowflake database.
 *
 * [[Session.udaf]] returns an object of this class.
 *
 * To register a UDAF, you must:
 *
 *   1. Define a UDAF class.
 *   1. Create an instance of that class, and register that instance as a UDAF.
 *
 * The next sections describe these steps in more detail.
 *
 * =Defining the UDAF Class=
 *
 * Define a class that inherits from one of the `UDAF[N]` classes (e.g. `UDAF0`, `UDAF1`, etc.),
 * where ''n'' specifies the number of input arguments for your UDAF. For example, if your UDAF
 * passes in 2 input arguments, extend the `UDAF2` class.
 *
 * In your class, override the following methods:
 *   - `initialize()`, which returns the initial state of the aggregation.
 *   - `accumulate()`, which updates the aggregation state with a new input row.
 *   - `merge()`, which merges two aggregation states into one.
 *   - `terminate()`, which produces the final output value from the aggregation state.
 *   - `outputType()`, which returns a [[types.DataType]] object that describes the type of the
 *     returned value.
 *
 * ==Example of a UDAF Class==
 *
 * The following is an example of a UDAF class that computes the average of integers. The state is a
 * custom class that holds the sum and count.
 *
 * The UDAF passes in 1 argument, so the class extends `UDAF1`.
 *
 * {{{
 *    // State class must be Serializable with a no-arg constructor for Kryo deserialization
 *    class AvgState extends Serializable {
 *        var sum: Long = 0L
 *        var count: Long = 0L
 *    }
 *
 *    class MyAvgUDAF extends UDAF1[AvgState, Double, Int] {
 *        override def initialize(): AvgState = new AvgState()
 *        override def accumulate(state: AvgState, input: Int): AvgState = {
 *            state.sum += input
 *            state.count += 1
 *            state
 *        }
 *        override def merge(state1: AvgState, state2: AvgState): AvgState = {
 *            state1.sum += state2.sum
 *            state1.count += state2.count
 *            state1
 *        }
 *        override def terminate(state: AvgState): Double = {
 *            if (state.count == 0) 0.0 else state.sum.toDouble / state.count
 *        }
 *        override def outputType(): DataType = DoubleType
 *    }
 * }}}
 *
 * =Registering the UDAF=
 *
 * Next, create an instance of the new class, and register the class by calling one of the
 * [[UDAFRegistration]] methods. You can register a temporary or permanent UDAF by name.
 *
 * ==Registering a Temporary UDAF By Name==
 *
 * To register a temporary UDAF by name, call `registerTemporary`, passing in a name for the UDAF
 * and an instance of the UDAF class. For example:
 * {{{
 *    // Use the MyAvgUDAF defined in previous example.
 *    val myAvg = session.udaf.registerTemporary("my_avg", new MyAvgUDAF())
 *    df.select(myAvg(col("a"))).show()
 * }}}
 *
 * ==Registering a Permanent UDAF By Name==
 *
 * If you need to use the UDAF in subsequent sessions, register a permanent UDAF.
 *
 * When registering a permanent UDAF, you must specify a stage where the registration method will
 * upload the JAR files for the UDAF and its dependencies. For example:
 * {{{
 *    val myAvg = session.udaf.registerPermanent("my_avg", new MyAvgUDAF(), "@myStage")
 *    df.select(myAvg(col("a"))).show()
 * }}}
 *
 * @since 1.19.0
 */
// scalastyle:on
class UDAFRegistration(session: Session) extends Logging {
  private[snowpark] val handler = new UDXRegistrationHandler(session)

  /**
   * Registers a Scala UDAF instance as a temporary anonymous UDAF that is scoped to this session.
   *
   * @param udaf
   *   The Scala UDAF instance to be registered.
   * @return
   *   A UserDefinedFunction representing the UDAF.
   * @since 1.19.0
   */
  def registerTemporary(udaf: UDAF): UserDefinedFunction = udf("registerTemporary") {
    handler.registerScalaUDAF(None, udaf, None)
  }

  /**
   * Registers a Scala UDAF instance as a temporary UDAF.
   *
   * @param funcName
   *   The name of the UDAF.
   * @param udaf
   *   The Scala UDAF instance to be registered.
   * @return
   *   A UserDefinedFunction representing the UDAF.
   * @since 1.19.0
   */
  def registerTemporary(funcName: String, udaf: UDAF): UserDefinedFunction =
    udf("registerTemporary", execName = funcName) {
      handler.registerScalaUDAF(Some(funcName), udaf, None)
    }

  /**
   * Registers a Scala UDAF instance as a permanent UDAF.
   *
   * @param funcName
   *   The name of the UDAF.
   * @param udaf
   *   The Scala UDAF instance to be registered.
   * @param stageLocation
   *   The stage location to upload JARs.
   * @return
   *   A UserDefinedFunction representing the UDAF.
   * @since 1.19.0
   */
  def registerPermanent(funcName: String, udaf: UDAF, stageLocation: String): UserDefinedFunction =
    udf("registerPermanent", execName = funcName, execFilePath = stageLocation) {
      handler.registerScalaUDAF(Some(funcName), udaf, Some(stageLocation))
    }

  /**
   * Registers a Java UDAF instance as a temporary anonymous UDAF that is scoped to this session.
   *
   * @param udaf
   *   The Java UDAF instance.
   * @return
   *   A UserDefinedFunction representing the UDAF.
   * @since 1.19.0
   */
  def registerTemporary(udaf: JavaUDAF): UserDefinedFunction = udf("registerTemporary") {
    registerJavaUDAF(None, udaf, None)
  }

  /**
   * Registers a Java UDAF instance as a temporary UDAF.
   *
   * @param funcName
   *   The name of the UDAF.
   * @param udaf
   *   The Java UDAF instance.
   * @return
   *   A UserDefinedFunction representing the UDAF.
   * @since 1.19.0
   */
  def registerTemporary(funcName: String, udaf: JavaUDAF): UserDefinedFunction =
    udf("registerTemporary", execName = funcName) {
      registerJavaUDAF(Some(funcName), udaf, None)
    }

  /**
   * Registers a Java UDAF instance as a permanent UDAF.
   *
   * @param funcName
   *   The name of the UDAF.
   * @param udaf
   *   The Java UDAF instance.
   * @param stageLocation
   *   The stage location to upload JARs.
   * @return
   *   A UserDefinedFunction representing the UDAF.
   * @since 1.19.0
   */
  def registerPermanent(
      funcName: String,
      udaf: JavaUDAF,
      stageLocation: String): UserDefinedFunction =
    udf("registerPermanent", execName = funcName, execFilePath = stageLocation) {
      registerJavaUDAF(Some(funcName), udaf, Some(stageLocation))
    }

  private[snowpark] def registerJavaUDAF(
      name: Option[String],
      udaf: JavaUDAF,
      stageLocation: Option[String]): UserDefinedFunction =
    handler.registerJavaUDAF(name, udaf, stageLocation)

  @inline protected def udf(funcName: String, execName: String = "", execFilePath: String = "")(
      func: => UserDefinedFunction): UserDefinedFunction = {
    OpenTelemetry.udx(
      "UDAFRegistration",
      funcName,
      execName,
      UDXRegistrationHandler.udafClassName,
      execFilePath)(func)
  }
}
