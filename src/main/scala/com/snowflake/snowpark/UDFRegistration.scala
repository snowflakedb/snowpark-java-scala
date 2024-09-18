package com.snowflake.snowpark

import com.snowflake.snowpark.internal.ScalaFunctions._
import com.snowflake.snowpark.internal._
import scala.reflect.runtime.universe.TypeTag

// scalastyle:off
/** Provides methods to register lambdas and functions as UDFs in the Snowflake database.
  *
  * [[Session.udf]] returns an object of this class.
  *
  * You can use this object to register temporary UDFs that you plan to use in the current session:
  * {{{
  *   session.udf.registerTemporary("mydoubleudf", (x: Int) => x * x)
  *   session.sql(s"SELECT mydoubleudf(c) from T)
  * }}}
  *
  * You can also register permanent UDFs that you can use in subsequent sessions. When registering a
  * permanent UDF, you must specify a stage where the registration method will upload the JAR files
  * for the UDF and any dependencies.
  * {{{
  *   session.udf.registerPermanent("mydoubleudf", (x: Int) => x * x, "mystage")
  *   session.sql(s"SELECT mydoubleudf(c) from T)
  * }}}
  *
  * The methods that register a UDF return a [[UserDefinedFunction]] object, which you can use in
  * [[Column]] expressions.
  * {{{
  *   val myUdf = session.udf.registerTemporary("mydoubleudf", (x: Int) => x * x)
  *   session.table("T").select(myUdf(col("c")))
  * }}}
  *
  * If you do not need to refer to a UDF by name, use
  * [[com.snowflake.snowpark.functions.udf[RT](* com.snowflake.snowpark.functions.udf]] to create an
  * anonymous UDF instead.
  *
  * Snowflake supports the following data types for the parameters for a UDF:
  *
  * | SQL Type  | Scala Type                                  | Notes                                                                                                                       |
  * |:----------|:--------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------|
  * | NUMBER    | Short or Option[Short]                      | Supported                                                                                                                   |
  * | NUMBER    | Int or Option[Int]                          | Supported                                                                                                                   |
  * | NUMBER    | Long or Option[Long]                        | Supported                                                                                                                   |
  * | FLOAT     | Float or Option[Float]                      | Supported                                                                                                                   |
  * | DOUBLE    | Double or Option[Double]                    | Supported                                                                                                                   |
  * | NUMBER    | java.math.BigDecimal                        | Supported                                                                                                                   |
  * | VARCHAR   | String or java.lang.String                  | Supported                                                                                                                   |
  * | BOOL      | Boolean or Option[Boolean]                  | Supported                                                                                                                   |
  * | DATE      | java.sql.Date                               | Supported                                                                                                                   |
  * | TIMESTAMP | java.sql.Timestamp                          | Supported                                                                                                                   |
  * | BINARY    | Array[Byte]                                 | Supported                                                                                                                   |
  * | ARRAY     | Array[String] or Array[Variant]             | Supported array of type Array[String] or Array[Variant]                                                                     |
  * | OBJECT    | Map[String, String] or Map[String, Variant] | Supported mutable map of type scala.collection.mutable.Map[String, String] or scala.collection.mutable.Map[String, Variant] |
  * | GEOGRAPHY | com.snowflake.snowpark.types.Geography      | Supported                                                                                                                   |
  * | VARIANT   | com.snowflake.snowpark.types.Variant        | Supported                                                                                                                   |
  *
  * @since 0.1.0
  */
// scalastyle:on
class UDFRegistration(session: Session) extends Logging {
  private[snowpark] val handler = new UDXRegistrationHandler(session)

  // scalastyle:off line.size.limit
  /* Code below for registerTemporary 0 - 22 generated by this script
    (0 to 22).foreach { x =>
      val types = (1 to x).foldRight("RT")((i, s) => {s"A$i, $s"})
      val typeTags = (1 to x).map(i => s"A$i: TypeTag").foldLeft("RT: TypeTag")(_ + ", " + _)
      val s = if (x > 1) "s" else ""
      val version = if (x > 10) "0.12.0" else "0.6.0"
      println(s"""
        |/**
        | * Registers a Scala closure of $x argument$s as a temporary anonymous UDF that is
        | * scoped to this session.
        | *
        | * @tparam RT Return type of the UDF.
        | * @since $version
        | */
        |def registerTemporary[$typeTags](func: Function$x[$types]): UserDefinedFunction =
        | udf("registerTemporary") {
        |  register(None, _toUdf(func))
        |}""".stripMargin)
    }
   */

  /** Registers a Scala closure of 0 argument as a temporary anonymous UDF that is scoped to this
    * session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.6.0
    */
  def registerTemporary[RT: TypeTag](func: Function0[RT]): UserDefinedFunction =
    udf("registerTemporary") {
      register(None, _toUdf(func))
    }

  /** Registers a Scala closure of 1 argument as a temporary anonymous UDF that is scoped to this
    * session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.6.0
    */
  def registerTemporary[RT: TypeTag, A1: TypeTag](func: Function1[A1, RT]): UserDefinedFunction =
    udf("registerTemporary") {
      register(None, _toUdf(func))
    }

  /** Registers a Scala closure of 2 arguments as a temporary anonymous UDF that is scoped to this
    * session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.6.0
    */
  def registerTemporary[RT: TypeTag, A1: TypeTag, A2: TypeTag](
      func: Function2[A1, A2, RT]): UserDefinedFunction =
    udf("registerTemporary") {
      register(None, _toUdf(func))
    }

  /** Registers a Scala closure of 3 arguments as a temporary anonymous UDF that is scoped to this
    * session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.6.0
    */
  def registerTemporary[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag](
      func: Function3[A1, A2, A3, RT]): UserDefinedFunction =
    udf("registerTemporary") {
      register(None, _toUdf(func))
    }

  /** Registers a Scala closure of 4 arguments as a temporary anonymous UDF that is scoped to this
    * session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.6.0
    */
  def registerTemporary[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag](
      func: Function4[A1, A2, A3, A4, RT]): UserDefinedFunction =
    udf("registerTemporary") {
      register(None, _toUdf(func))
    }

  /** Registers a Scala closure of 5 arguments as a temporary anonymous UDF that is scoped to this
    * session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.6.0
    */
  def registerTemporary[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag](func: Function5[A1, A2, A3, A4, A5, RT]): UserDefinedFunction =
    udf("registerTemporary") {
      register(None, _toUdf(func))
    }

  /** Registers a Scala closure of 6 arguments as a temporary anonymous UDF that is scoped to this
    * session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.6.0
    */
  def registerTemporary[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag](func: Function6[A1, A2, A3, A4, A5, A6, RT]): UserDefinedFunction =
    udf("registerTemporary") {
      register(None, _toUdf(func))
    }

  /** Registers a Scala closure of 7 arguments as a temporary anonymous UDF that is scoped to this
    * session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.6.0
    */
  def registerTemporary[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag](func: Function7[A1, A2, A3, A4, A5, A6, A7, RT]): UserDefinedFunction =
    udf("registerTemporary") {
      register(None, _toUdf(func))
    }

  /** Registers a Scala closure of 8 arguments as a temporary anonymous UDF that is scoped to this
    * session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.6.0
    */
  def registerTemporary[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag](func: Function8[A1, A2, A3, A4, A5, A6, A7, A8, RT]): UserDefinedFunction =
    udf("registerTemporary") {
      register(None, _toUdf(func))
    }

  /** Registers a Scala closure of 9 arguments as a temporary anonymous UDF that is scoped to this
    * session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.6.0
    */
  def registerTemporary[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag](func: Function9[A1, A2, A3, A4, A5, A6, A7, A8, A9, RT]): UserDefinedFunction =
    udf("registerTemporary") {
      register(None, _toUdf(func))
    }

  /** Registers a Scala closure of 10 arguments as a temporary anonymous UDF that is scoped to this
    * session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.6.0
    */
  def registerTemporary[
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
      func: Function10[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT]): UserDefinedFunction =
    udf("registerTemporary") {
      register(None, _toUdf(func))
    }

  /** Registers a Scala closure of 11 arguments as a temporary anonymous UDF that is scoped to this
    * session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    */
  def registerTemporary[
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
      func: Function11[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, RT]): UserDefinedFunction =
    udf("registerTemporary") {
      register(None, _toUdf(func))
    }

  /** Registers a Scala closure of 12 arguments as a temporary anonymous UDF that is scoped to this
    * session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    */
  def registerTemporary[
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
      : UserDefinedFunction =
    udf("registerTemporary") {
      register(None, _toUdf(func))
    }

  /** Registers a Scala closure of 13 arguments as a temporary anonymous UDF that is scoped to this
    * session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    */
  def registerTemporary[
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
      : UserDefinedFunction =
    udf("registerTemporary") {
      register(None, _toUdf(func))
    }

  /** Registers a Scala closure of 14 arguments as a temporary anonymous UDF that is scoped to this
    * session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    */
  def registerTemporary[
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
      : UserDefinedFunction =
    udf("registerTemporary") {
      register(None, _toUdf(func))
    }

  /** Registers a Scala closure of 15 arguments as a temporary anonymous UDF that is scoped to this
    * session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    */
  def registerTemporary[
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
      : UserDefinedFunction =
    udf("registerTemporary") {
      register(None, _toUdf(func))
    }

  /** Registers a Scala closure of 16 arguments as a temporary anonymous UDF that is scoped to this
    * session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    */
  def registerTemporary[
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
      : UserDefinedFunction =
    udf("registerTemporary") {
      register(None, _toUdf(func))
    }

  /** Registers a Scala closure of 17 arguments as a temporary anonymous UDF that is scoped to this
    * session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    */
  def registerTemporary[
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
        RT]): UserDefinedFunction =
    udf("registerTemporary") {
      register(None, _toUdf(func))
    }

  /** Registers a Scala closure of 18 arguments as a temporary anonymous UDF that is scoped to this
    * session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    */
  def registerTemporary[
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
        RT]): UserDefinedFunction =
    udf("registerTemporary") {
      register(None, _toUdf(func))
    }

  /** Registers a Scala closure of 19 arguments as a temporary anonymous UDF that is scoped to this
    * session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    */
  def registerTemporary[
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
        RT]): UserDefinedFunction =
    udf("registerTemporary") {
      register(None, _toUdf(func))
    }

  /** Registers a Scala closure of 20 arguments as a temporary anonymous UDF that is scoped to this
    * session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    */
  def registerTemporary[
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
        RT]): UserDefinedFunction =
    udf("registerTemporary") {
      register(None, _toUdf(func))
    }

  /** Registers a Scala closure of 21 arguments as a temporary anonymous UDF that is scoped to this
    * session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    */
  def registerTemporary[
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
        RT]): UserDefinedFunction =
    udf("registerTemporary") {
      register(None, _toUdf(func))
    }

  /** Registers a Scala closure of 22 arguments as a temporary anonymous UDF that is scoped to this
    * session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    */
  def registerTemporary[
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
        RT]): UserDefinedFunction =
    udf("registerTemporary") {
      register(None, _toUdf(func))
    }

  // scalastyle:off line.size.limit
  /* Code below for registerTemporary 0-22 generated by this script
    (0 to 22).foreach { x =>
      val types = (1 to x).foldRight("RT")((i, s) => {s"A$i, $s"})
      val typeTags = (1 to x).map(i => s"A$i: TypeTag").foldLeft("RT: TypeTag")(_ + ", " + _)
      val s = if (x > 1) "s" else ""
      val version = if (x > 10) "0.12.0" else "0.1.0"
      println(s"""
        |/**
        | * Registers a Scala closure of $x argument$s as a temporary Snowflake Java UDF that you
        | * plan to use in the current session.
        | *
        | * @tparam RT Return type of the UDF.
        | * @since $version
        | */
        |def registerTemporary[$typeTags](name: String, func: Function$x[$types]): UserDefinedFunction =
        | udf("registerTemporary", execName = name) {
        |  register(Some(name), _toUdf(func))
        |}""".stripMargin)
    }
   */
  /** Registers a Scala closure of 0 argument as a temporary Snowflake Java UDF that you plan to use
    * in the current session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.1.0
    */
  def registerTemporary[RT: TypeTag](name: String, func: Function0[RT]): UserDefinedFunction =
    udf("registerTemporary", execName = name) {
      register(Some(name), _toUdf(func))
    }

  /** Registers a Scala closure of 1 argument as a temporary Snowflake Java UDF that you plan to use
    * in the current session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.1.0
    */
  def registerTemporary[RT: TypeTag, A1: TypeTag](
      name: String,
      func: Function1[A1, RT]): UserDefinedFunction =
    udf("registerTemporary", execName = name) {
      register(Some(name), _toUdf(func))
    }

  /** Registers a Scala closure of 2 arguments as a temporary Snowflake Java UDF that you plan to
    * use in the current session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.1.0
    */
  def registerTemporary[RT: TypeTag, A1: TypeTag, A2: TypeTag](
      name: String,
      func: Function2[A1, A2, RT]): UserDefinedFunction =
    udf("registerTemporary", execName = name) {
      register(Some(name), _toUdf(func))
    }

  /** Registers a Scala closure of 3 arguments as a temporary Snowflake Java UDF that you plan to
    * use in the current session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.1.0
    */
  def registerTemporary[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag](
      name: String,
      func: Function3[A1, A2, A3, RT]): UserDefinedFunction =
    udf("registerTemporary", execName = name) {
      register(Some(name), _toUdf(func))
    }

  /** Registers a Scala closure of 4 arguments as a temporary Snowflake Java UDF that you plan to
    * use in the current session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.1.0
    */
  def registerTemporary[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag](
      name: String,
      func: Function4[A1, A2, A3, A4, RT]): UserDefinedFunction =
    udf("registerTemporary", execName = name) {
      register(Some(name), _toUdf(func))
    }

  /** Registers a Scala closure of 5 arguments as a temporary Snowflake Java UDF that you plan to
    * use in the current session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.1.0
    */
  def registerTemporary[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag](name: String, func: Function5[A1, A2, A3, A4, A5, RT]): UserDefinedFunction =
    udf("registerTemporary", execName = name) {
      register(Some(name), _toUdf(func))
    }

  /** Registers a Scala closure of 6 arguments as a temporary Snowflake Java UDF that you plan to
    * use in the current session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.1.0
    */
  def registerTemporary[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag](name: String, func: Function6[A1, A2, A3, A4, A5, A6, RT]): UserDefinedFunction =
    udf("registerTemporary", execName = name) {
      register(Some(name), _toUdf(func))
    }

  /** Registers a Scala closure of 7 arguments as a temporary Snowflake Java UDF that you plan to
    * use in the current session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.1.0
    */
  def registerTemporary[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag](
      name: String,
      func: Function7[A1, A2, A3, A4, A5, A6, A7, RT]): UserDefinedFunction =
    udf("registerTemporary", execName = name) {
      register(Some(name), _toUdf(func))
    }

  /** Registers a Scala closure of 8 arguments as a temporary Snowflake Java UDF that you plan to
    * use in the current session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.1.0
    */
  def registerTemporary[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag](
      name: String,
      func: Function8[A1, A2, A3, A4, A5, A6, A7, A8, RT]): UserDefinedFunction =
    udf("registerTemporary", execName = name) {
      register(Some(name), _toUdf(func))
    }

  /** Registers a Scala closure of 9 arguments as a temporary Snowflake Java UDF that you plan to
    * use in the current session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.1.0
    */
  def registerTemporary[
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
      name: String,
      func: Function9[A1, A2, A3, A4, A5, A6, A7, A8, A9, RT]): UserDefinedFunction =
    udf("registerTemporary", execName = name) {
      register(Some(name), _toUdf(func))
    }

  /** Registers a Scala closure of 10 arguments as a temporary Snowflake Java UDF that you plan to
    * use in the current session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.1.0
    */
  def registerTemporary[
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
      name: String,
      func: Function10[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT]): UserDefinedFunction =
    udf("registerTemporary", execName = name) {
      register(Some(name), _toUdf(func))
    }

  /** Registers a Scala closure of 11 arguments as a temporary Snowflake Java UDF that you plan to
    * use in the current session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    */
  def registerTemporary[
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
      name: String,
      func: Function11[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, RT]): UserDefinedFunction =
    udf("registerTemporary", execName = name) {
      register(Some(name), _toUdf(func))
    }

  /** Registers a Scala closure of 12 arguments as a temporary Snowflake Java UDF that you plan to
    * use in the current session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    */
  def registerTemporary[
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
      A12: TypeTag](
      name: String,
      func: Function12[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, RT])
      : UserDefinedFunction =
    udf("registerTemporary", execName = name) {
      register(Some(name), _toUdf(func))
    }

  /** Registers a Scala closure of 13 arguments as a temporary Snowflake Java UDF that you plan to
    * use in the current session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    */
  def registerTemporary[
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
      name: String,
      func: Function13[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, RT])
      : UserDefinedFunction =
    udf("registerTemporary", execName = name) {
      register(Some(name), _toUdf(func))
    }

  /** Registers a Scala closure of 14 arguments as a temporary Snowflake Java UDF that you plan to
    * use in the current session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    */
  def registerTemporary[
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
      name: String,
      func: Function14[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, RT])
      : UserDefinedFunction =
    udf("registerTemporary", execName = name) {
      register(Some(name), _toUdf(func))
    }

  /** Registers a Scala closure of 15 arguments as a temporary Snowflake Java UDF that you plan to
    * use in the current session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    */
  def registerTemporary[
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
      name: String,
      func: Function15[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, RT])
      : UserDefinedFunction =
    udf("registerTemporary", execName = name) {
      register(Some(name), _toUdf(func))
    }

  /** Registers a Scala closure of 16 arguments as a temporary Snowflake Java UDF that you plan to
    * use in the current session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    */
  def registerTemporary[
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
      name: String,
      func: Function16[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, RT])
      : UserDefinedFunction =
    udf("registerTemporary", execName = name) {
      register(Some(name), _toUdf(func))
    }

  /** Registers a Scala closure of 17 arguments as a temporary Snowflake Java UDF that you plan to
    * use in the current session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    */
  def registerTemporary[
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
      name: String,
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
        RT]): UserDefinedFunction =
    udf("registerTemporary", execName = name) {
      register(Some(name), _toUdf(func))
    }

  /** Registers a Scala closure of 18 arguments as a temporary Snowflake Java UDF that you plan to
    * use in the current session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    */
  def registerTemporary[
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
      name: String,
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
        RT]): UserDefinedFunction =
    udf("registerTemporary", execName = name) {
      register(Some(name), _toUdf(func))
    }

  /** Registers a Scala closure of 19 arguments as a temporary Snowflake Java UDF that you plan to
    * use in the current session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    */
  def registerTemporary[
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
      name: String,
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
        RT]): UserDefinedFunction =
    udf("registerTemporary", execName = name) {
      register(Some(name), _toUdf(func))
    }

  /** Registers a Scala closure of 20 arguments as a temporary Snowflake Java UDF that you plan to
    * use in the current session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    */
  def registerTemporary[
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
      name: String,
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
        RT]): UserDefinedFunction =
    udf("registerTemporary", execName = name) {
      register(Some(name), _toUdf(func))
    }

  /** Registers a Scala closure of 21 arguments as a temporary Snowflake Java UDF that you plan to
    * use in the current session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    */
  def registerTemporary[
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
      name: String,
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
        RT]): UserDefinedFunction =
    udf("registerTemporary", execName = name) {
      register(Some(name), _toUdf(func))
    }

  /** Registers a Scala closure of 22 arguments as a temporary Snowflake Java UDF that you plan to
    * use in the current session.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    */
  def registerTemporary[
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
      name: String,
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
        RT]): UserDefinedFunction =
    udf("registerTemporary", execName = name) {
      register(Some(name), _toUdf(func))
    }

  // scalastyle:off line.size.limit
  /* Code below for _toUdf 0-22 generated by this script
    (0 to 22).foreach { x =>
      val types = (1 to x).foldRight("RT")((i, s) => {s"A$i, $s"})
      val typeTags = (1 to x).map(i => s"A$i: TypeTag").foldLeft("RT: TypeTag")(_ + ", " + _)
      val s = if (x > 1) "s" else ""
      val version = if (x > 10) "0.12.0" else "0.6.0"
      println(s"""
        |/**
        | * Registers a Scala closure of $x argument$s as a Snowflake Java UDF.
        | *
        | * The function uploads the JAR files that the UDF depends upon to the specified stage.
        | * Each JAR file is uploaded to a subdirectory named after the MD5 checksum for the file.
        | *
        | * If you register multiple UDFs and specify the same stage location, any dependent JAR
        | * files used by those functions will only be uploaded once. The JAR file for the UDF code
        | * itself will be uploaded to a subdirectory named after the UDF.
        | *
        | * @tparam RT Return type of the UDF.
        | * @since $version
        | * @param stageLocation Stage location where the JAR files for the UDF and its
        | *   and its dependencies should be uploaded.
        | */
        |def registerPermanent[$typeTags](name: String, func: Function$x[$types], stageLocation: String): UserDefinedFunction =
        | udf("registerPermanent", execName = name, execFilePath = stageLocation) {
        |  register(Some(name), _toUdf(func), Some(stageLocation))
        |}""".stripMargin)
    }
   */
  /** Registers a Scala closure of 0 argument as a Snowflake Java UDF.
    *
    * The function uploads the JAR files that the UDF depends upon to the specified stage. Each JAR
    * file is uploaded to a subdirectory named after the MD5 checksum for the file.
    *
    * If you register multiple UDFs and specify the same stage location, any dependent JAR files
    * used by those functions will only be uploaded once. The JAR file for the UDF code itself will
    * be uploaded to a subdirectory named after the UDF.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.6.0
    * @param stageLocation
    *   Stage location where the JAR files for the UDF and its and its dependencies should be
    *   uploaded.
    */
  def registerPermanent[RT: TypeTag](
      name: String,
      func: Function0[RT],
      stageLocation: String): UserDefinedFunction =
    udf("registerPermanent", execName = name, execFilePath = stageLocation) {
      register(Some(name), _toUdf(func), Some(stageLocation))
    }

  /** Registers a Scala closure of 1 argument as a Snowflake Java UDF.
    *
    * The function uploads the JAR files that the UDF depends upon to the specified stage. Each JAR
    * file is uploaded to a subdirectory named after the MD5 checksum for the file.
    *
    * If you register multiple UDFs and specify the same stage location, any dependent JAR files
    * used by those functions will only be uploaded once. The JAR file for the UDF code itself will
    * be uploaded to a subdirectory named after the UDF.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.6.0
    * @param stageLocation
    *   Stage location where the JAR files for the UDF and its and its dependencies should be
    *   uploaded.
    */
  def registerPermanent[RT: TypeTag, A1: TypeTag](
      name: String,
      func: Function1[A1, RT],
      stageLocation: String): UserDefinedFunction =
    udf("registerPermanent", execName = name, execFilePath = stageLocation) {
      register(Some(name), _toUdf(func), Some(stageLocation))
    }

  /** Registers a Scala closure of 2 arguments as a Snowflake Java UDF.
    *
    * The function uploads the JAR files that the UDF depends upon to the specified stage. Each JAR
    * file is uploaded to a subdirectory named after the MD5 checksum for the file.
    *
    * If you register multiple UDFs and specify the same stage location, any dependent JAR files
    * used by those functions will only be uploaded once. The JAR file for the UDF code itself will
    * be uploaded to a subdirectory named after the UDF.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.6.0
    * @param stageLocation
    *   Stage location where the JAR files for the UDF and its and its dependencies should be
    *   uploaded.
    */
  def registerPermanent[RT: TypeTag, A1: TypeTag, A2: TypeTag](
      name: String,
      func: Function2[A1, A2, RT],
      stageLocation: String): UserDefinedFunction =
    udf("registerPermanent", execName = name, execFilePath = stageLocation) {
      register(Some(name), _toUdf(func), Some(stageLocation))
    }

  /** Registers a Scala closure of 3 arguments as a Snowflake Java UDF.
    *
    * The function uploads the JAR files that the UDF depends upon to the specified stage. Each JAR
    * file is uploaded to a subdirectory named after the MD5 checksum for the file.
    *
    * If you register multiple UDFs and specify the same stage location, any dependent JAR files
    * used by those functions will only be uploaded once. The JAR file for the UDF code itself will
    * be uploaded to a subdirectory named after the UDF.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.6.0
    * @param stageLocation
    *   Stage location where the JAR files for the UDF and its and its dependencies should be
    *   uploaded.
    */
  def registerPermanent[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag](
      name: String,
      func: Function3[A1, A2, A3, RT],
      stageLocation: String): UserDefinedFunction =
    udf("registerPermanent", execName = name, execFilePath = stageLocation) {
      register(Some(name), _toUdf(func), Some(stageLocation))
    }

  /** Registers a Scala closure of 4 arguments as a Snowflake Java UDF.
    *
    * The function uploads the JAR files that the UDF depends upon to the specified stage. Each JAR
    * file is uploaded to a subdirectory named after the MD5 checksum for the file.
    *
    * If you register multiple UDFs and specify the same stage location, any dependent JAR files
    * used by those functions will only be uploaded once. The JAR file for the UDF code itself will
    * be uploaded to a subdirectory named after the UDF.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.6.0
    * @param stageLocation
    *   Stage location where the JAR files for the UDF and its and its dependencies should be
    *   uploaded.
    */
  def registerPermanent[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag](
      name: String,
      func: Function4[A1, A2, A3, A4, RT],
      stageLocation: String): UserDefinedFunction =
    udf("registerPermanent", execName = name, execFilePath = stageLocation) {
      register(Some(name), _toUdf(func), Some(stageLocation))
    }

  /** Registers a Scala closure of 5 arguments as a Snowflake Java UDF.
    *
    * The function uploads the JAR files that the UDF depends upon to the specified stage. Each JAR
    * file is uploaded to a subdirectory named after the MD5 checksum for the file.
    *
    * If you register multiple UDFs and specify the same stage location, any dependent JAR files
    * used by those functions will only be uploaded once. The JAR file for the UDF code itself will
    * be uploaded to a subdirectory named after the UDF.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.6.0
    * @param stageLocation
    *   Stage location where the JAR files for the UDF and its and its dependencies should be
    *   uploaded.
    */
  def registerPermanent[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag](
      name: String,
      func: Function5[A1, A2, A3, A4, A5, RT],
      stageLocation: String): UserDefinedFunction =
    udf("registerPermanent", execName = name, execFilePath = stageLocation) {
      register(Some(name), _toUdf(func), Some(stageLocation))
    }

  /** Registers a Scala closure of 6 arguments as a Snowflake Java UDF.
    *
    * The function uploads the JAR files that the UDF depends upon to the specified stage. Each JAR
    * file is uploaded to a subdirectory named after the MD5 checksum for the file.
    *
    * If you register multiple UDFs and specify the same stage location, any dependent JAR files
    * used by those functions will only be uploaded once. The JAR file for the UDF code itself will
    * be uploaded to a subdirectory named after the UDF.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.6.0
    * @param stageLocation
    *   Stage location where the JAR files for the UDF and its and its dependencies should be
    *   uploaded.
    */
  def registerPermanent[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag](
      name: String,
      func: Function6[A1, A2, A3, A4, A5, A6, RT],
      stageLocation: String): UserDefinedFunction =
    udf("registerPermanent", execName = name, execFilePath = stageLocation) {
      register(Some(name), _toUdf(func), Some(stageLocation))
    }

  /** Registers a Scala closure of 7 arguments as a Snowflake Java UDF.
    *
    * The function uploads the JAR files that the UDF depends upon to the specified stage. Each JAR
    * file is uploaded to a subdirectory named after the MD5 checksum for the file.
    *
    * If you register multiple UDFs and specify the same stage location, any dependent JAR files
    * used by those functions will only be uploaded once. The JAR file for the UDF code itself will
    * be uploaded to a subdirectory named after the UDF.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.6.0
    * @param stageLocation
    *   Stage location where the JAR files for the UDF and its and its dependencies should be
    *   uploaded.
    */
  def registerPermanent[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag](
      name: String,
      func: Function7[A1, A2, A3, A4, A5, A6, A7, RT],
      stageLocation: String): UserDefinedFunction =
    udf("registerPermanent", execName = name, execFilePath = stageLocation) {
      register(Some(name), _toUdf(func), Some(stageLocation))
    }

  /** Registers a Scala closure of 8 arguments as a Snowflake Java UDF.
    *
    * The function uploads the JAR files that the UDF depends upon to the specified stage. Each JAR
    * file is uploaded to a subdirectory named after the MD5 checksum for the file.
    *
    * If you register multiple UDFs and specify the same stage location, any dependent JAR files
    * used by those functions will only be uploaded once. The JAR file for the UDF code itself will
    * be uploaded to a subdirectory named after the UDF.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.6.0
    * @param stageLocation
    *   Stage location where the JAR files for the UDF and its and its dependencies should be
    *   uploaded.
    */
  def registerPermanent[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag](
      name: String,
      func: Function8[A1, A2, A3, A4, A5, A6, A7, A8, RT],
      stageLocation: String): UserDefinedFunction =
    udf("registerPermanent", execName = name, execFilePath = stageLocation) {
      register(Some(name), _toUdf(func), Some(stageLocation))
    }

  /** Registers a Scala closure of 9 arguments as a Snowflake Java UDF.
    *
    * The function uploads the JAR files that the UDF depends upon to the specified stage. Each JAR
    * file is uploaded to a subdirectory named after the MD5 checksum for the file.
    *
    * If you register multiple UDFs and specify the same stage location, any dependent JAR files
    * used by those functions will only be uploaded once. The JAR file for the UDF code itself will
    * be uploaded to a subdirectory named after the UDF.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.6.0
    * @param stageLocation
    *   Stage location where the JAR files for the UDF and its and its dependencies should be
    *   uploaded.
    */
  def registerPermanent[
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
      name: String,
      func: Function9[A1, A2, A3, A4, A5, A6, A7, A8, A9, RT],
      stageLocation: String): UserDefinedFunction =
    udf("registerPermanent", execName = name, execFilePath = stageLocation) {
      register(Some(name), _toUdf(func), Some(stageLocation))
    }

  /** Registers a Scala closure of 10 arguments as a Snowflake Java UDF.
    *
    * The function uploads the JAR files that the UDF depends upon to the specified stage. Each JAR
    * file is uploaded to a subdirectory named after the MD5 checksum for the file.
    *
    * If you register multiple UDFs and specify the same stage location, any dependent JAR files
    * used by those functions will only be uploaded once. The JAR file for the UDF code itself will
    * be uploaded to a subdirectory named after the UDF.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.6.0
    * @param stageLocation
    *   Stage location where the JAR files for the UDF and its and its dependencies should be
    *   uploaded.
    */
  def registerPermanent[
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
      name: String,
      func: Function10[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT],
      stageLocation: String): UserDefinedFunction =
    udf("registerPermanent", execName = name, execFilePath = stageLocation) {
      register(Some(name), _toUdf(func), Some(stageLocation))
    }

  /** Registers a Scala closure of 11 arguments as a Snowflake Java UDF.
    *
    * The function uploads the JAR files that the UDF depends upon to the specified stage. Each JAR
    * file is uploaded to a subdirectory named after the MD5 checksum for the file.
    *
    * If you register multiple UDFs and specify the same stage location, any dependent JAR files
    * used by those functions will only be uploaded once. The JAR file for the UDF code itself will
    * be uploaded to a subdirectory named after the UDF.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    * @param stageLocation
    *   Stage location where the JAR files for the UDF and its and its dependencies should be
    *   uploaded.
    */
  def registerPermanent[
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
      name: String,
      func: Function11[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, RT],
      stageLocation: String): UserDefinedFunction =
    udf("registerPermanent", execName = name, execFilePath = stageLocation) {
      register(Some(name), _toUdf(func), Some(stageLocation))
    }

  /** Registers a Scala closure of 12 arguments as a Snowflake Java UDF.
    *
    * The function uploads the JAR files that the UDF depends upon to the specified stage. Each JAR
    * file is uploaded to a subdirectory named after the MD5 checksum for the file.
    *
    * If you register multiple UDFs and specify the same stage location, any dependent JAR files
    * used by those functions will only be uploaded once. The JAR file for the UDF code itself will
    * be uploaded to a subdirectory named after the UDF.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    * @param stageLocation
    *   Stage location where the JAR files for the UDF and its and its dependencies should be
    *   uploaded.
    */
  def registerPermanent[
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
      A12: TypeTag](
      name: String,
      func: Function12[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, RT],
      stageLocation: String): UserDefinedFunction =
    udf("registerPermanent", execName = name, execFilePath = stageLocation) {
      register(Some(name), _toUdf(func), Some(stageLocation))
    }

  /** Registers a Scala closure of 13 arguments as a Snowflake Java UDF.
    *
    * The function uploads the JAR files that the UDF depends upon to the specified stage. Each JAR
    * file is uploaded to a subdirectory named after the MD5 checksum for the file.
    *
    * If you register multiple UDFs and specify the same stage location, any dependent JAR files
    * used by those functions will only be uploaded once. The JAR file for the UDF code itself will
    * be uploaded to a subdirectory named after the UDF.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    * @param stageLocation
    *   Stage location where the JAR files for the UDF and its and its dependencies should be
    *   uploaded.
    */
  def registerPermanent[
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
      name: String,
      func: Function13[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, RT],
      stageLocation: String): UserDefinedFunction =
    udf("registerPermanent", execName = name, execFilePath = stageLocation) {
      register(Some(name), _toUdf(func), Some(stageLocation))
    }

  /** Registers a Scala closure of 14 arguments as a Snowflake Java UDF.
    *
    * The function uploads the JAR files that the UDF depends upon to the specified stage. Each JAR
    * file is uploaded to a subdirectory named after the MD5 checksum for the file.
    *
    * If you register multiple UDFs and specify the same stage location, any dependent JAR files
    * used by those functions will only be uploaded once. The JAR file for the UDF code itself will
    * be uploaded to a subdirectory named after the UDF.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    * @param stageLocation
    *   Stage location where the JAR files for the UDF and its and its dependencies should be
    *   uploaded.
    */
  def registerPermanent[
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
      name: String,
      func: Function14[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, RT],
      stageLocation: String): UserDefinedFunction =
    udf("registerPermanent", execName = name, execFilePath = stageLocation) {
      register(Some(name), _toUdf(func), Some(stageLocation))
    }

  /** Registers a Scala closure of 15 arguments as a Snowflake Java UDF.
    *
    * The function uploads the JAR files that the UDF depends upon to the specified stage. Each JAR
    * file is uploaded to a subdirectory named after the MD5 checksum for the file.
    *
    * If you register multiple UDFs and specify the same stage location, any dependent JAR files
    * used by those functions will only be uploaded once. The JAR file for the UDF code itself will
    * be uploaded to a subdirectory named after the UDF.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    * @param stageLocation
    *   Stage location where the JAR files for the UDF and its and its dependencies should be
    *   uploaded.
    */
  def registerPermanent[
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
      name: String,
      func: Function15[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, RT],
      stageLocation: String): UserDefinedFunction =
    udf("registerPermanent", execName = name, execFilePath = stageLocation) {
      register(Some(name), _toUdf(func), Some(stageLocation))
    }

  /** Registers a Scala closure of 16 arguments as a Snowflake Java UDF.
    *
    * The function uploads the JAR files that the UDF depends upon to the specified stage. Each JAR
    * file is uploaded to a subdirectory named after the MD5 checksum for the file.
    *
    * If you register multiple UDFs and specify the same stage location, any dependent JAR files
    * used by those functions will only be uploaded once. The JAR file for the UDF code itself will
    * be uploaded to a subdirectory named after the UDF.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    * @param stageLocation
    *   Stage location where the JAR files for the UDF and its and its dependencies should be
    *   uploaded.
    */
  def registerPermanent[
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
      name: String,
      func: Function16[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, RT],
      stageLocation: String): UserDefinedFunction =
    udf("registerPermanent", execName = name, execFilePath = stageLocation) {
      register(Some(name), _toUdf(func), Some(stageLocation))
    }

  /** Registers a Scala closure of 17 arguments as a Snowflake Java UDF.
    *
    * The function uploads the JAR files that the UDF depends upon to the specified stage. Each JAR
    * file is uploaded to a subdirectory named after the MD5 checksum for the file.
    *
    * If you register multiple UDFs and specify the same stage location, any dependent JAR files
    * used by those functions will only be uploaded once. The JAR file for the UDF code itself will
    * be uploaded to a subdirectory named after the UDF.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    * @param stageLocation
    *   Stage location where the JAR files for the UDF and its and its dependencies should be
    *   uploaded.
    */
  def registerPermanent[
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
      name: String,
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
        RT],
      stageLocation: String): UserDefinedFunction =
    udf("registerPermanent", execName = name, execFilePath = stageLocation) {
      register(Some(name), _toUdf(func), Some(stageLocation))
    }

  /** Registers a Scala closure of 18 arguments as a Snowflake Java UDF.
    *
    * The function uploads the JAR files that the UDF depends upon to the specified stage. Each JAR
    * file is uploaded to a subdirectory named after the MD5 checksum for the file.
    *
    * If you register multiple UDFs and specify the same stage location, any dependent JAR files
    * used by those functions will only be uploaded once. The JAR file for the UDF code itself will
    * be uploaded to a subdirectory named after the UDF.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    * @param stageLocation
    *   Stage location where the JAR files for the UDF and its and its dependencies should be
    *   uploaded.
    */
  def registerPermanent[
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
      name: String,
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
        RT],
      stageLocation: String): UserDefinedFunction =
    udf("registerPermanent", execName = name, execFilePath = stageLocation) {
      register(Some(name), _toUdf(func), Some(stageLocation))
    }

  /** Registers a Scala closure of 19 arguments as a Snowflake Java UDF.
    *
    * The function uploads the JAR files that the UDF depends upon to the specified stage. Each JAR
    * file is uploaded to a subdirectory named after the MD5 checksum for the file.
    *
    * If you register multiple UDFs and specify the same stage location, any dependent JAR files
    * used by those functions will only be uploaded once. The JAR file for the UDF code itself will
    * be uploaded to a subdirectory named after the UDF.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    * @param stageLocation
    *   Stage location where the JAR files for the UDF and its and its dependencies should be
    *   uploaded.
    */
  def registerPermanent[
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
      name: String,
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
        RT],
      stageLocation: String): UserDefinedFunction =
    udf("registerPermanent", execName = name, execFilePath = stageLocation) {
      register(Some(name), _toUdf(func), Some(stageLocation))
    }

  /** Registers a Scala closure of 20 arguments as a Snowflake Java UDF.
    *
    * The function uploads the JAR files that the UDF depends upon to the specified stage. Each JAR
    * file is uploaded to a subdirectory named after the MD5 checksum for the file.
    *
    * If you register multiple UDFs and specify the same stage location, any dependent JAR files
    * used by those functions will only be uploaded once. The JAR file for the UDF code itself will
    * be uploaded to a subdirectory named after the UDF.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    * @param stageLocation
    *   Stage location where the JAR files for the UDF and its and its dependencies should be
    *   uploaded.
    */
  def registerPermanent[
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
      name: String,
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
        RT],
      stageLocation: String): UserDefinedFunction =
    udf("registerPermanent", execName = name, execFilePath = stageLocation) {
      register(Some(name), _toUdf(func), Some(stageLocation))
    }

  /** Registers a Scala closure of 21 arguments as a Snowflake Java UDF.
    *
    * The function uploads the JAR files that the UDF depends upon to the specified stage. Each JAR
    * file is uploaded to a subdirectory named after the MD5 checksum for the file.
    *
    * If you register multiple UDFs and specify the same stage location, any dependent JAR files
    * used by those functions will only be uploaded once. The JAR file for the UDF code itself will
    * be uploaded to a subdirectory named after the UDF.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    * @param stageLocation
    *   Stage location where the JAR files for the UDF and its and its dependencies should be
    *   uploaded.
    */
  def registerPermanent[
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
      name: String,
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
        RT],
      stageLocation: String): UserDefinedFunction =
    udf("registerPermanent", execName = name, execFilePath = stageLocation) {
      register(Some(name), _toUdf(func), Some(stageLocation))
    }

  /** Registers a Scala closure of 22 arguments as a Snowflake Java UDF.
    *
    * The function uploads the JAR files that the UDF depends upon to the specified stage. Each JAR
    * file is uploaded to a subdirectory named after the MD5 checksum for the file.
    *
    * If you register multiple UDFs and specify the same stage location, any dependent JAR files
    * used by those functions will only be uploaded once. The JAR file for the UDF code itself will
    * be uploaded to a subdirectory named after the UDF.
    *
    * @tparam RT
    *   Return type of the UDF.
    * @since 0.12.0
    * @param stageLocation
    *   Stage location where the JAR files for the UDF and its and its dependencies should be
    *   uploaded.
    */
  def registerPermanent[
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
      name: String,
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
        RT],
      stageLocation: String): UserDefinedFunction =
    udf("registerPermanent", execName = name, execFilePath = stageLocation) {
      register(Some(name), _toUdf(func), Some(stageLocation))
    }

  private[snowpark] def register(
      name: Option[String],
      udf: UserDefinedFunction,
      // if stageLocation is none, this udf will be temporary udf
      stageLocation: Option[String] = None): UserDefinedFunction =
    handler.registerUDF(name, udf, stageLocation)

  @inline protected def udf(funcName: String, execName: String = "", execFilePath: String = "")(
      func: => UserDefinedFunction): UserDefinedFunction = {
    OpenTelemetry.udx(
      "UDFRegistration",
      funcName,
      execName,
      s"${UDXRegistrationHandler.className}.${UDXRegistrationHandler.methodName}",
      execFilePath)(func)
  }
}
