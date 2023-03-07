package com.snowflake.snowpark

import com.snowflake.snowpark.internal.{Logging, UDXRegistrationHandler}
import com.snowflake.snowpark.udtf.UDTF
import com.snowflake.snowpark_java.udtf.JavaUDTF

// scalastyle:off
/**
 * Provides methods to register a UDTF (user-defined table function) in the Snowflake database.
 *
 * [[Session.udtf]] returns an object of this class.
 *
 * To register an UDTF, you must:
 *
 *   1. Define a UDTF class.
 *   1. Create an instance of that class, and register that instance as a UDTF.
 *
 * The next sections describe these steps in more detail.
 *
 * =Defining the UDTF Class=
 *
 * Define a class that inherits from one of the `UDTF[N]` classes (e.g. `UDTF0`, `UDTF1`, etc.),
 * where ''n'' specifies the number of input arguments for your UDTF. For example, if your
 * UDTF passes in 3 input arguments, extend the `UDTF3` class.
 *
 * In your class, override the following three methods:
 *    - `process()` , which is called once for each row in the input partition.
 *    - `endPartition()`, which is called once for each partition after all rows have been passed to `process()`.
 *    - `outputSchema()`, which returns a [[types.StructType]] object that describes the schema for the returned rows.
 *
 * When a UDTF is called, the rows are grouped into partitions before they are passed to the UDTF:
 *    - If the statement that calls the UDTF specifies the PARTITION clause (explicit partitions),
 *      that clause determines how the rows are partitioned.
 *    - If the statement does not specify the PARTITION clause (implicit partitions),
 *      Snowflake determines how best to partition the rows.
 *
 * For an explanation of partitions, see
 * [[https://docs.snowflake.com/en/developer-guide/udf/java/udf-java-tabular-functions.html#label-udf-java-partitions Table Functions and Partitions]]
 *
 * ==Defining the process() Method==
 *
 * This method is invoked once for each row in the input partition.
 *
 * The arguments passed to the registered UDTF are passed to `process()`. For each
 * argument passed to the UDTF, you must have a corresponding argument in the signature
 * of the `process()` method. Make sure that the type of the argument in the `process()`
 * method matches the Snowflake data type of the corresponding argument in the UDTF.
 *
 * Snowflake supports the following data types for the parameters for a UDTF:
 *
 * | SQL Type | Scala Type| Notes |
 * | ---  |  --- | --- |
 * | NUMBER | Short or Option[Short] | Supported |
 * | NUMBER | Int or Option[Int] | Supported |
 * | NUMBER | Long or Option[Long] | Supported |
 * | FLOAT | Float or Option[Float] | Supported |
 * | DOUBLE | Double or Option[Double]  | Supported |
 * | NUMBER | java.math.BigDecimal | Supported |
 * | VARCHAR | String or java.lang.String | Supported |
 * | BOOL | Boolean or Option[Boolean]| Supported |
 * | DATE | java.sql.Date | Supported |
 * | TIMESTAMP | java.sql.Timestamp| Supported |
 * | BINARY | Array[Byte] | Supported |
 * | ARRAY| Array[String] or Array[Variant] | Supported array of type Array[String] or Array[Variant] |
 * | OBJECT | Map[String, String] or Map[String, Variant] | Supported mutable map of type scala.collection.mutable.Map[String, String] or scala.collection.mutable.Map[String, Variant] |
 * | VARIANT   | com.snowflake.snowpark.types.Variant | Supported |
 *
 * ==Defining the endPartition() Method==
 *
 * This method is invoked once for each partition, after all rows in that partition have been
 * passed to the `process()` method.
 *
 * You can use this method to generate output rows, based on any state information that you
 * aggregate in the `process()` method.
 *
 * ==Defining the outputSchema() Method==
 *
 * In this method, define the output schema for the rows returned by the `process()` and
 * `endPartition()` methods.
 *
 * Construct and return a [[types.StructType]] object that uses an Array of [[types.StructField]] objects
 * to specify the Snowflake data type of each field in a returned row.
 *
 * Snowflake supports the following DataTypes for the output schema for a UDTF:
 *
 * | DataType | SQL Type | Notes |
 * | ---  |  --- | --- |
 * | BooleanType | Boolean | Supported |
 * | ShortType | NUMBER | Supported |
 * | IntegerType | NUMBER | Supported |
 * | LongType | NUMBER | Supported |
 * | DecimalType | NUMBER | Supported |
 * | FloatType | FLOAT | Supported |
 * | DoubleType | DOUBLE | Supported |
 * | StringType | VARCHAR | Supported |
 * | BinaryType | BINARY | Supported |
 * | TimeType | TIME | Supported |
 * | DateType | DATE | Supported |
 * | TimestampType | TIMESTAMP | Supported |
 * | VariantType | VARIANT | Supported |
 * | ArrayType(StringType) | ARRAY | Supported |
 * | ArrayType(VariantType) | ARRAY | Supported |
 * | MapType(StringType, StringType) | OBJECT | Supported |
 * | MapType(StringType, VariantType) | OBJECT | Supported |
 *
 * ==Example of a UDTF Class==
 *
 * The following is an example of a UDTF class that generates a range of rows.
 *
 * The UDTF passes in 2 arguments, so the class extends `UDTF2`.
 *
 * The arguments `start` and `count` specify the starting number for the row and the
 * number of rows to generate.
 *
 * {{{
 *    class MyRangeUdtf extends UDTF2[Int, Int] {
 *        override def process(start: Int, count: Int): Iterable[Row] =
 *            (start until (start + count)).map(Row(_))
 *        override def endPartition(): Iterable[Row] = Array.empty[Row]
 *        override def outputSchema(): StructType = StructType(StructField("C1", IntegerType))
 *    }
 * }}}
 *
 * =Registering the UDTF=
 *
 * Next, create an instance of the new class, and register the class by calling one of the
 * [[UDTFRegistration]] methods. You can register a temporary or permanent UDTF
 * by name. If you don't need to call the UDTF by name, you can register an anonymous
 * UDTF.
 *
 * ==Registering a Temporary UDTF By Name==
 *
 * To register a temporary UDTF by name, call `registerTemporary`, passing in a name
 * for the UDTF and an instance of the UDTF class. For example:
 * {{{
 *    // Use the MyRangeUdtf defined in previous example.
 *    val tableFunction = session.udtf.registerTemporary("myUdtf", new MyRangeUdtf())
 *    session.tableFunction(tableFunction, lit(10), lit(5)).show
 * }}}
 *
 * ==Registering a Permanent UDTF By Name==
 *
 * If you need to use the UDTF in subsequent sessions, register a permanent UDTF.
 *
 * When registering a permanent UDTF, you must specify a stage where the registration
 * method will upload the JAR files for the UDTF and its dependencies. For example:
 * {{{
 *    val tableFunction = session.udtf.registerPermanent("myUdtf", new MyRangeUdtf(), "@myStage")
 *    session.tableFunction(tableFunction, lit(10), lit(5)).show
 *  }}}
 *
 * ==Registering an Anonymous Temporary UDTF==
 *
 * If you do not need to refer to a UDTF by name, use [[registerTemporary(udtf* UDTF)]]
 * to create an anonymous UDTF instead.
 *
 * ==Calling a UDTF==
 * The methods that register a UDTF return a [[TableFunction]] object, which you can use in
 * [[Session.tableFunction]].
 * {{{
 *    val tableFunction = session.udtf.registerTemporary("myUdtf", new MyRangeUdtf())
 *    session.tableFunction(tableFunction, lit(10), lit(5)).show
 * }}}
 *
 * @since 1.2.0
 *
 */
// scalastyle:on
class UDTFRegistration(session: Session) extends Logging {
  private[snowpark] val handler = new UDXRegistrationHandler(session)

  /**
   * Registers an UDTF instance as a temporary anonymous UDTF that is
   * scoped to this session.
   *
   * @param udtf The UDTF instance to be registered
   * @since 1.2.0
   */
  def registerTemporary(udtf: UDTF): TableFunction = handler.registerUDTF(None, udtf)

  /**
   * Registers an UDTF instance as a temporary Snowflake Java UDTF that you
   * plan to use in the current session.
   *
   * @param funcName The UDTF function name
   * @param udtf The UDTF instance to be registered
   * @since 1.2.0
   */
  def registerTemporary(funcName: String, udtf: UDTF): TableFunction =
    handler.registerUDTF(Some(funcName), udtf)

  /**
   * Registers an UDTF instance as a Snowflake Java UDTF.
   *
   * The function uploads the JAR files that the UDTF depends upon to the specified stage.
   * Each JAR file is uploaded to a subdirectory named after the MD5 checksum for the file.
   *
   * If you register multiple UDTFs and specify the same stage location, any dependent JAR
   * files used by those functions will only be uploaded once. The JAR file for the UDTF code
   * itself will be uploaded to a subdirectory named after the UDTF.
   *
   * @param funcName The UDTF function name
   * @param udtf The UDTF instance to be registered.
   * @param stageLocation Stage location where the JAR files for the UDTF and its
   *   and its dependencies should be uploaded
   * @since 1.2.0
   */
  def registerPermanent(funcName: String, udtf: UDTF, stageLocation: String): TableFunction =
    handler.registerUDTF(Some(funcName), udtf, Some(stageLocation))

  // Internal function for Java API UDTF support
  private[snowpark] def registerJavaUDTF(
      name: Option[String],
      udtf: JavaUDTF,
      stageLocation: Option[String]): TableFunction =
    handler.registerJavaUDTF(name, udtf, stageLocation)
}
