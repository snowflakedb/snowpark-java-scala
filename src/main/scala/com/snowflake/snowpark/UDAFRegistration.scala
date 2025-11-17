package com.snowflake.snowpark

import com.snowflake.snowpark.internal.{Logging, UDXRegistrationHandler}
import com.snowflake.snowpark_java.udaf.JavaUDAF

/**
 * Provides methods to register UDAF (User-Defined Aggregate Function) in Snowflake.
 *
 * Similar to UDFRegistration and UDTFRegistration, but for aggregate functions.
 *
 * @since 1.16.0
 */
class UDAFRegistration(session: Session) extends Logging {
  private[snowpark] val handler = new UDXRegistrationHandler(session)

  /**
   * Registers a Java UDAF instance as a temporary anonymous UDAF.
   *
   * @param udaf
   *   The Java UDAF instance
   * @return
   *   A Column representing the aggregate function
   * @since 1.16.0
   */
  def registerTemporary(udaf: JavaUDAF[_, _]): Column = {
    registerJavaUDAF(None, udaf, None)
  }

  /**
   * Registers a Java UDAF instance as a temporary named UDAF.
   *
   * @param name
   *   The name for the UDAF
   * @param udaf
   *   The Java UDAF instance
   * @return
   *   A Column representing the aggregate function
   * @since 1.16.0
   */
  def registerTemporary(name: String, udaf: JavaUDAF[_, _]): Column = {
    registerJavaUDAF(Some(name), udaf, None)
  }

  /**
   * Registers a Java UDAF instance as a permanent UDAF.
   *
   * @param name
   *   The name for the UDAF
   * @param udaf
   *   The Java UDAF instance
   * @param stageLocation
   *   Stage location for uploading JAR files
   * @return
   *   A Column representing the aggregate function
   * @since 1.16.0
   */
  def registerPermanent(name: String, udaf: JavaUDAF[_, _], stageLocation: String): Column = {
    registerJavaUDAF(Some(name), udaf, Some(stageLocation))
  }

  // Internal function for Java API UDAF support
  private[snowpark] def registerJavaUDAF(
      name: Option[String],
      udaf: JavaUDAF[_, _],
      stageLocation: Option[String]): Column =
    handler.registerJavaUDAF(name, udaf, stageLocation)
}
