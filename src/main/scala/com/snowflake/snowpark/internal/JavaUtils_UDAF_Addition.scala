package com.snowflake.snowpark.internal

import com.snowflake.snowpark.{Column, UDAFRegistration}
import com.snowflake.snowpark_java.udaf.JavaUDAF

/**
 * Additional methods for JavaUtils to support UDAF registration. This file contains the bridge
 * methods between Java and Scala APIs for UDAF.
 *
 * Add these methods to JavaUtils.scala
 */
object JavaUtils_UDAF_Addition {

  /**
   * Register a Java UDAF from Java API. Similar to registerJavaUDTF but for aggregate functions.
   *
   * @param udafRegistration
   *   The UDAFRegistration instance
   * @param name
   *   Optional function name
   * @param javaUdaf
   *   The Java UDAF instance
   * @param stageLocation
   *   Optional stage location for permanent UDAF
   * @return
   *   A Column representing the aggregate function
   */
  def registerJavaUDAF(
      udafRegistration: UDAFRegistration,
      name: String,
      javaUdaf: JavaUDAF[_, _],
      stageLocation: String): Column = {
    udafRegistration.registerJavaUDAF(Option(name), javaUdaf, Option(stageLocation))
  }
}

/*
 * Add the following method to JavaUtils.scala after the registerJavaUDTF methods:
 *
 *   def registerJavaUDAF(
 *       udafRegistration: UDAFRegistration,
 *       name: String,
 *       javaUdaf: JavaUDAF[_, _],
 *       stageLocation: String): Column =
 *     udafRegistration.registerJavaUDAF(Option(name), javaUdaf, Option(stageLocation))
 */
