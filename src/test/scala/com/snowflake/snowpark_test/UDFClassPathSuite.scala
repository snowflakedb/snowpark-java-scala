package com.snowflake.snowpark_test

import com.snowflake.snowpark.internal.UDFClassPath
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class UDFClassPathSuite extends AnyFunSuite {

  test("isClassDefined closes temporary URLClassLoader") {
    val snowparkUri = new File(
      UDFClassPath.snowparkJar.location.getOrElse(
        fail("snowpark jar location must be resolvable"))).toURI
    assert(
      UDFClassPath
        .isClassDefined(Seq(snowparkUri), classOf[com.snowflake.snowpark.Session].getName))
    assert(!UDFClassPath.isClassDefined(Seq(snowparkUri), "com.example.DoesNotExist"))
    // Call again to ensure jar handles from the temporary loader were released.
    assert(
      UDFClassPath
        .isClassDefined(Seq(snowparkUri), classOf[com.snowflake.snowpark.Session].getName))
  }
}
