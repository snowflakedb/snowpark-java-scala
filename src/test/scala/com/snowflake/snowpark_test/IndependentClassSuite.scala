package com.snowflake.snowpark_test

import org.scalatest.FunSuite
import org.scalatest.exceptions.TestFailedException
import com.snowflake.snowpark.internal.Utils

import scala.language.postfixOps
import sys.process._

// verify those classes do not depend on Snowpark package
class IndependentClassSuite extends FunSuite {
  lazy val pathPrefix = s"target/scala-${Utils.ScalaCompatVersion}/"
  private def generatePath(path: String): String = pathPrefix + path
  test("scala variant") {
    checkDependencies(
      generatePath("classes/com/snowflake/snowpark/types/Variant.class"),
      Seq("com.snowflake.snowpark.types.Variant"))

    checkDependencies(
      generatePath("classes/com/snowflake/snowpark/types/Variant$.class"),
      Seq(
        "com.snowflake.snowpark.types.Variant",
        "com.snowflake.snowpark.types.Geography",
        "com.snowflake.snowpark.types.Geometry"))
  }

  test("java variant") {
    checkDependencies(
      generatePath("classes/com/snowflake/snowpark_java/types/Variant.class"),
      Seq(
        "com.snowflake.snowpark_java.types.Variant",
        "com.snowflake.snowpark_java.types.Geography"))
  }

  test("scala geography") {
    checkDependencies(
      generatePath("classes/com/snowflake/snowpark/types/Geography.class"),
      Seq("com.snowflake.snowpark.types.Geography"))

    checkDependencies(
      generatePath("classes/com/snowflake/snowpark/types/Geography$.class"),
      Seq("com.snowflake.snowpark.types.Geography"))
  }

  test("java geography") {
    checkDependencies(
      generatePath("classes/com/snowflake/snowpark_java/types/Geography.class"),
      Seq("com.snowflake.snowpark_java.types.Geography"))
  }

  test("scala geometry") {
    checkDependencies(
      generatePath("classes/com/snowflake/snowpark/types/Geometry.class"),
      Seq("com.snowflake.snowpark.types.Geometry"))

    checkDependencies(
      generatePath("classes/com/snowflake/snowpark/types/Geometry$.class"),
      Seq("com.snowflake.snowpark.types.Geometry"))
  }

  test("java geometry") {
    checkDependencies(
      generatePath("classes/com/snowflake/snowpark_java/types/Geometry.class"),
      Seq("com.snowflake.snowpark_java.types.Geometry"))
  }

  // negative test, to make sure this test method works
  test("session") {
    assertThrows[TestFailedException] {
      checkDependencies(
        generatePath("classes/com/snowflake/snowpark/Session.class"),
        Seq("com.snowflake.snowpark.Session"))
    }
  }

  def checkDependencies(path: String, except: Seq[String]): Unit = {
    val scanResult: String = s"jdeps -v $path" !!

    // file should exist
    assert(!scanResult.contains("Path does not exist"))
    var replaced = scanResult
    except.foreach(str => replaced = replaced.replaceAll(str, ""))
    assert(!replaced.contains("snowpark"))
  }
}
