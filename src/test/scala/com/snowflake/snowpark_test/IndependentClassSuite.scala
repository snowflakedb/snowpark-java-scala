package com.snowflake.snowpark_test

import org.scalatest.FunSuite
import org.scalatest.exceptions.TestFailedException

import scala.language.postfixOps
import sys.process._

// verify those classes do not depend on Snowpark package
class IndependentClassSuite extends FunSuite {
  test("scala variant") {
    checkDependencies(
      "target/classes/com/snowflake/snowpark/types/Variant.class",
      Seq("com.snowflake.snowpark.types.Variant"))

    checkDependencies(
      "target/classes/com/snowflake/snowpark/types/Variant$.class",
      Seq("com.snowflake.snowpark.types.Variant", "com.snowflake.snowpark.types.Geography"))
  }

  test("java variant") {
    checkDependencies(
      "target/classes/com/snowflake/snowpark_java/types/Variant.class",
      Seq(
        "com.snowflake.snowpark_java.types.Variant",
        "com.snowflake.snowpark_java.types.Geography"))
  }

  test("scala geography") {
    checkDependencies(
      "target/classes/com/snowflake/snowpark/types/Geography.class",
      Seq("com.snowflake.snowpark.types.Geography"))

    checkDependencies(
      "target/classes/com/snowflake/snowpark/types/Geography$.class",
      Seq("com.snowflake.snowpark.types.Geography"))
  }

  test("java geography") {
    checkDependencies(
      "target/classes/com/snowflake/snowpark_java/types/Geography.class",
      Seq("com.snowflake.snowpark_java.types.Geography"))
  }

  // negative test, to make sure this test method works
  test("session") {
    assertThrows[TestFailedException] {
      checkDependencies(
        "target/classes/com/snowflake/snowpark/Session.class",
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
