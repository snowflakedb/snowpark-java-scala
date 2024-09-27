package com.snowflake.code_verification

import com.snowflake.snowpark.CodeVerification
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable

@CodeVerification
class PomSuite extends AnyFunSuite {

  private val pomFileName = "pom.xml"
  private val fipsPomFileName = "fips-pom.xml"
  private val javaDocPomFileName = "java_doc.xml"

  // todo: should be replaced by SBT
  ignore("project versions should be updated together") {
    assert(
      PomUtils.getProjectVersion(pomFileName) ==
        PomUtils.getProjectVersion(javaDocPomFileName))
    assert(
      PomUtils.getProjectVersion(pomFileName) ==
        PomUtils.getProjectVersion(fipsPomFileName))
    assert(
      PomUtils
        .getProjectVersion(pomFileName)
        .matches("\\d+\\.\\d+\\.\\d+(-SNAPSHOT)?"))
  }

  ignore("dependencies of pom and fips should be updated together") {
    val pomDependencies = PomUtils.getProductDependencies(pomFileName)
    val fipsDependencies = PomUtils.getProductDependencies(fipsPomFileName)

    val cache = mutable.Map(fipsDependencies.toSeq: _*)
    pomDependencies.foreach { case (id, version) =>
      val name = if (id == "snowflake-jdbc") "snowflake-jdbc-fips" else id
      assert(cache.keySet.contains(name))
      assert(version == cache(name))
      cache.remove(name)
    }
    assert(cache.isEmpty)
  }
}
