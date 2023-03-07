package com.snowflake.code_verification

import scala.collection.mutable
import scala.language.postfixOps
import scala.xml.XML

object PomUtils {

  def getProjectVersion(pomFilePath: String): String = {
    val pom = XML.loadFile(pomFilePath)
    (pom \ "version") text
  }

  // result format: name -> version
  // only product dependencies, no test dependencies
  def getProductDependencies(pomFilePath: String): Map[String, String] = {
    val pom = XML.loadFile(pomFilePath)
    val result = new mutable.HashMap[String, String]()
    val dependencies = pom \ "dependencies" \ "dependency"
    // only product dependencies, no test dependencies
    val prodDep = dependencies.filter(node => (node \ "scope").text != "test")
    prodDep.foreach(node => {
      val id = (node \ "artifactId").text
      var version = (node \ "version").text

      // replace place holder by the real value.
      // for example, replace ${scala.version} by 2.12.11
      if (version.startsWith("$")) {
        val start = version.indexOf("{")
        val end = version.indexOf("}")
        version = (pom \\ version.substring(start + 1, end)).text
      }
      result.put(id, version)
    })
    // also add jdk as a dependency
    result.put("jdk", (pom \\ "maven.compiler.target").text)
    result.toMap
  }
}
