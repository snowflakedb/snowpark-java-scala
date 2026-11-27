package com.snowflake.snowpark.internal.jdbc4

import java.io.{File, FileInputStream}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.DriverManager
import java.util.Properties

import javax.xml.parsers.DocumentBuilderFactory

import com.snowflake.session.api.SnowflakeSessionLoader
import com.snowflake.snowpark.SnowparkClientException
import com.snowflake.snowpark.internal.jdbc.SnowflakeSessions
import org.scalatest.funsuite.AnyFunSuite

import scala.io.Source

// Runs on the production classpath of the regular client: Snowpark plus only the exact JDBC 4.x
// driver (`jdbcVersionExact` in build.sbt), whose real SnowflakeClientSessionFactory provides the
// client session. See jdbc4Compat in build.sbt.
class Jdbc4CompatSuite extends AnyFunSuite {
  private val loader = Class.forName("com.snowflake.snowpark.Session").getClassLoader
  private val expectedJar = sys.props("snowpark.expectedJdbcJar")
  private val expectedGav = sys.props("snowpark.expectedJdbcGav")

  private def location(c: Class[_]): String =
    c.getProtectionDomain.getCodeSource.getLocation.getPath

  private def sha256(file: File): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val in = new FileInputStream(file)
    try {
      val buf = new Array[Byte](8192)
      var n = in.read(buf)
      while (n >= 0) {
        digest.update(buf, 0, n)
        n = in.read(buf)
      }
    } finally in.close()
    digest.digest().map("%02x".format(_)).mkString
  }

  private def classpathFiles: Seq[File] =
    sys.props("java.class.path").split(File.pathSeparator).filter(_.nonEmpty).map(new File(_)).toSeq

  private def exactJar: File = {
    val jars = classpathFiles.filter(_.getName == expectedJar)
    assert(jars.size == 1, jars)
    jars.head
  }

  test("the classpath has only the exact JDBC 4.x driver jar") {
    assert(classpathFiles.map(_.getName).filter(_.startsWith("snowflake-jdbc")) == Seq(expectedJar))
    val driver = DriverManager.getDriver("jdbc:snowflake://example.snowflakecomputing.com")
    assert(location(driver.getClass).endsWith("/" + expectedJar))
    assert(driver.getMajorVersion == 4)
    // JDBC 3.x classes, used only by the legacy Jdbc3Session fallback, are absent
    intercept[ClassNotFoundException](
      Class.forName("net.snowflake.client.jdbc.SnowflakeConnectionV1"))
  }

  test("session API classes come from Snowpark, not from the driver jar") {
    assert(!location(classOf[SnowflakeSessionLoader]).contains("snowflake-jdbc"))
  }

  test("ServiceLoader discovers the client session factory provided by the JDBC jar") {
    val factory = SnowflakeSessionLoader.loadClientSessionFactory(loader)
    assert(factory.isPresent)
    assert(location(factory.get.getClass).endsWith("/" + expectedJar))
  }

  test("the JDBC client session factory is used instead of the JDBC 3.x fallback") {
    // The JDBC 3.x fallback needs classes that are absent here, so reaching the driver's
    // factory is the only way to get Snowpark's invalid-connect-string error.
    val e = intercept[SnowparkClientException] {
      SnowflakeSessions.createClientSession("not-a-snowflake-url", new Properties())
    }
    assert(e.errorCode == "0405", e.getMessage)
  }

  test("the exact-runtime resource matches jdbcVersionExact and the classpath driver jar") {
    val resource = sys.props("snowpark.exactRuntimeMetadataPath")
    val url = loader.getResource(resource)
    assert(url != null, s"$resource is not packaged with Snowpark")
    val props = new Properties()
    val in = url.openStream()
    try props.load(in)
    finally in.close()
    assert(props.getProperty("JDBC_GAV") == expectedGav)
    assert(props.getProperty("JDBC_SHA256") == sha256(exactJar))
  }

  test("the published POM depends on exactly the jdbcVersionExact driver") {
    val doc = DocumentBuilderFactory
      .newInstance()
      .newDocumentBuilder()
      .parse(new File(sys.props("snowpark.pomFile")))
    val dependencies = doc.getElementsByTagName("dependency")
    val snowflake = (0 until dependencies.getLength).map(dependencies.item).flatMap { node =>
      val children = node.getChildNodes
      val fields = (0 until children.getLength)
        .map(children.item)
        .map { c =>
          c.getNodeName -> c.getTextContent.trim
        }
        .toMap
      if (fields.get("groupId").contains("net.snowflake")) {
        Some(
          fields("groupId") + ":" + fields("artifactId") + ":" + fields("version") ->
            fields.getOrElse("scope", "compile"))
      } else None
    }
    // only the exact driver, with compile scope, and never the legacy 3.x one
    assert(snowflake == Seq(expectedGav -> "compile"))
    val pom = Source.fromFile(sys.props("snowpark.pomFile"), StandardCharsets.UTF_8.name).mkString
    assert(!pom.contains(sys.props("snowpark.legacyJdbcVersion")))
    assert(!pom.contains("jdbc3-legacy"))
  }
}
