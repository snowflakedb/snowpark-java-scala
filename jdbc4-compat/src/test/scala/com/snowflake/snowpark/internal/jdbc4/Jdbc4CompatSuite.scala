package com.snowflake.snowpark.internal.jdbc4

import java.io.File
import java.sql.DriverManager
import java.util.Properties

import com.snowflake.session.api.SnowflakeSessionLoader
import com.snowflake.snowpark.internal.jdbc.SnowflakeSessions
import org.scalatest.funsuite.AnyFunSuite

// Runs on a classpath where only the JDBC 4.x driver is present (see jdbc4Compat in build.sbt).
//
// The published snowflake-jdbc 4.4.0 does not register a SnowflakeClientSessionFactory; the regular
// client exact path needs a later JDBC release that does. Until then the provider is a test-only
// one registered in this project's test resources. The Java stored-procedure exact path does not
// use the client factory.
class Jdbc4CompatSuite extends AnyFunSuite {
  private val loader = Class.forName("com.snowflake.snowpark.Session").getClassLoader

  private def location(c: Class[_]): String =
    c.getProtectionDomain.getCodeSource.getLocation.getPath

  test("the classpath has only the expected JDBC 4.x driver jar") {
    val expected = sys.props("snowpark.expectedJdbcJar")
    val drivers = sys.props("java.class.path").split(File.pathSeparator).map(new File(_).getName)
    assert(drivers.filter(_.startsWith("snowflake-jdbc")).toSeq == Seq(expected))
    val driver = DriverManager.getDriver("jdbc:snowflake://example.snowflakecomputing.com")
    assert(location(driver.getClass).endsWith("/" + expected))
    assert(driver.getMajorVersion == 4)
  }

  test("session API classes come from Snowpark, not from the driver jar") {
    assert(!location(classOf[SnowflakeSessionLoader]).contains("snowflake-jdbc"))
  }

  test("ServiceLoader discovers the client session factory on the classpath") {
    val factory = SnowflakeSessionLoader.loadClientSessionFactory(loader)
    assert(factory.isPresent)
    assert(factory.get.isInstanceOf[SentinelClientSessionFactory])
  }

  test("a client session factory on the classpath is used instead of the JDBC 3.x fallback") {
    // The fallback needs JDBC 3.x classes that are absent here and would fail differently.
    intercept[SentinelProviderCalled] {
      SnowflakeSessions.createClientSession(
        "jdbc:snowflake://example.snowflakecomputing.com",
        new Properties())
    }
  }
}
