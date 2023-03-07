package com.snowflake.snowpark

import com.snowflake.snowpark.internal.{ParameterUtils, ServerConnection}
import net.snowflake.client.core.SFSessionProperty

class ParameterSuite extends SNTestBase {

  val options: Map[String, String] = Session.loadConfFromFile(defaultProfile)

  test("forward all parameter not in the parameter list") {
    val newOptions = options + ("abc" -> "123")
    val properties = ParameterUtils.jdbcConfig(newOptions, isScalaAPI = true)
    assert(properties.containsKey("abc"))
  }

  test("test set application name") {
    val application = SFSessionProperty.APPLICATION.getPropertyKey
    val applicationName = "user_app_name"
    val newOptions = options + (application -> applicationName)
    val properties = ParameterUtils.jdbcConfig(newOptions, isScalaAPI = true)
    assert(properties.get(application) == applicationName)

    val sessionWithApplicationName: Session =
      Session.builder
        .configFile(defaultProfile)
        .config(application, applicationName)
        .create

    assert(
      sessionWithApplicationName.conn.connection.getSfSession.getConnectionPropertiesMap
        .get(SFSessionProperty.APPLICATION) == applicationName)
  }

  test("url") {
    val optionWithoutUrl: Map[String, String] = options.filter {
      // scalastyle:off
      case (key, _) => key.toLowerCase != "url"
      // scalastyle:on
    }

    assertThrows[IllegalArgumentException](ServerConnection.connectionString(optionWithoutUrl))

    val expected = "jdbc:snowflake://localhost:443"
    val result1 = ServerConnection.connectionString(optionWithoutUrl + ("url" -> "localhost"))
    val result2 = ServerConnection.connectionString(optionWithoutUrl + ("url" -> "localhost:443"))

    assert(expected == result1)
    assert(result1 == result2)

    val expected1 = "jdbc:snowflake://localhost:8080"
    val result3 =
      ServerConnection.connectionString(optionWithoutUrl + ("url" -> "localhost:8080"))

    assert(expected1 == result3)
  }

  test("invalid rsa key") {
    val optionWithoutKey: Map[String, String] = options.filter {
      // scalastyle:off
      case (key, _) => key.toLowerCase != "privatekey"
      // scalastyle:on
    }

    assertThrows[Exception](
      ParameterUtils
        .jdbcConfig(optionWithoutKey + ("privatekey" -> "wrong key"), isScalaAPI = true))
  }
}
