package com.snowflake.snowpark

import com.snowflake.snowpark.internal.{ParameterUtils, ServerConnection}
import net.snowflake.client.core.SFSessionProperty

import java.security.KeyPairGenerator
import java.security.spec.PKCS8EncodedKeySpec
import java.util.Base64

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
      sessionWithApplicationName.conn.connection.getSFBaseSession.getConnectionPropertiesMap
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

    val ex = intercept[SnowparkClientException] {
      ParameterUtils
        .jdbcConfig(optionWithoutKey + ("privatekey" -> "wrong key"), isScalaAPI = true)
    }
    assert(ex.message.contains("Failed to parse PKCS#8 RSA Private key"))
    assert(ex.message.contains("Failed to parse PKCS#1 RSA Private key"))
  }

  test("enable to read PKCS#8 private keys") {
    // no need to verify PKCS#1 format key additionally,
    // since all Github Action tests use PKCS#1 key to authenticate with Snowflake server.
    ParameterUtils.parsePrivateKey(generatePKCS8Key())
    succeed
  }

  private def generatePKCS8Key(): String = {
    val keyPairGenerator = KeyPairGenerator.getInstance("RSA")
    keyPairGenerator.initialize(2048)
    val keyPair = keyPairGenerator.generateKeyPair()
    val privateKey = keyPair.getPrivate
    val encodedKeySpec = new PKCS8EncodedKeySpec(privateKey.getEncoded)
    Base64.getEncoder.encodeToString(encodedKeySpec.getEncoded)
  }
}
