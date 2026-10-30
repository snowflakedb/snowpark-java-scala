package com.snowflake.snowpark.internal

import net.snowflake.client.util.SecretDetector
import org.scalatest.funsuite.AnyFunSuite

// Compares SecretMasker with the JDBC 3.x SecretDetector. Everything JDBC 3.x masks must be masked
// the same way. The behavior that only JDBC 4.x has is covered separately with fixed expectations.
class SecretMaskerSuite extends AnyFunSuite {

  private val privateKeyBody = "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7VJTUt9Us8cKj" // pragma: allowlist secret

  private val sameAsDriver: Seq[(String, String)] = Seq(
    "aws key id" ->
      ("create stage s url='s3://b/' credentials=(aws_key_id='AKIAIOSFODNN7EXAMPLE' " + // pragma: allowlist secret
        "aws_secret_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')"), // pragma: allowlist secret
    "aws access key id, upper case and spaces" ->
      "ACCESS_KEY_ID  =  'abc123' SECRET_ACCESS_KEY= 'def456'",
    "aws token json" ->
      """{"accessToken":"abcdefghijklmnopqrstuvwxyz0123456789ABCD==","other":1}""", // pragma: allowlist secret
    "aws temp token and key secret json" ->
      """{"tempToken": "abcdefghijklmnopqrstuvwxyz0123456789", "keySecret" : "0123456789abcdef0123456789abcdef+/="}""", // pragma: allowlist secret
    "oauth json" ->
      """{"access_token":"ey.abc-DEF_123","refresh_token": "r3fr3sh!#$%&'()*+,-./:;<=>?@[]^_`{|}~"}""",
    "sas token in url" ->
      "https://acct.blob.core.windows.net/c/f?sv=2020&sig=abcdefghijklmnop1234%2Fxyz%3D&se=2030",
    "signature and aws access key id in url" ->
      "https://x.s3.amazonaws.com/f?AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE&signature=abcdefghijklmnop1234", // pragma: allowlist secret
    "password in sas style" -> "jdbc:snowflake://a.snowflakecomputing.com?password=abcdefghijklmnopqr",
    "passcode in sas style" -> "passcode=0123456789abcdef0123",
    "password in connection string" ->
      "jdbc:snowflake://a.snowflakecomputing.com/?user=bob&password=Sup3rS3cret!&db=x",
    "password with quotes and colon" -> "password: 'pa55w0rd!@#' next",
    "pwd" -> "pwd=hunter22 and more",
    "password too short" -> "password=abc12",
    "passcode json" -> """{"passcode" : "123456789"}""",
    "connection token" -> "token: abcdefgh12345678 and more",
    "assertion content" -> "assertion content: abcdefgh12345678=/_-+ end",
    "token too short" -> "token=abc",
    "private key" -> ("-----BEGIN PRIVATE KEY-----\\n" + privateKeyBody + "\\n" + // pragma: allowlist secret
      "-----END PRIVATE KEY-----"),
    "private key with escaped newlines in body" ->
      ("{\"key\":\"-----BEGIN PRIVATE KEY-----\\n" + privateKeyBody + "\\n" + privateKeyBody + // pragma: allowlist secret
        "\\n-----END PRIVATE KEY-----\\n\"}"),
    "private key data" -> ("{\"privateKeyData\": \"" + privateKeyBody + "\"}"),
    "private key data too short" -> "{\"privateKeyData\": \"abc\"}",
    "encryption material" ->
      """{"a":1,"encryptionMaterial" : {"queryId":"x","smkId":1,"queryStageMasterKey":"k"},"b":2}""",
    "encryption material no spaces" -> """"encryptionMaterial":{"k":"v"} tail""",
    "multi line" -> "line1 password=secret123\nline2 AWS_KEY_ID='abc' line3 token=0123456789",
    "mixed secrets" ->
      ("copy into t from @s credentials=(aws_key_id='AKIA' aws_secret_key='sec') " +
        "password=abcdefghijklmnopq token: 1234567890 {\"accessToken\":\"" + "a" * 40 + "\"}"),
    "empty" -> "",
    "no secrets" -> "SELECT * FROM t WHERE a = 1 -- nothing to hide",
    "unicode" -> "pässwörd=geheim12345 password=пароль123456",
    "adjacent matches" -> "password=aaaaaa password=bbbbbb token=cccccccc token=dddddddd",
    "secret after 100000 chars" -> ("x" * 100001 + " password=secret123"),
    "secret at 100000 chars boundary" -> ("x" * 99990 + " password=secret123"),
    "secret before 100000 chars" -> ("password=secret123 " + "x" * 100010),
    "access token after 100000 chars" ->
      ("x" * 100001 + """ {"accessToken":"abcdefghijklmnopqrstuvwxyz0123456789ABCD"}""")) // pragma: allowlist secret

  sameAsDriver.foreach { case (name, input) =>
    test(s"same output as JDBC 3.x SecretDetector: $name") {
      val expected = SecretDetector.maskSecrets(input)
      assert(SecretMasker.maskSecrets(input) == expected)
    }
  }

  test("masked values are not in the output") {
    val masked = SecretMasker.maskSecrets(
      "password=Sup3rS3cret! aws_key_id='AKIAEXAMPLE' sig=abcdefghijklmnop1234 " +
        "token=0123456789abc")
    assert(masked == "password=****  aws_key_id='****' sig=**** token=****")
  }

  test("null is returned as null") {
    assert(SecretMasker.maskSecrets(null) == null)
    assert(SecretDetector.maskSecrets(null) == null)
  }

  test("session token containing ':' is masked as a whole (JDBC 4.x behavior)") {
    val token = "ver:2-hint:1234-did:5-abcdefghijklmnop"
    assert(SecretMasker.maskSecrets(s"token: $token end") == "token: **** end")
    assert(SecretMasker.maskSecrets(s"""{"token":"$token"}""") == """{"token":"****"}""")
  }

  test("SSE-C customer key header is masked (JDBC 4.x behavior)") {
    val value = "abcdefghijklmnopqrstuvwxyz0123456789ABCD==" // pragma: allowlist secret
    assert(
      SecretMasker.maskSecrets(s"x-amz-server-side-encryption-customer-key: $value") ==
        "x-amz-server-side-encryption-customer-key: ****")
    assert(
      SecretMasker.maskSecrets(
        s"header name: x-amz-server-side-encryption-customer-key, value: $value") ==
        "header name: x-amz-server-side-encryption-customer-key, value: ****")
  }

  test(
    "X-Amz-Credential and X-Amz-Security-Token query parameters are masked (JDBC 4.x behavior)") {
    assert(
      SecretMasker.maskSecrets(
        "https://h/f?X-Amz-Credential=AKIA%2F20200101%2Fus-east-1&X-Amz-Date=1") ==
        "https://h/f?X-Amz-Credential=****&X-Amz-Date=1")
    assert(
      SecretMasker.maskSecrets("https://h/f?x-amz-security-token=FwoGZXIvYXdz.abc_def-ghi&a=b") ==
        "https://h/f?x-amz-security-token=****&a=b")
  }

  test("qrmk is masked (JDBC 4.x behavior)") {
    assert(SecretMasker.maskSecrets("qrmk: abcdefghijklmnop1234==") == "qrmk: ****")
    assert(SecretMasker.maskSecrets("\"qrmk\":\"abcdefghijklmnop1234=\"") == "\"qrmk\":\"****\"")
  }

  test("Logging.maskSecrets uses SecretMasker") {
    assert(Logging.maskSecrets(null) == null)
    assert(Logging.maskSecrets("password=secret123") == "password=**** ")
  }
}
