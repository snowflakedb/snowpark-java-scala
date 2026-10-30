package com.snowflake.snowpark.internal;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Masks credentials in log messages. Mirrors the behavior of the Snowflake JDBC driver's {@code
 * SecretDetector.maskSecrets(String)}, so the output of masking does not depend on which driver
 * version is on the classpath.
 *
 * <p>The patterns are the union of JDBC 3.x and JDBC 4.x. Where the two drivers differ, the more
 * restrictive behavior is used: the connection token value may contain ':' (JDBC 4.x only), and the
 * SSE-C customer key header, X-Amz-* query parameters and "qrmk" are masked (JDBC 4.x only).
 */
public final class SecretMasker {
  private SecretMasker() {}

  // only attempt to find secrets in its leading 100Kb
  private static final int MAX_LENGTH = 100 * 1000;

  private static final Pattern AWS_KEY_PATTERN =
      Pattern.compile(
          "(aws_key_id|aws_secret_key|access_key_id|secret_access_key)(\\s*=\\s*)'([^']+)'",
          Pattern.CASE_INSENSITIVE);

  // Tokens in serialized JSON
  private static final Pattern AWS_TOKEN_PATTERN =
      Pattern.compile(
          "(accessToken|tempToken|keySecret)\"\\s*:\\s*\"([a-z0-9/+]{32,}={0,2})\"",
          Pattern.CASE_INSENSITIVE);

  // OAuth tokens in serialized JSON
  private static final Pattern OAUTH_JSON_PATTERN =
      Pattern.compile(
          "(access_token|refresh_token)\""
              + "\\s*:\\s*"
              + "\"([a-zA-Z0-9!\"#$%&'\\()*+,-./:;<=>?@\\[\\]^_`\\{|\\}~]{3,})\"",
          Pattern.CASE_INSENSITIVE);

  // Signature in the query string of a URL in SAS based authentication
  private static final Pattern SAS_TOKEN_PATTERN =
      Pattern.compile(
          "(sig|signature|AWSAccessKeyId|password|passcode)=([a-z0-9%/+]{16,})",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern PASSWORD_PATTERN =
      Pattern.compile(
          "(password|passcode|pwd)"
              + "([\'\"\\s:=]+)"
              + "([a-z0-9!\"#$%&'\\()*+,-./:;<=>?@\\[\\]^_`\\{|\\}~]{6,})",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern PRIVATE_KEY_PATTERN =
      Pattern.compile(
          "-----BEGIN PRIVATE KEY-----" // #pragma: allowlist secret
              + "\\\\n([a-z0-9/+=\\\\n]{32,})\\\\n"
              + "-----END PRIVATE KEY-----",
          Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);

  private static final Pattern PRIVATE_KEY_DATA_PATTERN =
      Pattern.compile(
          "\"privateKeyData\": \"([a-z0-9/+=\\\\n]{10,})\"",
          Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);

  // ':' must stay in the value class: session tokens start with "ver:<n>-", and without ':' the
  // match stops after "ver", which is below the 8-character minimum.
  private static final Pattern CONNECTION_TOKEN_PATTERN =
      Pattern.compile(
          "(token|assertion content)" + "(['\"\\s:=]+)" + "([a-z0-9=/_\\-+:]{8,})",
          Pattern.CASE_INSENSITIVE);

  // X-Amz-Signature is covered by the "signature=" alternative in SAS_TOKEN_PATTERN.
  private static final Pattern SIGV4_PARAM_PATTERN =
      Pattern.compile(
          "(x-amz-credential|x-amz-security-token)=([a-z0-9%/+=._-]{8,})",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern QRMK_PATTERN =
      Pattern.compile("(qrmk)" + "([\'\"\\s:=]+)" + "([a-z0-9/+=]{16,})", Pattern.CASE_INSENSITIVE);

  // The qrmk key also travels as the SSE-C customer key header, labelled by the header name.
  private static final Pattern SSE_C_CUSTOMER_KEY_PATTERN =
      Pattern.compile(
          "(x-amz-server-side-encryption-customer-key)"
              + "((?:\\s*,\\s*value)?[\'\"\\s:=]+)"
              + "([a-z0-9+/]{16,}={0,2})",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern ENCRYPTION_MATERIAL_PATTERN =
      Pattern.compile("\"encryptionMaterial\"\\s*:\\s*\\{.*?\\}", Pattern.CASE_INSENSITIVE);

  /**
   * Masks any secrets present in the input string.
   *
   * @param text text which may contain secrets
   * @return the masked string, or null if the input is null
   */
  public static String maskSecrets(String text) {
    if (text == null) {
      return null;
    }
    // Filters with a specific literal label run before the ones keyed off a generic label
    // ("token", "password", "sig"), so a generic filter cannot consume part of a specific one.
    return filterAccessTokens(
        filterConnectionTokens(
            filterPassword(
                filterSASTokens(
                    filterAWSKeys(
                        filterOAuthTokens(
                            filterEncryptionMaterial(
                                filterQrmk(filterSigV4Params(filterSseCustomerKey(text))))))))));
  }

  private static String prefix(String text) {
    return text.length() <= MAX_LENGTH ? text : text.substring(0, MAX_LENGTH);
  }

  private static String replaceIfFound(String text, Pattern pattern, String replacement) {
    if (text == null) {
      return null;
    }
    Matcher matcher = pattern.matcher(prefix(text));
    if (matcher.find()) {
      return matcher.replaceAll(replacement);
    }
    return text;
  }

  private static String filterAWSKeys(String text) {
    return replaceIfFound(text, AWS_KEY_PATTERN, "$1$2'****'");
  }

  private static String filterSASTokens(String text) {
    return replaceIfFound(text, SAS_TOKEN_PATTERN, "$1=****");
  }

  private static String filterPassword(String text) {
    return replaceIfFound(text, PASSWORD_PATTERN, "$1$2**** ");
  }

  private static String filterConnectionTokens(String text) {
    return replaceIfFound(text, CONNECTION_TOKEN_PATTERN, "$1$2****");
  }

  private static String filterSigV4Params(String text) {
    return replaceIfFound(text, SIGV4_PARAM_PATTERN, "$1=****");
  }

  private static String filterQrmk(String text) {
    return replaceIfFound(text, QRMK_PATTERN, "$1$2****");
  }

  private static String filterSseCustomerKey(String text) {
    return replaceIfFound(text, SSE_C_CUSTOMER_KEY_PATTERN, "$1$2****");
  }

  private static String filterOAuthTokens(String text) {
    return replaceIfFound(text, OAUTH_JSON_PATTERN, "$1\":\"****\"");
  }

  private static String filterEncryptionMaterial(String text) {
    return replaceIfFound(text, ENCRYPTION_MATERIAL_PATTERN, "\"encryptionMaterial\" : ****");
  }

  // Unlike the other filters, this one does not truncate its input to MAX_LENGTH.
  private static String filterAccessTokens(String message) {
    Matcher awsMatcher = AWS_TOKEN_PATTERN.matcher(message);
    if (awsMatcher.find()) {
      message = awsMatcher.replaceAll("$1\":\"XXXX\"");
    }

    Matcher azureMatcher = SAS_TOKEN_PATTERN.matcher(message);
    if (azureMatcher.find()) {
      message = azureMatcher.replaceAll("$1=XXXX");
    }

    Matcher gcsMatcher = PRIVATE_KEY_PATTERN.matcher(message);
    if (gcsMatcher.find()) {
      message =
          gcsMatcher.replaceAll(
              "-----BEGIN PRIVATE KEY-----" // #pragma: allowlist secret
                  + "\\\\nXXXX\\\\n"
                  + "-----END PRIVATE KEY-----");
    }

    gcsMatcher = PRIVATE_KEY_DATA_PATTERN.matcher(message);
    if (gcsMatcher.find()) {
      message = gcsMatcher.replaceAll("\"privateKeyData\": \"XXXX\"");
    }

    return message;
  }
}
