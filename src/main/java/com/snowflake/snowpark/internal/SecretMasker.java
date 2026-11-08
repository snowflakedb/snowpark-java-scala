package com.snowflake.snowpark.internal;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Masks credentials in log messages. Mirrors the behavior of the Snowflake JDBC driver's {@code
 * SecretDetector.maskSecrets(String)}, so the output of masking does not depend on which driver
 * version is on the classpath.
 *
 * <p>The JDBC 3.x filters run first, unchanged, so everything JDBC 3.x masks is masked the same
 * way. The filters only JDBC 4.x has (session tokens containing ':', the SSE-C customer key header,
 * X-Amz-* query parameters and "qrmk") run afterwards and can only mask more.
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

  private static final Pattern CONNECTION_TOKEN_PATTERN =
      Pattern.compile(
          "(token|assertion content)" + "(['\"\\s:=]+)" + "([a-z0-9=/_\\-+]{8,})",
          Pattern.CASE_INSENSITIVE);

  // The filters below run after the JDBC 3.x ones, so a value may already start with the "****"
  // a JDBC 3.x filter left in it (e.g. "token=" inside "x-amz-security-token=").
  private static String valueOrMaskedRest(String valueClass, int minLength) {
    return "(\\*{4}" + valueClass + "*|" + valueClass + "{" + minLength + ",})";
  }

  // Session tokens look like "ver:<n>-hint:<n>-...", which CONNECTION_TOKEN_PATTERN stops at.
  private static final Pattern SESSION_TOKEN_PATTERN =
      Pattern.compile(
          "(token|assertion content)" + "(['\"\\s:=]+)" + "(ver:[0-9]+-[a-z0-9=/_\\-+:]+)",
          Pattern.CASE_INSENSITIVE);

  // X-Amz-Signature is covered by the "signature=" alternative in SAS_TOKEN_PATTERN.
  private static final Pattern SIGV4_PARAM_PATTERN =
      Pattern.compile(
          "(x-amz-credential|x-amz-security-token)(=)" + valueOrMaskedRest("[a-z0-9%/+=._-]", 8),
          Pattern.CASE_INSENSITIVE);

  private static final Pattern QRMK_PATTERN =
      Pattern.compile(
          "(qrmk)" + "([\'\"\\s:=]+)" + valueOrMaskedRest("[a-z0-9/+=]", 16),
          Pattern.CASE_INSENSITIVE);

  // The qrmk key also travels as the SSE-C customer key header, labelled by the header name.
  private static final Pattern SSE_C_CUSTOMER_KEY_PATTERN =
      Pattern.compile(
          "(x-amz-server-side-encryption-customer-key)"
              + "((?:\\s*,\\s*value)?[\'\"\\s:=]+)"
              + valueOrMaskedRest("[a-z0-9+/=]", 16),
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
    return maskJdbc4OnlySecrets(maskJdbc3Secrets(text));
  }

  // Same filters, patterns and order as the JDBC 3.x SecretDetector.maskSecrets.
  private static String maskJdbc3Secrets(String text) {
    return filterAccessTokens(
        filterConnectionTokens(
            filterPassword(
                filterSASTokens(
                    filterAWSKeys(filterOAuthTokens(filterEncryptionMaterial(text)))))));
  }

  private static String maskJdbc4OnlySecrets(String text) {
    String masked = text;
    for (Pattern pattern :
        new Pattern[] {
          SSE_C_CUSTOMER_KEY_PATTERN, SIGV4_PARAM_PATTERN, QRMK_PATTERN, SESSION_TOKEN_PATTERN
        }) {
      masked = replaceInPrefix(masked, pattern);
    }
    return masked;
  }

  // Unlike replaceIfFound, keeps the text past MAX_LENGTH, so masking more never shortens a
  // message.
  private static String replaceInPrefix(String text, Pattern pattern) {
    String head = prefix(text);
    String replaced = pattern.matcher(head).replaceAll("$1$2****");
    return head.length() == text.length() ? replaced : replaced + text.substring(MAX_LENGTH);
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
