package com.snowflake.snowpark_java.types;

/** Custom Snowflake class that provides access to snowflake secrets. */
public class SnowflakeSecrets {

  /**
   * Create a new instance of SnowflakeSecrets.
   *
   * @param secretName name of the secret object.
   */
  public static SnowflakeSecrets newInstance() {
    throw new UnsupportedOperationException();
  }

  /**
   * Get the type of secret. On success, it returns a valid token type string.
   *
   * @param secretName name of the secret object.
   */
  public String getSecretType(String secretName) {
    throw new UnsupportedOperationException();
  }

  /**
   * Get the user name and password from the secret. On success, it returns a valid object with user
   * name and password.
   *
   * @param secretName name of the secret object.
   */
  public UsernamePassword getUsernamePassword(String secretName) {
    throw new UnsupportedOperationException();
  }

  /**
   * Get the Cloud provider token from the secret. On success, it returns a valid object with access
   * key id, secret access key and token.
   *
   * @param secretName name of the secret object.
   */
  public CloudProviderToken getCloudProviderToken(String secretName) {
    throw new UnsupportedOperationException();
  }
  /**
   * Get the secret generic string of the secret. On success, it returns a valid token string.
   *
   * @param secretName name of the secret object.
   */
  public String getGenericSecretString(String secretName) {
    throw new UnsupportedOperationException();
  }

  /**
   * Get the OAuth2 Access Token of the secret. On success, it returns a valid OAuth2 token string.
   *
   * @param secretName name of the secret object.
   */
  public String getOAuthAccessToken(String secretName) {
    throw new UnsupportedOperationException();
  }
}
