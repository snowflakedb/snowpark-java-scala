package com.snowflake.snowpark_java.types;

/** Custom Snowflake class that provides access to accessKeyId & secretAccessKey & token secret object. */
public class CloudProviderToken {
  private final String accessKeyId;
  private final String secretAccessKey;
  private final String token;

  public CloudProviderToken(String id, String key, String token) {
    this.accessKeyId = id;
    this.secretAccessKey = key;
    this.token = token;
  }

  public String getAccessKeyId() {
    return accessKeyId;
  }

  public String getSecretAccessKey() {
    return secretAccessKey;
  }

  public String getToken() {
    return token;
  }
}
