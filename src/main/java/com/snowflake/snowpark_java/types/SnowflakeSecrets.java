package com.snowflake.snowpark_java.types;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

/** @hidden Custom Snowflake class that provides access to snowflake secrets. */
public class SnowflakeSecrets {
  protected static final String SCLS_SPCS_SECRET_ENV_NAME = "SCLS_SPCS_SECRET_PATH";

  /** Check if running in SPCS environment. */
  private static boolean isSPCSEnvironment() {
    String basePath = System.getenv(SCLS_SPCS_SECRET_ENV_NAME);
    return basePath != null && !basePath.isEmpty();
  }

  protected SnowflakeSecrets() {}

  /** Create a new instance of SnowflakeSecrets. */
  public static SnowflakeSecrets newInstance() {
    if (isSPCSEnvironment()) {
      return new SnowflakeSpcsSecrets();
    }
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

/** SPCS-specific implementation of SnowflakeSecrets. */
class SnowflakeSpcsSecrets extends SnowflakeSecrets {

  SnowflakeSpcsSecrets() {
    super();
  }

  /** Get the base path for SPCS secrets from environment variable. */
  private String getSPCSBasePath() {
    String basePath = System.getenv(SCLS_SPCS_SECRET_ENV_NAME);
    if (basePath == null || basePath.isEmpty()) {
      throw new UnsupportedOperationException(
          "Secret API is only supported on Snowflake server and Spark Classic's SPCS container environments.");
    }
    return basePath;
  }

  /**
   * Get the directory path for a specific secret in SPCS environment.
   *
   * @param secretName name of the secret object.
   */
  private Path getSPCSSecretDir(String secretName) {
    String basePath = getSPCSBasePath();
    Path secretDir = Paths.get(basePath, secretName);
    if (!Files.exists(secretDir)) {
      throw new IllegalArgumentException("Secret directory not found: " + secretDir);
    }
    if (!Files.isDirectory(secretDir)) {
      throw new IllegalArgumentException("Secret path is not a directory: " + secretDir);
    }
    return secretDir;
  }

  /**
   * Read a specific secret file from SPCS filesystem.
   *
   * @param secretName name of the secret object.
   * @param filename name of the file within the secret directory.
   */
  private String readSPCSSecretFile(String secretName, String filename) {
    String basePath = getSPCSBasePath();
    Path secretPath = Paths.get(basePath, secretName, filename);
    if (!Files.exists(secretPath)) {
      throw new IllegalArgumentException("Secret file not found: " + secretPath);
    }
    if (!Files.isRegularFile(secretPath)) {
      throw new IllegalArgumentException("Secret path is not a file: " + secretPath);
    }

    try {
      String content = new String(Files.readAllBytes(secretPath));
      return content.replaceAll("[\r\n]+$", "");
    } catch (IOException e) {
      throw new RuntimeException("Failed to read secret file: " + secretPath, e);
    }
  }

  /**
   * Determine the type of a secret by examining files in its SPCS directory.
   *
   * @param secretName name of the secret object.
   */
  private String getSecretTypeFromSPCS(String secretName) {
    Path secretDir = getSPCSSecretDir(secretName);

    Set<String> files = new HashSet<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(secretDir)) {
      for (Path entry : stream) {
        if (Files.isRegularFile(entry)) {
          String fileName = entry.getFileName().toString();
          // Skip hidden files
          if (!fileName.startsWith(".")) {
            files.add(fileName.toUpperCase());
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to list secret directory: " + secretDir, e);
    }

    if (files.isEmpty()) {
      throw new IllegalArgumentException("No secret files found in directory: " + secretDir);
    }

    if (files.contains("USERNAME") && files.contains("PASSWORD") && files.size() == 2) {
      return "PASSWORD";
    }

    if (files.size() == 1) {
      String file = files.iterator().next();
      if ("SECRET_STRING".equals(file)) {
        return "GENERIC_STRING";
      } else if ("ACCESS_TOKEN".equals(file)) {
        return "OAUTH2";
      } else {
        throw new IllegalArgumentException(
            "Unknown secret file type '" + file + "' in directory: " + secretDir);
      }
    }

    throw new IllegalArgumentException(
        "Secret directory contains unexpected files: " + files + " in " + secretDir);
  }

  @Override
  public String getSecretType(String secretName) {
    return getSecretTypeFromSPCS(secretName);
  }

  @Override
  public UsernamePassword getUsernamePassword(String secretName) {
    String username = readSPCSSecretFile(secretName, "username");
    String password = readSPCSSecretFile(secretName, "password");
    return new UsernamePassword(username, password);
  }

  @Override
  public String getGenericSecretString(String secretName) {
    return readSPCSSecretFile(secretName, "secret_string");
  }

  @Override
  public String getOAuthAccessToken(String secretName) {
    return readSPCSSecretFile(secretName, "access_token");
  }
}
