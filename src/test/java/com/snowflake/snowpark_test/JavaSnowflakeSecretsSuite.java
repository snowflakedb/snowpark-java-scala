package com.snowflake.snowpark_test;

import com.snowflake.snowpark_java.types.SnowflakeSecrets;
import com.snowflake.snowpark_java.types.UsernamePassword;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JavaSnowflakeSecretsSuite {
  private static final String SCLS_SPCS_SECRET_ENV_NAME =
      "SNOWFLAKE_CONTAINER_SERVICES_SECRET_PATH_PREFIX";
  private Path tempDir;
  private String originalEnv;

  @Before
  public void setUp() throws IOException {
    tempDir = Files.createTempDirectory("test_scls_spcs_creds");
    originalEnv = System.getenv(SCLS_SPCS_SECRET_ENV_NAME);
    setEnv(SCLS_SPCS_SECRET_ENV_NAME, tempDir.toString());
  }

  @After
  public void tearDown() throws IOException {
    if (tempDir != null) {
      deleteDirectory(tempDir);
    }
    setEnv(SCLS_SPCS_SECRET_ENV_NAME, originalEnv);
  }

  @Test
  public void testSecretsMockSclsSpcs() throws IOException {
    // Create generic, OAuth, and password secrets
    createSecretFile("my_generic_secret", "secret_string", "my_secret_value");
    createSecretFile("my_oauth_secret", "access_token", "oauth_token_12345");

    Path passwordDir = tempDir.resolve("my_password_secret");
    Files.createDirectories(passwordDir);
    Files.write(passwordDir.resolve("username"), "test_username".getBytes());
    Files.write(passwordDir.resolve("password"), "test_password".getBytes());

    // Test getGenericSecretString, getOAuthAccessToken, and getUsernamePassword
    SnowflakeSecrets secrets = SnowflakeSecrets.newInstance();

    assert secrets.getGenericSecretString("my_generic_secret").equals("my_secret_value");
    assert secrets.getOAuthAccessToken("my_oauth_secret").equals("oauth_token_12345");

    UsernamePassword creds = secrets.getUsernamePassword("my_password_secret");
    assert creds != null;
    assert creds.getUsername().equals("test_username");
    assert creds.getPassword().equals("test_password");

    // Test getSecretType
    assert secrets.getSecretType("my_generic_secret").equals("GENERIC_STRING");
    assert secrets.getSecretType("my_oauth_secret").equals("OAUTH2");
    assert secrets.getSecretType("my_password_secret").equals("PASSWORD");
  }

  @Test
  public void testSecretsMockSclsSpcsErrorCases() throws IOException {
    Path edgeDir = tempDir.resolve("edge");
    Files.createDirectories(edgeDir);
    Files.write(edgeDir.resolve("secret_string"), "value\n".getBytes());
    Files.write(edgeDir.resolve(".DS_Store"), "garbage".getBytes());
    Files.write(edgeDir.resolve("unknown_file"), "extra".getBytes());

    Path emptyDir = tempDir.resolve("empty");
    Files.createDirectories(emptyDir);

    Path fileAsDir = tempDir.resolve("notdir");
    Files.write(fileAsDir, "fake".getBytes());

    SnowflakeSecrets secrets = SnowflakeSecrets.newInstance();

    assert secrets.getGenericSecretString("edge").equals("value");

    try {
      secrets.getSecretType("edge");
      assert false : "Expected IllegalArgumentException for unexpected files";
    } catch (IllegalArgumentException e) {
      assert e.getMessage().contains("Unknown secret type for");
    }

    try {
      secrets.getSecretType("nonexistent");
      assert false : "Expected IllegalArgumentException for nonexistent secret";
    } catch (IllegalArgumentException e) {
      assert e.getMessage().contains("does not exist or not authorized");
    }

    try {
      secrets.getGenericSecretString("empty");
      assert false : "Expected IllegalArgumentException for missing file";
    } catch (IllegalArgumentException e) {
      assert e.getMessage().contains("does not exist or not authorized");
    }

    try {
      secrets.getSecretType("empty");
      assert false : "Expected IllegalArgumentException for empty directory";
    } catch (IllegalArgumentException e) {
      assert e.getMessage().contains("does not exist or not authorized");
    }

    try {
      secrets.getSecretType("notdir");
      assert false : "Expected IllegalArgumentException for file instead of directory";
    } catch (IllegalArgumentException e) {
      assert e.getMessage().contains("does not exist or not authorized");
    }
  }

  /** Helper method to set environment variables for testing. */
  private void setEnv(String key, String value) {
    try {
      Map<String, String> env = System.getenv();
      Field field = env.getClass().getDeclaredField("m");
      field.setAccessible(true);
      Map<String, String> writeableEnv = (Map<String, String>) field.get(env);
      if (value == null) {
        writeableEnv.remove(key);
      } else {
        writeableEnv.put(key, value);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to set environment variable", e);
    }
  }

  /** Helper method to create a secret directory with a single file. */
  private void createSecretFile(String secretName, String fileName, String content)
      throws IOException {
    Path secretDir = tempDir.resolve(secretName);
    Files.createDirectories(secretDir);
    Files.write(secretDir.resolve(fileName), content.getBytes());
  }

  /** Helper method to recursively delete a directory and its contents. */
  private void deleteDirectory(Path directory) throws IOException {
    if (!Files.exists(directory)) {
      return;
    }

    File dir = directory.toFile();
    if (dir.isDirectory()) {
      File[] children = dir.listFiles();
      if (children != null) {
        for (File child : children) {
          deleteDirectory(child.toPath());
        }
      }
    }
    Files.delete(directory);
  }
}
