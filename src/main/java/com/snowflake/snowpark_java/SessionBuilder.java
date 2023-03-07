package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;
import java.util.Map;

/**
 * Provides methods to set configuration properties and create a {@code Session}.
 *
 * @see com.snowflake.snowpark_java.Session Session
 * @since 0.8.0
 */
public class SessionBuilder {

  private final com.snowflake.snowpark.Session.SessionBuilder builder;

  SessionBuilder() {
    builder = JavaUtils.session_setJavaAPI(com.snowflake.snowpark.Session.builder());
  }

  /**
   * Adds the configuration properties in the specified file to the {@code SessionBuilder}
   * configuration.
   *
   * @param path Path to the file containing the configuration properties.
   * @return This {@code SessionBuilder} object.
   * @since 0.8.0
   */
  public SessionBuilder configFile(String path) {
    builder.configFile(path);
    return this;
  }

  /**
   * Adds the specified configuration property and value to the SessionBuilder configuration.
   *
   * @param key Name of the configuration property.
   * @param value Value of the configuration property.
   * @return A reference of this SessionBuilder object
   * @since 1.1.0
   */
  public SessionBuilder config(String key, String value) {
    this.builder.config(key, value);
    return this;
  }

  /**
   * Adds the specified Map of configuration properties to the SessionBuilder configuration.
   *
   * <p>Note that calling this method overwrites any existing configuration properties that you have
   * already set in the SessionBuilder.
   *
   * @param configs A Java Map contains configurations
   * @return A reference of this SessionBuilder object
   * @since 1.1.0
   */
  public SessionBuilder configs(Map<String, String> configs) {
    this.builder.configs(configs);
    return this;
  }

  /**
   * Creates a new {@code Session}.
   *
   * @return A {@code Session} object
   * @since 0.8.0
   */
  public Session create() {
    // disable closure cleaner in Java session,
    // it only works with Scala UDF.
    this.builder.config("snowpark_enable_closure_cleaner", "never");
    return new Session(builder.create());
  }
}
