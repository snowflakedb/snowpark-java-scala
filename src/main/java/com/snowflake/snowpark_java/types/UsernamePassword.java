package com.snowflake.snowpark_java.types;

/** Custom Snowflake class that provides access to username & password secret object. */
public class UsernamePassword {
  private final String username;
  private final String password;

  public UsernamePassword(String username, String password) {
    this.username = username;
    this.password = password;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }
}
