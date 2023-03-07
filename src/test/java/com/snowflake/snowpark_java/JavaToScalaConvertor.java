package com.snowflake.snowpark_java;

public class JavaToScalaConvertor {
  public static com.snowflake.snowpark.Row javaToScalaRow(com.snowflake.snowpark_java.Row row) {
    return row.getScalaRow();
  }

  public static com.snowflake.snowpark.Session javaToScalaSession(
      com.snowflake.snowpark_java.Session session) {
    return session.getScalaSession();
  }
}
