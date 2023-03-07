package com.snowflake.snowpark_java;

import java.util.Optional;

/**
 * This reference to a Stored Procedure which can be created by {@code Session.sproc().register}
 * methods, and used in {@code Session.storedProcedure()} method.
 *
 * <p>For example:
 *
 * <pre>{@code
 * StoredProcedure sproc = session.sproc.registerTemporary(
 *   (Session session, Integer num) -> {
 *     int result = session.sql("select " + num).collect()[0].getInt(0);
 *     return result + 100;
 *   }, DataTypes.IntegerType, DataTypes.IntegerType
 * );
 * session.storedProcedure(sproc, 123).show();
 * }</pre>
 *
 * @since 1.8.0
 */
public class StoredProcedure {
  final com.snowflake.snowpark.StoredProcedure sp;

  StoredProcedure(com.snowflake.snowpark.StoredProcedure sp) {
    this.sp = sp;
  }

  /**
   * Get the name of the given stored procedure
   *
   * @return the name of the given stored procedure
   * @since 1.8.0
   */
  public Optional<String> getName() {
    if (sp.name().isDefined()) {
      return Optional.of(sp.name().get());
    } else {
      return Optional.empty();
    }
  }
}
