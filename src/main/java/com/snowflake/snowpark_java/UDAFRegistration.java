package com.snowflake.snowpark_java;

import com.snowflake.snowpark_java.udaf.JavaUDAF;

/**
 * Provides methods to register a UDAF (user-defined aggregate function) in the Snowflake database.
 * {@code Session.udaf()} returns an object of this class.
 */
public class UDAFRegistration {
  private final com.snowflake.snowpark.UDAFRegistration udafRegistration;

  UDAFRegistration(com.snowflake.snowpark.UDAFRegistration udafRegistration) {
    this.udafRegistration = udafRegistration;
  }

  /**
   * Registers a UDAF instance as an anonymous temporary Snowflake UDAF that you plan to use in the
   * session.
   *
   * @param udaf The UDAF instance to be registered
   * @return A UserDefinedFunction that represents the corresponding FUNCTION created in Snowflake
   */
  public UserDefinedFunction registerTemporary(JavaUDAF udaf) {
    return new UserDefinedFunction(udafRegistration.registerTemporary(udaf));
  }

  /**
   * Registers a UDAF instance as a temporary Snowflake UDAF that you plan to use in the session.
   *
   * @param funcName The name that you want to use to refer to the UDAF.
   * @param udaf The UDAF instance to be registered
   * @return A UserDefinedFunction that represents the corresponding FUNCTION created in Snowflake
   */
  public UserDefinedFunction registerTemporary(String funcName, JavaUDAF udaf) {
    return new UserDefinedFunction(udafRegistration.registerTemporary(funcName, udaf));
  }

  /**
   * Registers a UDAF instance as a Snowflake UDAF.
   *
   * @param funcName The name that you want to use to refer to the UDAF.
   * @param udaf The UDAF instance to be registered.
   * @param stageLocation Stage location where the JAR files for the UDAF and its dependencies
   *     should be uploaded
   * @return A UserDefinedFunction that represents the corresponding FUNCTION created in Snowflake
   */
  public UserDefinedFunction registerPermanent(
      String funcName, JavaUDAF udaf, String stageLocation) {
    return new UserDefinedFunction(
        udafRegistration.registerPermanent(funcName, udaf, stageLocation));
  }
}
