package com.snowflake.snowpark_test;

import com.snowflake.snowpark_java.Functions;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.sproc.JavaSProc0;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.udf.JavaUDF0;
import org.junit.Test;

public class JavaUDXOpenTelemetrySuite extends JavaUDXOpenTelemetryEnabled {

  public JavaUDXOpenTelemetrySuite() {}

  private boolean dependencyAdded = false;

  @Override
  public Session getSession() {
    Session session = super.getSession();
    if (!dependencyAdded) {
      dependencyAdded = true;
      addDepsToClassPath(session);
    }
    testSpanExporter.reset();
    return session;
  }

  @Test
  public void udf() {
    String className = "snow.snowpark.UDFRegistration";
    String className2 = "snow.snowpark.Functions";
    JavaUDF0 func = () -> 100;
    getSession().udf().registerTemporary(func, DataTypes.IntegerType);
    checkUdfSpan(className, "registerTemporary", "", "");
    String funcName = randomFunctionName();
    String funcName2 = randomFunctionName();
    getSession().udf().registerTemporary(funcName, func, DataTypes.IntegerType);
    checkUdfSpan(className, "registerTemporary", funcName, "");
    Functions.udf(func, DataTypes.IntegerType);
    checkUdfSpan(className2, "udf", "", "");

    String stageName = randomName();
    try {
      createStage(stageName, false);
      getSession().udf().registerPermanent(funcName2, func, DataTypes.IntegerType, stageName);
      checkUdfSpan(className, "registerPermanent", funcName2, stageName);
    } finally {
      dropStage(stageName);
      getSession().sql("drop function " + funcName2 + "(int, int)").collect();
    }
  }

  @Test
  public void udtf() {
    String className = "snow.snowpark.UDTFRegistration";
    getSession().udtf().registerTemporary(new MyJavaUDTFOf0());
    checkUdfSpan(className, "registerTemporary", "", "");
    String funcName = randomFunctionName();
    String funcName2 = randomFunctionName();
    getSession().udtf().registerTemporary(funcName, new MyJavaUDTFOf0());
    checkUdfSpan(className, "registerTemporary", funcName, "");

    String stageName = randomStageName();
    try {
      createStage(stageName, false);
      getSession().udtf().registerPermanent(funcName2, new MyJavaUDTFOf2(), stageName);
      checkUdfSpan(className, "registerPermanent", funcName2, stageName);
    } finally {
      getSession().sql("drop function " + funcName2 + "(int, int)").collect();
      dropStage(stageName);
    }
  }

  @Test
  public void sproc() {
    String className = "snow.snowpark.SProcRegistration";
    String spName = randomName();
    String spName2 = randomName();
    String stageName = randomStageName();
    JavaSProc0 sproc = session -> "SUCCESS";
    getSession().sproc().registerTemporary(sproc, DataTypes.StringType);
    checkUdfSpan(className, "registerTemporary", "", "");
    getSession().sproc().registerTemporary(spName2, sproc, DataTypes.StringType);
    checkUdfSpan(className, "registerTemporary", spName2, "");
    try {
      createStage(stageName, false);
      getSession().sproc().registerPermanent(spName, sproc, DataTypes.StringType, stageName, true);
      checkUdfSpan(className, "registerPermanent", spName, stageName);
    } finally {
      dropStage(stageName);
      getSession().sql("drop procedure " + spName + "()").collect();
    }
  }

  private void checkUdfSpan(
      String className, String funcName, String execName, String execFilePath) {
    StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    StackTraceElement file = stack[2];
    checkSpan(
        className,
        funcName,
        "JavaUDXOpenTelemetrySuite.java",
        file.getLineNumber() - 1,
        execName,
        "SnowUDF.compute",
        execFilePath);
  }
}
