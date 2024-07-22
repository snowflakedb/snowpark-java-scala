package com.snowflake.snowpark_test;

import com.snowflake.snowpark_java.DataFrame;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.UserDefinedFunction;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.udf.JavaUDF0;
import org.junit.Test;

public class JavaUDXOpenTelemetrySuite extends JavaOpenTelemetryEnabled {

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
        JavaUDF0 func = () -> 100;
        getSession().udf().registerTemporary(func, DataTypes.IntegerType);
        checkUdfSpan(className, "registerTemporary", "", "");
    }

    private void checkUdfSpan(
            String className,
            String funcName,
            String execName,
            String execFilePath
    ) {
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
