package com.snowflake.snowpark_test;

import static org.junit.Assert.assertEquals;

import com.snowflake.snowpark_java.DataFrame;
import com.snowflake.snowpark_java.Functions;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.UserDefinedFunction;
import com.snowflake.snowpark_java.test.MySumUDAF;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.types.StructType;
import org.junit.Test;

public class JavaUDAFClientSuite {
  @Test
  public void testJavaTemporaryUDAF() {
    Session session = Session.builder().configFile("profile.properties").create();
    try {
      session.sql("ALTER SESSION SET ENABLE_JAVA_UDAF = TRUE").collect();
      MySumUDAF udaf = new MySumUDAF();
      UserDefinedFunction mySum = session.udaf().registerTemporary("java_my_sum", udaf);

      DataFrame df =
          session.createDataFrame(
              new Row[] {Row.create(1), Row.create(2), Row.create(3)},
              StructType.create(new StructField("a", DataTypes.IntegerType)));

      Row[] result = df.select(mySum.apply(Functions.col("a"))).collect();
      assertEquals(1, result.length);
      assertEquals(6L, result[0].getLong(0));
    } finally {
      session.close();
    }
  }
}
