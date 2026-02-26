package com.snowflake.snowpark_test;

import static org.junit.Assert.assertEquals;

import com.snowflake.snowpark.internal.JavaUtils;
import com.snowflake.snowpark_java.AggregateFunction;
import com.snowflake.snowpark_java.DataFrame;
import com.snowflake.snowpark_java.Functions;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.test.MyImmutableSumUDAF;
import com.snowflake.snowpark_java.test.MyLargeClosureUDAF;
import com.snowflake.snowpark_java.test.MySumUDAF;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.types.StructType;
import org.junit.Test;

public class JavaUDAFClientSuite extends UDFTestBase {
  @Test
  public void testJavaTemporaryUDAF() {
    udafTest(
        () -> {
          MySumUDAF udaf = new MySumUDAF();
          AggregateFunction mySum = getSession().udaf().registerTemporary("java_my_sum", udaf);

          DataFrame df =
              getSession()
                  .createDataFrame(
                      new Row[] {Row.create(1), Row.create(2), Row.create(3)},
                      StructType.create(new StructField("a", DataTypes.IntegerType)));

          Row[] result = df.select(mySum.apply(Functions.col("a"))).collect();
          assertEquals(1, result.length);
          assertEquals(6L, result[0].getLong(0));
        },
        getSession());
  }

  /**
   * Test that JavaUDAF works correctly with immutable state types. This verifies that the generated
   * wrapper code uses the return value of accumulate() and merge() rather than assuming in-place
   * mutation of the state object.
   */
  @Test
  public void testJavaUDAFWithImmutableState() {
    udafTest(
        () -> {
          AggregateFunction mySum =
              getSession().udaf().registerTemporary("java_immutable_sum", new MyImmutableSumUDAF());

          DataFrame df =
              getSession()
                  .createDataFrame(
                      new Row[] {Row.create(1), Row.create(2), Row.create(3)},
                      StructType.create(new StructField("a", DataTypes.IntegerType)));

          Row[] result = df.select(mySum.apply(Functions.col("a"))).collect();
          assertEquals(1, result.length);
          assertEquals(6L, result[0].getLong(0));
        },
        getSession());
  }

  /**
   * Test that JavaUDAF works when the serialized closure exceeds MAX_INLINE_CLOSURE_SIZE_BYTES
   * (8KB), triggering the file-based closure upload path.
   */
  @Test
  public void testJavaUDAFWithLargeClosure() {
    udafTest(
        () -> {
          MyLargeClosureUDAF udaf = new MyLargeClosureUDAF(1024);
          assert JavaUtils.serialize(udaf).length > 8192
              : "Serialized UDAF should exceed 8KB to test the large closure path";

          AggregateFunction mySum =
              getSession().udaf().registerTemporary("java_large_closure_sum", udaf);

          DataFrame df =
              getSession()
                  .createDataFrame(
                      new Row[] {Row.create(1), Row.create(2), Row.create(3)},
                      StructType.create(new StructField("a", DataTypes.IntegerType)));

          Row[] result = df.select(mySum.apply(Functions.col("a"))).collect();
          assertEquals(1, result.length);
          assertEquals(6L, result[0].getLong(0));
        },
        getSession());
  }
}
