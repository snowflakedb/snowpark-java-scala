package com.snowflake.snowpark_test;

import static com.snowflake.snowpark_java.Functions.*;
import static org.junit.Assert.assertEquals;

import com.snowflake.snowpark_java.AggregateFunction;
import com.snowflake.snowpark_java.DataFrame;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.*;
import com.snowflake.snowpark_java.udaf.*;
import org.junit.Test;

/** Test suite for JavaUDAF2-22 with multiple arguments. Each UDAF sums all its input arguments. */
public class JavaUDAFMultiArgSuite extends UDFTestBase {
  private boolean dependencyAdded = false;

  @Override
  public Session getSession() {
    Session session = super.getSession();
    if (!dependencyAdded) {
      dependencyAdded = true;
      addDepsToClassPath(session);
      session.sql("ALTER SESSION SET ENABLE_JAVA_UDAF = TRUE").collect();
    }
    return session;
  }

  private DataFrame createTestData() {
    return getSession()
        .createDataFrame(
            new Row[] {Row.create(1), Row.create(2), Row.create(3)},
            StructType.create(new StructField("a", DataTypes.IntegerType)));
  }

  @Test
  public void testJavaUDAFOf2() {
    AggregateFunction udaf = getSession().udaf().registerTemporary(new MyJavaUDAFOf2());
    // sum of (1+2) + (2+3) + (3+4) = 3 + 5 + 7 = 15
    DataFrame df = createTestData();
    Row[] rows = df.select(udaf.apply(col("a"), col("a").plus(lit(1)))).collect();
    assertEquals(1, rows.length);
    assertEquals(15L, rows[0].getLong(0));
  }

  @Test
  public void testJavaUDAFOf3() {
    AggregateFunction udaf = getSession().udaf().registerTemporary(new MyJavaUDAFOf3());
    // sum of (1+2+3) + (2+3+4) + (3+4+5) = 6 + 9 + 12 = 27
    DataFrame df = createTestData();
    Row[] rows =
        df.select(udaf.apply(col("a"), col("a").plus(lit(1)), col("a").plus(lit(2)))).collect();
    assertEquals(1, rows.length);
    assertEquals(27L, rows[0].getLong(0));
  }

  @Test
  public void testJavaUDAFOf4() {
    AggregateFunction udaf = getSession().udaf().registerTemporary(new MyJavaUDAFOf4());
    DataFrame df = createTestData();
    Row[] rows =
        df.select(
                udaf.apply(
                    col("a"), col("a").plus(lit(1)), col("a").plus(lit(2)), col("a").plus(lit(3))))
            .collect();
    assertEquals(1, rows.length);
    assertEquals(42L, rows[0].getLong(0));
  }

  @Test
  public void testJavaUDAFOf5() {
    AggregateFunction udaf = getSession().udaf().registerTemporary(new MyJavaUDAFOf5());
    DataFrame df = createTestData();
    Row[] rows =
        df.select(
                udaf.apply(
                    col("a"),
                    col("a").plus(lit(1)),
                    col("a").plus(lit(2)),
                    col("a").plus(lit(3)),
                    col("a").plus(lit(4))))
            .collect();
    assertEquals(1, rows.length);
    assertEquals(60L, rows[0].getLong(0));
  }

  @Test
  public void testJavaUDAFOf6() {
    AggregateFunction udaf = getSession().udaf().registerTemporary(new MyJavaUDAFOf6());
    DataFrame df = createTestData();
    Row[] rows =
        df.select(
                udaf.apply(
                    col("a"),
                    col("a").plus(lit(1)),
                    col("a").plus(lit(2)),
                    col("a").plus(lit(3)),
                    col("a").plus(lit(4)),
                    col("a").plus(lit(5))))
            .collect();
    assertEquals(1, rows.length);
    assertEquals(81L, rows[0].getLong(0));
  }

  @Test
  public void testJavaUDAFOf7() {
    AggregateFunction udaf = getSession().udaf().registerTemporary(new MyJavaUDAFOf7());
    DataFrame df = createTestData();
    Row[] rows =
        df.select(
                udaf.apply(
                    col("a"),
                    col("a").plus(lit(1)),
                    col("a").plus(lit(2)),
                    col("a").plus(lit(3)),
                    col("a").plus(lit(4)),
                    col("a").plus(lit(5)),
                    col("a").plus(lit(6))))
            .collect();
    assertEquals(1, rows.length);
    assertEquals(105L, rows[0].getLong(0));
  }

  @Test
  public void testJavaUDAFOf8() {
    AggregateFunction udaf = getSession().udaf().registerTemporary(new MyJavaUDAFOf8());
    DataFrame df = createTestData();
    Row[] rows =
        df.select(
                udaf.apply(
                    col("a"),
                    col("a").plus(lit(1)),
                    col("a").plus(lit(2)),
                    col("a").plus(lit(3)),
                    col("a").plus(lit(4)),
                    col("a").plus(lit(5)),
                    col("a").plus(lit(6)),
                    col("a").plus(lit(7))))
            .collect();
    assertEquals(1, rows.length);
    assertEquals(132L, rows[0].getLong(0));
  }

  @Test
  public void testJavaUDAFOf9() {
    AggregateFunction udaf = getSession().udaf().registerTemporary(new MyJavaUDAFOf9());
    DataFrame df = createTestData();
    Row[] rows =
        df.select(
                udaf.apply(
                    col("a"),
                    col("a").plus(lit(1)),
                    col("a").plus(lit(2)),
                    col("a").plus(lit(3)),
                    col("a").plus(lit(4)),
                    col("a").plus(lit(5)),
                    col("a").plus(lit(6)),
                    col("a").plus(lit(7)),
                    col("a").plus(lit(8))))
            .collect();
    assertEquals(1, rows.length);
    assertEquals(162L, rows[0].getLong(0));
  }

  @Test
  public void testJavaUDAFOf10() {
    AggregateFunction udaf = getSession().udaf().registerTemporary(new MyJavaUDAFOf10());
    DataFrame df = createTestData();
    Row[] rows =
        df.select(
                udaf.apply(
                    col("a"),
                    col("a").plus(lit(1)),
                    col("a").plus(lit(2)),
                    col("a").plus(lit(3)),
                    col("a").plus(lit(4)),
                    col("a").plus(lit(5)),
                    col("a").plus(lit(6)),
                    col("a").plus(lit(7)),
                    col("a").plus(lit(8)),
                    col("a").plus(lit(9))))
            .collect();
    assertEquals(1, rows.length);
    assertEquals(195L, rows[0].getLong(0));
  }

  @Test
  public void testJavaUDAFOf11() {
    AggregateFunction udaf = getSession().udaf().registerTemporary(new MyJavaUDAFOf11());
    DataFrame df = createTestData();
    Row[] rows =
        df.select(
                udaf.apply(
                    col("a"),
                    col("a").plus(lit(1)),
                    col("a").plus(lit(2)),
                    col("a").plus(lit(3)),
                    col("a").plus(lit(4)),
                    col("a").plus(lit(5)),
                    col("a").plus(lit(6)),
                    col("a").plus(lit(7)),
                    col("a").plus(lit(8)),
                    col("a").plus(lit(9)),
                    col("a").plus(lit(10))))
            .collect();
    assertEquals(1, rows.length);
    assertEquals(231L, rows[0].getLong(0));
  }

  @Test
  public void testJavaUDAFOf12() {
    AggregateFunction udaf = getSession().udaf().registerTemporary(new MyJavaUDAFOf12());
    DataFrame df = createTestData();
    Row[] rows =
        df.select(
                udaf.apply(
                    col("a"),
                    col("a").plus(lit(1)),
                    col("a").plus(lit(2)),
                    col("a").plus(lit(3)),
                    col("a").plus(lit(4)),
                    col("a").plus(lit(5)),
                    col("a").plus(lit(6)),
                    col("a").plus(lit(7)),
                    col("a").plus(lit(8)),
                    col("a").plus(lit(9)),
                    col("a").plus(lit(10)),
                    col("a").plus(lit(11))))
            .collect();
    assertEquals(1, rows.length);
    assertEquals(270L, rows[0].getLong(0));
  }

  @Test
  public void testJavaUDAFOf13() {
    AggregateFunction udaf = getSession().udaf().registerTemporary(new MyJavaUDAFOf13());
    DataFrame df = createTestData();
    Row[] rows =
        df.select(
                udaf.apply(
                    col("a"),
                    col("a").plus(lit(1)),
                    col("a").plus(lit(2)),
                    col("a").plus(lit(3)),
                    col("a").plus(lit(4)),
                    col("a").plus(lit(5)),
                    col("a").plus(lit(6)),
                    col("a").plus(lit(7)),
                    col("a").plus(lit(8)),
                    col("a").plus(lit(9)),
                    col("a").plus(lit(10)),
                    col("a").plus(lit(11)),
                    col("a").plus(lit(12))))
            .collect();
    assertEquals(1, rows.length);
    assertEquals(312L, rows[0].getLong(0));
  }

  @Test
  public void testJavaUDAFOf14() {
    AggregateFunction udaf = getSession().udaf().registerTemporary(new MyJavaUDAFOf14());
    DataFrame df = createTestData();
    Row[] rows =
        df.select(
                udaf.apply(
                    col("a"),
                    col("a").plus(lit(1)),
                    col("a").plus(lit(2)),
                    col("a").plus(lit(3)),
                    col("a").plus(lit(4)),
                    col("a").plus(lit(5)),
                    col("a").plus(lit(6)),
                    col("a").plus(lit(7)),
                    col("a").plus(lit(8)),
                    col("a").plus(lit(9)),
                    col("a").plus(lit(10)),
                    col("a").plus(lit(11)),
                    col("a").plus(lit(12)),
                    col("a").plus(lit(13))))
            .collect();
    assertEquals(1, rows.length);
    assertEquals(357L, rows[0].getLong(0));
  }

  @Test
  public void testJavaUDAFOf15() {
    AggregateFunction udaf = getSession().udaf().registerTemporary(new MyJavaUDAFOf15());
    DataFrame df = createTestData();
    Row[] rows =
        df.select(
                udaf.apply(
                    col("a"),
                    col("a").plus(lit(1)),
                    col("a").plus(lit(2)),
                    col("a").plus(lit(3)),
                    col("a").plus(lit(4)),
                    col("a").plus(lit(5)),
                    col("a").plus(lit(6)),
                    col("a").plus(lit(7)),
                    col("a").plus(lit(8)),
                    col("a").plus(lit(9)),
                    col("a").plus(lit(10)),
                    col("a").plus(lit(11)),
                    col("a").plus(lit(12)),
                    col("a").plus(lit(13)),
                    col("a").plus(lit(14))))
            .collect();
    assertEquals(1, rows.length);
    assertEquals(405L, rows[0].getLong(0));
  }

  @Test
  public void testJavaUDAFOf16() {
    AggregateFunction udaf = getSession().udaf().registerTemporary(new MyJavaUDAFOf16());
    DataFrame df = createTestData();
    Row[] rows =
        df.select(
                udaf.apply(
                    col("a"),
                    col("a").plus(lit(1)),
                    col("a").plus(lit(2)),
                    col("a").plus(lit(3)),
                    col("a").plus(lit(4)),
                    col("a").plus(lit(5)),
                    col("a").plus(lit(6)),
                    col("a").plus(lit(7)),
                    col("a").plus(lit(8)),
                    col("a").plus(lit(9)),
                    col("a").plus(lit(10)),
                    col("a").plus(lit(11)),
                    col("a").plus(lit(12)),
                    col("a").plus(lit(13)),
                    col("a").plus(lit(14)),
                    col("a").plus(lit(15))))
            .collect();
    assertEquals(1, rows.length);
    assertEquals(456L, rows[0].getLong(0));
  }

  @Test
  public void testJavaUDAFOf17() {
    AggregateFunction udaf = getSession().udaf().registerTemporary(new MyJavaUDAFOf17());
    DataFrame df = createTestData();
    Row[] rows =
        df.select(
                udaf.apply(
                    col("a"),
                    col("a").plus(lit(1)),
                    col("a").plus(lit(2)),
                    col("a").plus(lit(3)),
                    col("a").plus(lit(4)),
                    col("a").plus(lit(5)),
                    col("a").plus(lit(6)),
                    col("a").plus(lit(7)),
                    col("a").plus(lit(8)),
                    col("a").plus(lit(9)),
                    col("a").plus(lit(10)),
                    col("a").plus(lit(11)),
                    col("a").plus(lit(12)),
                    col("a").plus(lit(13)),
                    col("a").plus(lit(14)),
                    col("a").plus(lit(15)),
                    col("a").plus(lit(16))))
            .collect();
    assertEquals(1, rows.length);
    assertEquals(510L, rows[0].getLong(0));
  }

  @Test
  public void testJavaUDAFOf18() {
    AggregateFunction udaf = getSession().udaf().registerTemporary(new MyJavaUDAFOf18());
    DataFrame df = createTestData();
    Row[] rows =
        df.select(
                udaf.apply(
                    col("a"),
                    col("a").plus(lit(1)),
                    col("a").plus(lit(2)),
                    col("a").plus(lit(3)),
                    col("a").plus(lit(4)),
                    col("a").plus(lit(5)),
                    col("a").plus(lit(6)),
                    col("a").plus(lit(7)),
                    col("a").plus(lit(8)),
                    col("a").plus(lit(9)),
                    col("a").plus(lit(10)),
                    col("a").plus(lit(11)),
                    col("a").plus(lit(12)),
                    col("a").plus(lit(13)),
                    col("a").plus(lit(14)),
                    col("a").plus(lit(15)),
                    col("a").plus(lit(16)),
                    col("a").plus(lit(17))))
            .collect();
    assertEquals(1, rows.length);
    assertEquals(567L, rows[0].getLong(0));
  }

  @Test
  public void testJavaUDAFOf19() {
    AggregateFunction udaf = getSession().udaf().registerTemporary(new MyJavaUDAFOf19());
    DataFrame df = createTestData();
    Row[] rows =
        df.select(
                udaf.apply(
                    col("a"),
                    col("a").plus(lit(1)),
                    col("a").plus(lit(2)),
                    col("a").plus(lit(3)),
                    col("a").plus(lit(4)),
                    col("a").plus(lit(5)),
                    col("a").plus(lit(6)),
                    col("a").plus(lit(7)),
                    col("a").plus(lit(8)),
                    col("a").plus(lit(9)),
                    col("a").plus(lit(10)),
                    col("a").plus(lit(11)),
                    col("a").plus(lit(12)),
                    col("a").plus(lit(13)),
                    col("a").plus(lit(14)),
                    col("a").plus(lit(15)),
                    col("a").plus(lit(16)),
                    col("a").plus(lit(17)),
                    col("a").plus(lit(18))))
            .collect();
    assertEquals(1, rows.length);
    assertEquals(627L, rows[0].getLong(0));
  }

  @Test
  public void testJavaUDAFOf20() {
    AggregateFunction udaf = getSession().udaf().registerTemporary(new MyJavaUDAFOf20());
    DataFrame df = createTestData();
    Row[] rows =
        df.select(
                udaf.apply(
                    col("a"),
                    col("a").plus(lit(1)),
                    col("a").plus(lit(2)),
                    col("a").plus(lit(3)),
                    col("a").plus(lit(4)),
                    col("a").plus(lit(5)),
                    col("a").plus(lit(6)),
                    col("a").plus(lit(7)),
                    col("a").plus(lit(8)),
                    col("a").plus(lit(9)),
                    col("a").plus(lit(10)),
                    col("a").plus(lit(11)),
                    col("a").plus(lit(12)),
                    col("a").plus(lit(13)),
                    col("a").plus(lit(14)),
                    col("a").plus(lit(15)),
                    col("a").plus(lit(16)),
                    col("a").plus(lit(17)),
                    col("a").plus(lit(18)),
                    col("a").plus(lit(19))))
            .collect();
    assertEquals(1, rows.length);
    assertEquals(690L, rows[0].getLong(0));
  }

  @Test
  public void testJavaUDAFOf21() {
    AggregateFunction udaf = getSession().udaf().registerTemporary(new MyJavaUDAFOf21());
    DataFrame df = createTestData();
    Row[] rows =
        df.select(
                udaf.apply(
                    col("a"),
                    col("a").plus(lit(1)),
                    col("a").plus(lit(2)),
                    col("a").plus(lit(3)),
                    col("a").plus(lit(4)),
                    col("a").plus(lit(5)),
                    col("a").plus(lit(6)),
                    col("a").plus(lit(7)),
                    col("a").plus(lit(8)),
                    col("a").plus(lit(9)),
                    col("a").plus(lit(10)),
                    col("a").plus(lit(11)),
                    col("a").plus(lit(12)),
                    col("a").plus(lit(13)),
                    col("a").plus(lit(14)),
                    col("a").plus(lit(15)),
                    col("a").plus(lit(16)),
                    col("a").plus(lit(17)),
                    col("a").plus(lit(18)),
                    col("a").plus(lit(19)),
                    col("a").plus(lit(20))))
            .collect();
    assertEquals(1, rows.length);
    assertEquals(756L, rows[0].getLong(0));
  }

  @Test
  public void testJavaUDAFOf22() {
    AggregateFunction udaf = getSession().udaf().registerTemporary(new MyJavaUDAFOf22());
    DataFrame df = createTestData();
    Row[] rows =
        df.select(
                udaf.apply(
                    col("a"),
                    col("a").plus(lit(1)),
                    col("a").plus(lit(2)),
                    col("a").plus(lit(3)),
                    col("a").plus(lit(4)),
                    col("a").plus(lit(5)),
                    col("a").plus(lit(6)),
                    col("a").plus(lit(7)),
                    col("a").plus(lit(8)),
                    col("a").plus(lit(9)),
                    col("a").plus(lit(10)),
                    col("a").plus(lit(11)),
                    col("a").plus(lit(12)),
                    col("a").plus(lit(13)),
                    col("a").plus(lit(14)),
                    col("a").plus(lit(15)),
                    col("a").plus(lit(16)),
                    col("a").plus(lit(17)),
                    col("a").plus(lit(18)),
                    col("a").plus(lit(19)),
                    col("a").plus(lit(20)),
                    col("a").plus(lit(21))))
            .collect();
    assertEquals(1, rows.length);
    assertEquals(825L, rows[0].getLong(0));
  }
}

// ==================== UDAF Implementations ====================

class MyJavaUDAFOf2 implements JavaUDAF2<long[], Long, Integer, Integer> {
  @Override
  public DataType outputType() {
    return DataTypes.LongType;
  }

  @Override
  public long[] initialize() {
    return new long[] {0L};
  }

  @Override
  public long[] accumulate(long[] state, Integer i0, Integer i1) {
    if (i0 != null) state[0] += i0;
    if (i1 != null) state[0] += i1;
    return state;
  }

  @Override
  public long[] merge(long[] state1, long[] state2) {
    state1[0] += state2[0];
    return state1;
  }

  @Override
  public Long terminate(long[] state) {
    return state[0];
  }
}

class MyJavaUDAFOf3 implements JavaUDAF3<long[], Long, Integer, Integer, Integer> {
  @Override
  public DataType outputType() {
    return DataTypes.LongType;
  }

  @Override
  public long[] initialize() {
    return new long[] {0L};
  }

  @Override
  public long[] accumulate(long[] state, Integer i0, Integer i1, Integer i2) {
    if (i0 != null) state[0] += i0;
    if (i1 != null) state[0] += i1;
    if (i2 != null) state[0] += i2;
    return state;
  }

  @Override
  public long[] merge(long[] state1, long[] state2) {
    state1[0] += state2[0];
    return state1;
  }

  @Override
  public Long terminate(long[] state) {
    return state[0];
  }
}

class MyJavaUDAFOf4 implements JavaUDAF4<long[], Long, Integer, Integer, Integer, Integer> {
  @Override
  public DataType outputType() {
    return DataTypes.LongType;
  }

  @Override
  public long[] initialize() {
    return new long[] {0L};
  }

  @Override
  public long[] accumulate(long[] state, Integer i0, Integer i1, Integer i2, Integer i3) {
    if (i0 != null) state[0] += i0;
    if (i1 != null) state[0] += i1;
    if (i2 != null) state[0] += i2;
    if (i3 != null) state[0] += i3;
    return state;
  }

  @Override
  public long[] merge(long[] state1, long[] state2) {
    state1[0] += state2[0];
    return state1;
  }

  @Override
  public Long terminate(long[] state) {
    return state[0];
  }
}

class MyJavaUDAFOf5
    implements JavaUDAF5<long[], Long, Integer, Integer, Integer, Integer, Integer> {
  @Override
  public DataType outputType() {
    return DataTypes.LongType;
  }

  @Override
  public long[] initialize() {
    return new long[] {0L};
  }

  @Override
  public long[] accumulate(
      long[] state, Integer i0, Integer i1, Integer i2, Integer i3, Integer i4) {
    if (i0 != null) state[0] += i0;
    if (i1 != null) state[0] += i1;
    if (i2 != null) state[0] += i2;
    if (i3 != null) state[0] += i3;
    if (i4 != null) state[0] += i4;
    return state;
  }

  @Override
  public long[] merge(long[] state1, long[] state2) {
    state1[0] += state2[0];
    return state1;
  }

  @Override
  public Long terminate(long[] state) {
    return state[0];
  }
}

class MyJavaUDAFOf6
    implements JavaUDAF6<long[], Long, Integer, Integer, Integer, Integer, Integer, Integer> {
  @Override
  public DataType outputType() {
    return DataTypes.LongType;
  }

  @Override
  public long[] initialize() {
    return new long[] {0L};
  }

  @Override
  public long[] accumulate(
      long[] state, Integer i0, Integer i1, Integer i2, Integer i3, Integer i4, Integer i5) {
    if (i0 != null) state[0] += i0;
    if (i1 != null) state[0] += i1;
    if (i2 != null) state[0] += i2;
    if (i3 != null) state[0] += i3;
    if (i4 != null) state[0] += i4;
    if (i5 != null) state[0] += i5;
    return state;
  }

  @Override
  public long[] merge(long[] state1, long[] state2) {
    state1[0] += state2[0];
    return state1;
  }

  @Override
  public Long terminate(long[] state) {
    return state[0];
  }
}

class MyJavaUDAFOf7
    implements JavaUDAF7<
        long[], Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer> {
  @Override
  public DataType outputType() {
    return DataTypes.LongType;
  }

  @Override
  public long[] initialize() {
    return new long[] {0L};
  }

  @Override
  public long[] accumulate(
      long[] state,
      Integer i0,
      Integer i1,
      Integer i2,
      Integer i3,
      Integer i4,
      Integer i5,
      Integer i6) {
    if (i0 != null) state[0] += i0;
    if (i1 != null) state[0] += i1;
    if (i2 != null) state[0] += i2;
    if (i3 != null) state[0] += i3;
    if (i4 != null) state[0] += i4;
    if (i5 != null) state[0] += i5;
    if (i6 != null) state[0] += i6;
    return state;
  }

  @Override
  public long[] merge(long[] state1, long[] state2) {
    state1[0] += state2[0];
    return state1;
  }

  @Override
  public Long terminate(long[] state) {
    return state[0];
  }
}

class MyJavaUDAFOf8
    implements JavaUDAF8<
        long[], Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> {
  @Override
  public DataType outputType() {
    return DataTypes.LongType;
  }

  @Override
  public long[] initialize() {
    return new long[] {0L};
  }

  @Override
  public long[] accumulate(
      long[] state,
      Integer i0,
      Integer i1,
      Integer i2,
      Integer i3,
      Integer i4,
      Integer i5,
      Integer i6,
      Integer i7) {
    if (i0 != null) state[0] += i0;
    if (i1 != null) state[0] += i1;
    if (i2 != null) state[0] += i2;
    if (i3 != null) state[0] += i3;
    if (i4 != null) state[0] += i4;
    if (i5 != null) state[0] += i5;
    if (i6 != null) state[0] += i6;
    if (i7 != null) state[0] += i7;
    return state;
  }

  @Override
  public long[] merge(long[] state1, long[] state2) {
    state1[0] += state2[0];
    return state1;
  }

  @Override
  public Long terminate(long[] state) {
    return state[0];
  }
}

class MyJavaUDAFOf9
    implements JavaUDAF9<
        long[],
        Long,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer> {
  @Override
  public DataType outputType() {
    return DataTypes.LongType;
  }

  @Override
  public long[] initialize() {
    return new long[] {0L};
  }

  @Override
  public long[] accumulate(
      long[] state,
      Integer i0,
      Integer i1,
      Integer i2,
      Integer i3,
      Integer i4,
      Integer i5,
      Integer i6,
      Integer i7,
      Integer i8) {
    if (i0 != null) state[0] += i0;
    if (i1 != null) state[0] += i1;
    if (i2 != null) state[0] += i2;
    if (i3 != null) state[0] += i3;
    if (i4 != null) state[0] += i4;
    if (i5 != null) state[0] += i5;
    if (i6 != null) state[0] += i6;
    if (i7 != null) state[0] += i7;
    if (i8 != null) state[0] += i8;
    return state;
  }

  @Override
  public long[] merge(long[] state1, long[] state2) {
    state1[0] += state2[0];
    return state1;
  }

  @Override
  public Long terminate(long[] state) {
    return state[0];
  }
}

class MyJavaUDAFOf10
    implements JavaUDAF10<
        long[],
        Long,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer> {
  @Override
  public DataType outputType() {
    return DataTypes.LongType;
  }

  @Override
  public long[] initialize() {
    return new long[] {0L};
  }

  @Override
  public long[] accumulate(
      long[] state,
      Integer i0,
      Integer i1,
      Integer i2,
      Integer i3,
      Integer i4,
      Integer i5,
      Integer i6,
      Integer i7,
      Integer i8,
      Integer i9) {
    if (i0 != null) state[0] += i0;
    if (i1 != null) state[0] += i1;
    if (i2 != null) state[0] += i2;
    if (i3 != null) state[0] += i3;
    if (i4 != null) state[0] += i4;
    if (i5 != null) state[0] += i5;
    if (i6 != null) state[0] += i6;
    if (i7 != null) state[0] += i7;
    if (i8 != null) state[0] += i8;
    if (i9 != null) state[0] += i9;
    return state;
  }

  @Override
  public long[] merge(long[] state1, long[] state2) {
    state1[0] += state2[0];
    return state1;
  }

  @Override
  public Long terminate(long[] state) {
    return state[0];
  }
}

class MyJavaUDAFOf11
    implements JavaUDAF11<
        long[],
        Long,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer> {
  @Override
  public DataType outputType() {
    return DataTypes.LongType;
  }

  @Override
  public long[] initialize() {
    return new long[] {0L};
  }

  @Override
  public long[] accumulate(
      long[] state,
      Integer i0,
      Integer i1,
      Integer i2,
      Integer i3,
      Integer i4,
      Integer i5,
      Integer i6,
      Integer i7,
      Integer i8,
      Integer i9,
      Integer i10) {
    if (i0 != null) state[0] += i0;
    if (i1 != null) state[0] += i1;
    if (i2 != null) state[0] += i2;
    if (i3 != null) state[0] += i3;
    if (i4 != null) state[0] += i4;
    if (i5 != null) state[0] += i5;
    if (i6 != null) state[0] += i6;
    if (i7 != null) state[0] += i7;
    if (i8 != null) state[0] += i8;
    if (i9 != null) state[0] += i9;
    if (i10 != null) state[0] += i10;
    return state;
  }

  @Override
  public long[] merge(long[] state1, long[] state2) {
    state1[0] += state2[0];
    return state1;
  }

  @Override
  public Long terminate(long[] state) {
    return state[0];
  }
}

class MyJavaUDAFOf12
    implements JavaUDAF12<
        long[],
        Long,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer> {
  @Override
  public DataType outputType() {
    return DataTypes.LongType;
  }

  @Override
  public long[] initialize() {
    return new long[] {0L};
  }

  @Override
  public long[] accumulate(
      long[] state,
      Integer i0,
      Integer i1,
      Integer i2,
      Integer i3,
      Integer i4,
      Integer i5,
      Integer i6,
      Integer i7,
      Integer i8,
      Integer i9,
      Integer i10,
      Integer i11) {
    if (i0 != null) state[0] += i0;
    if (i1 != null) state[0] += i1;
    if (i2 != null) state[0] += i2;
    if (i3 != null) state[0] += i3;
    if (i4 != null) state[0] += i4;
    if (i5 != null) state[0] += i5;
    if (i6 != null) state[0] += i6;
    if (i7 != null) state[0] += i7;
    if (i8 != null) state[0] += i8;
    if (i9 != null) state[0] += i9;
    if (i10 != null) state[0] += i10;
    if (i11 != null) state[0] += i11;
    return state;
  }

  @Override
  public long[] merge(long[] state1, long[] state2) {
    state1[0] += state2[0];
    return state1;
  }

  @Override
  public Long terminate(long[] state) {
    return state[0];
  }
}

class MyJavaUDAFOf13
    implements JavaUDAF13<
        long[],
        Long,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer> {
  @Override
  public DataType outputType() {
    return DataTypes.LongType;
  }

  @Override
  public long[] initialize() {
    return new long[] {0L};
  }

  @Override
  public long[] accumulate(
      long[] state,
      Integer i0,
      Integer i1,
      Integer i2,
      Integer i3,
      Integer i4,
      Integer i5,
      Integer i6,
      Integer i7,
      Integer i8,
      Integer i9,
      Integer i10,
      Integer i11,
      Integer i12) {
    if (i0 != null) state[0] += i0;
    if (i1 != null) state[0] += i1;
    if (i2 != null) state[0] += i2;
    if (i3 != null) state[0] += i3;
    if (i4 != null) state[0] += i4;
    if (i5 != null) state[0] += i5;
    if (i6 != null) state[0] += i6;
    if (i7 != null) state[0] += i7;
    if (i8 != null) state[0] += i8;
    if (i9 != null) state[0] += i9;
    if (i10 != null) state[0] += i10;
    if (i11 != null) state[0] += i11;
    if (i12 != null) state[0] += i12;
    return state;
  }

  @Override
  public long[] merge(long[] state1, long[] state2) {
    state1[0] += state2[0];
    return state1;
  }

  @Override
  public Long terminate(long[] state) {
    return state[0];
  }
}

class MyJavaUDAFOf14
    implements JavaUDAF14<
        long[],
        Long,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer> {
  @Override
  public DataType outputType() {
    return DataTypes.LongType;
  }

  @Override
  public long[] initialize() {
    return new long[] {0L};
  }

  @Override
  public long[] accumulate(
      long[] state,
      Integer i0,
      Integer i1,
      Integer i2,
      Integer i3,
      Integer i4,
      Integer i5,
      Integer i6,
      Integer i7,
      Integer i8,
      Integer i9,
      Integer i10,
      Integer i11,
      Integer i12,
      Integer i13) {
    if (i0 != null) state[0] += i0;
    if (i1 != null) state[0] += i1;
    if (i2 != null) state[0] += i2;
    if (i3 != null) state[0] += i3;
    if (i4 != null) state[0] += i4;
    if (i5 != null) state[0] += i5;
    if (i6 != null) state[0] += i6;
    if (i7 != null) state[0] += i7;
    if (i8 != null) state[0] += i8;
    if (i9 != null) state[0] += i9;
    if (i10 != null) state[0] += i10;
    if (i11 != null) state[0] += i11;
    if (i12 != null) state[0] += i12;
    if (i13 != null) state[0] += i13;
    return state;
  }

  @Override
  public long[] merge(long[] state1, long[] state2) {
    state1[0] += state2[0];
    return state1;
  }

  @Override
  public Long terminate(long[] state) {
    return state[0];
  }
}

class MyJavaUDAFOf15
    implements JavaUDAF15<
        long[],
        Long,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer> {
  @Override
  public DataType outputType() {
    return DataTypes.LongType;
  }

  @Override
  public long[] initialize() {
    return new long[] {0L};
  }

  @Override
  public long[] accumulate(
      long[] state,
      Integer i0,
      Integer i1,
      Integer i2,
      Integer i3,
      Integer i4,
      Integer i5,
      Integer i6,
      Integer i7,
      Integer i8,
      Integer i9,
      Integer i10,
      Integer i11,
      Integer i12,
      Integer i13,
      Integer i14) {
    if (i0 != null) state[0] += i0;
    if (i1 != null) state[0] += i1;
    if (i2 != null) state[0] += i2;
    if (i3 != null) state[0] += i3;
    if (i4 != null) state[0] += i4;
    if (i5 != null) state[0] += i5;
    if (i6 != null) state[0] += i6;
    if (i7 != null) state[0] += i7;
    if (i8 != null) state[0] += i8;
    if (i9 != null) state[0] += i9;
    if (i10 != null) state[0] += i10;
    if (i11 != null) state[0] += i11;
    if (i12 != null) state[0] += i12;
    if (i13 != null) state[0] += i13;
    if (i14 != null) state[0] += i14;
    return state;
  }

  @Override
  public long[] merge(long[] state1, long[] state2) {
    state1[0] += state2[0];
    return state1;
  }

  @Override
  public Long terminate(long[] state) {
    return state[0];
  }
}

class MyJavaUDAFOf16
    implements JavaUDAF16<
        long[],
        Long,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer> {
  @Override
  public DataType outputType() {
    return DataTypes.LongType;
  }

  @Override
  public long[] initialize() {
    return new long[] {0L};
  }

  @Override
  public long[] accumulate(
      long[] state,
      Integer i0,
      Integer i1,
      Integer i2,
      Integer i3,
      Integer i4,
      Integer i5,
      Integer i6,
      Integer i7,
      Integer i8,
      Integer i9,
      Integer i10,
      Integer i11,
      Integer i12,
      Integer i13,
      Integer i14,
      Integer i15) {
    if (i0 != null) state[0] += i0;
    if (i1 != null) state[0] += i1;
    if (i2 != null) state[0] += i2;
    if (i3 != null) state[0] += i3;
    if (i4 != null) state[0] += i4;
    if (i5 != null) state[0] += i5;
    if (i6 != null) state[0] += i6;
    if (i7 != null) state[0] += i7;
    if (i8 != null) state[0] += i8;
    if (i9 != null) state[0] += i9;
    if (i10 != null) state[0] += i10;
    if (i11 != null) state[0] += i11;
    if (i12 != null) state[0] += i12;
    if (i13 != null) state[0] += i13;
    if (i14 != null) state[0] += i14;
    if (i15 != null) state[0] += i15;
    return state;
  }

  @Override
  public long[] merge(long[] state1, long[] state2) {
    state1[0] += state2[0];
    return state1;
  }

  @Override
  public Long terminate(long[] state) {
    return state[0];
  }
}

class MyJavaUDAFOf17
    implements JavaUDAF17<
        long[],
        Long,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer> {
  @Override
  public DataType outputType() {
    return DataTypes.LongType;
  }

  @Override
  public long[] initialize() {
    return new long[] {0L};
  }

  @Override
  public long[] accumulate(
      long[] state,
      Integer i0,
      Integer i1,
      Integer i2,
      Integer i3,
      Integer i4,
      Integer i5,
      Integer i6,
      Integer i7,
      Integer i8,
      Integer i9,
      Integer i10,
      Integer i11,
      Integer i12,
      Integer i13,
      Integer i14,
      Integer i15,
      Integer i16) {
    if (i0 != null) state[0] += i0;
    if (i1 != null) state[0] += i1;
    if (i2 != null) state[0] += i2;
    if (i3 != null) state[0] += i3;
    if (i4 != null) state[0] += i4;
    if (i5 != null) state[0] += i5;
    if (i6 != null) state[0] += i6;
    if (i7 != null) state[0] += i7;
    if (i8 != null) state[0] += i8;
    if (i9 != null) state[0] += i9;
    if (i10 != null) state[0] += i10;
    if (i11 != null) state[0] += i11;
    if (i12 != null) state[0] += i12;
    if (i13 != null) state[0] += i13;
    if (i14 != null) state[0] += i14;
    if (i15 != null) state[0] += i15;
    if (i16 != null) state[0] += i16;
    return state;
  }

  @Override
  public long[] merge(long[] state1, long[] state2) {
    state1[0] += state2[0];
    return state1;
  }

  @Override
  public Long terminate(long[] state) {
    return state[0];
  }
}

class MyJavaUDAFOf18
    implements JavaUDAF18<
        long[],
        Long,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer> {
  @Override
  public DataType outputType() {
    return DataTypes.LongType;
  }

  @Override
  public long[] initialize() {
    return new long[] {0L};
  }

  @Override
  public long[] accumulate(
      long[] state,
      Integer i0,
      Integer i1,
      Integer i2,
      Integer i3,
      Integer i4,
      Integer i5,
      Integer i6,
      Integer i7,
      Integer i8,
      Integer i9,
      Integer i10,
      Integer i11,
      Integer i12,
      Integer i13,
      Integer i14,
      Integer i15,
      Integer i16,
      Integer i17) {
    if (i0 != null) state[0] += i0;
    if (i1 != null) state[0] += i1;
    if (i2 != null) state[0] += i2;
    if (i3 != null) state[0] += i3;
    if (i4 != null) state[0] += i4;
    if (i5 != null) state[0] += i5;
    if (i6 != null) state[0] += i6;
    if (i7 != null) state[0] += i7;
    if (i8 != null) state[0] += i8;
    if (i9 != null) state[0] += i9;
    if (i10 != null) state[0] += i10;
    if (i11 != null) state[0] += i11;
    if (i12 != null) state[0] += i12;
    if (i13 != null) state[0] += i13;
    if (i14 != null) state[0] += i14;
    if (i15 != null) state[0] += i15;
    if (i16 != null) state[0] += i16;
    if (i17 != null) state[0] += i17;
    return state;
  }

  @Override
  public long[] merge(long[] state1, long[] state2) {
    state1[0] += state2[0];
    return state1;
  }

  @Override
  public Long terminate(long[] state) {
    return state[0];
  }
}

class MyJavaUDAFOf19
    implements JavaUDAF19<
        long[],
        Long,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer> {
  @Override
  public DataType outputType() {
    return DataTypes.LongType;
  }

  @Override
  public long[] initialize() {
    return new long[] {0L};
  }

  @Override
  public long[] accumulate(
      long[] state,
      Integer i0,
      Integer i1,
      Integer i2,
      Integer i3,
      Integer i4,
      Integer i5,
      Integer i6,
      Integer i7,
      Integer i8,
      Integer i9,
      Integer i10,
      Integer i11,
      Integer i12,
      Integer i13,
      Integer i14,
      Integer i15,
      Integer i16,
      Integer i17,
      Integer i18) {
    if (i0 != null) state[0] += i0;
    if (i1 != null) state[0] += i1;
    if (i2 != null) state[0] += i2;
    if (i3 != null) state[0] += i3;
    if (i4 != null) state[0] += i4;
    if (i5 != null) state[0] += i5;
    if (i6 != null) state[0] += i6;
    if (i7 != null) state[0] += i7;
    if (i8 != null) state[0] += i8;
    if (i9 != null) state[0] += i9;
    if (i10 != null) state[0] += i10;
    if (i11 != null) state[0] += i11;
    if (i12 != null) state[0] += i12;
    if (i13 != null) state[0] += i13;
    if (i14 != null) state[0] += i14;
    if (i15 != null) state[0] += i15;
    if (i16 != null) state[0] += i16;
    if (i17 != null) state[0] += i17;
    if (i18 != null) state[0] += i18;
    return state;
  }

  @Override
  public long[] merge(long[] state1, long[] state2) {
    state1[0] += state2[0];
    return state1;
  }

  @Override
  public Long terminate(long[] state) {
    return state[0];
  }
}

class MyJavaUDAFOf20
    implements JavaUDAF20<
        long[],
        Long,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer> {
  @Override
  public DataType outputType() {
    return DataTypes.LongType;
  }

  @Override
  public long[] initialize() {
    return new long[] {0L};
  }

  @Override
  public long[] accumulate(
      long[] state,
      Integer i0,
      Integer i1,
      Integer i2,
      Integer i3,
      Integer i4,
      Integer i5,
      Integer i6,
      Integer i7,
      Integer i8,
      Integer i9,
      Integer i10,
      Integer i11,
      Integer i12,
      Integer i13,
      Integer i14,
      Integer i15,
      Integer i16,
      Integer i17,
      Integer i18,
      Integer i19) {
    if (i0 != null) state[0] += i0;
    if (i1 != null) state[0] += i1;
    if (i2 != null) state[0] += i2;
    if (i3 != null) state[0] += i3;
    if (i4 != null) state[0] += i4;
    if (i5 != null) state[0] += i5;
    if (i6 != null) state[0] += i6;
    if (i7 != null) state[0] += i7;
    if (i8 != null) state[0] += i8;
    if (i9 != null) state[0] += i9;
    if (i10 != null) state[0] += i10;
    if (i11 != null) state[0] += i11;
    if (i12 != null) state[0] += i12;
    if (i13 != null) state[0] += i13;
    if (i14 != null) state[0] += i14;
    if (i15 != null) state[0] += i15;
    if (i16 != null) state[0] += i16;
    if (i17 != null) state[0] += i17;
    if (i18 != null) state[0] += i18;
    if (i19 != null) state[0] += i19;
    return state;
  }

  @Override
  public long[] merge(long[] state1, long[] state2) {
    state1[0] += state2[0];
    return state1;
  }

  @Override
  public Long terminate(long[] state) {
    return state[0];
  }
}

class MyJavaUDAFOf21
    implements JavaUDAF21<
        long[],
        Long,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer> {
  @Override
  public DataType outputType() {
    return DataTypes.LongType;
  }

  @Override
  public long[] initialize() {
    return new long[] {0L};
  }

  @Override
  public long[] accumulate(
      long[] state,
      Integer i0,
      Integer i1,
      Integer i2,
      Integer i3,
      Integer i4,
      Integer i5,
      Integer i6,
      Integer i7,
      Integer i8,
      Integer i9,
      Integer i10,
      Integer i11,
      Integer i12,
      Integer i13,
      Integer i14,
      Integer i15,
      Integer i16,
      Integer i17,
      Integer i18,
      Integer i19,
      Integer i20) {
    if (i0 != null) state[0] += i0;
    if (i1 != null) state[0] += i1;
    if (i2 != null) state[0] += i2;
    if (i3 != null) state[0] += i3;
    if (i4 != null) state[0] += i4;
    if (i5 != null) state[0] += i5;
    if (i6 != null) state[0] += i6;
    if (i7 != null) state[0] += i7;
    if (i8 != null) state[0] += i8;
    if (i9 != null) state[0] += i9;
    if (i10 != null) state[0] += i10;
    if (i11 != null) state[0] += i11;
    if (i12 != null) state[0] += i12;
    if (i13 != null) state[0] += i13;
    if (i14 != null) state[0] += i14;
    if (i15 != null) state[0] += i15;
    if (i16 != null) state[0] += i16;
    if (i17 != null) state[0] += i17;
    if (i18 != null) state[0] += i18;
    if (i19 != null) state[0] += i19;
    if (i20 != null) state[0] += i20;
    return state;
  }

  @Override
  public long[] merge(long[] state1, long[] state2) {
    state1[0] += state2[0];
    return state1;
  }

  @Override
  public Long terminate(long[] state) {
    return state[0];
  }
}

class MyJavaUDAFOf22
    implements JavaUDAF22<
        long[],
        Long,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer,
        Integer> {
  @Override
  public DataType outputType() {
    return DataTypes.LongType;
  }

  @Override
  public long[] initialize() {
    return new long[] {0L};
  }

  @Override
  public long[] accumulate(
      long[] state,
      Integer i0,
      Integer i1,
      Integer i2,
      Integer i3,
      Integer i4,
      Integer i5,
      Integer i6,
      Integer i7,
      Integer i8,
      Integer i9,
      Integer i10,
      Integer i11,
      Integer i12,
      Integer i13,
      Integer i14,
      Integer i15,
      Integer i16,
      Integer i17,
      Integer i18,
      Integer i19,
      Integer i20,
      Integer i21) {
    if (i0 != null) state[0] += i0;
    if (i1 != null) state[0] += i1;
    if (i2 != null) state[0] += i2;
    if (i3 != null) state[0] += i3;
    if (i4 != null) state[0] += i4;
    if (i5 != null) state[0] += i5;
    if (i6 != null) state[0] += i6;
    if (i7 != null) state[0] += i7;
    if (i8 != null) state[0] += i8;
    if (i9 != null) state[0] += i9;
    if (i10 != null) state[0] += i10;
    if (i11 != null) state[0] += i11;
    if (i12 != null) state[0] += i12;
    if (i13 != null) state[0] += i13;
    if (i14 != null) state[0] += i14;
    if (i15 != null) state[0] += i15;
    if (i16 != null) state[0] += i16;
    if (i17 != null) state[0] += i17;
    if (i18 != null) state[0] += i18;
    if (i19 != null) state[0] += i19;
    if (i20 != null) state[0] += i20;
    if (i21 != null) state[0] += i21;
    return state;
  }

  @Override
  public long[] merge(long[] state1, long[] state2) {
    state1[0] += state2[0];
    return state1;
  }

  @Override
  public Long terminate(long[] state) {
    return state[0];
  }
}
