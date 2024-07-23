package com.snowflake.snowpark_test;

import static com.snowflake.snowpark_java.Functions.*;

import com.snowflake.snowpark.TestUtils;
import com.snowflake.snowpark_java.*;
import com.snowflake.snowpark_java.types.*;
import com.snowflake.snowpark_java.udtf.*;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.TimeZone;
import java.util.stream.Stream;
import org.junit.Test;

public class JavaUDTFNonStoredProcSuite extends UDFTestBase {
  public JavaUDTFNonStoredProcSuite() {}

  private String tableName = randomTableName();
  private boolean dependencyAdded = false;
  private Session anotherSession = null;

  private boolean testStageCreated = false;
  private String testStageName = randomStageName();

  private void createTestStage() {
    if (!testStageCreated) {
      testStageCreated = true;
      createStage(testStageName, false);
    }
  }

  public Session getAnotherSession() {
    if (anotherSession == null) {
      anotherSession = createSession();
      createTestStage();
      addDepsToClassPath(anotherSession, testStageName);
    }
    return anotherSession;
  }

  @Override
  public Session getSession() {
    Session session = super.getSession();
    if (!dependencyAdded) {
      dependencyAdded = true;
      createTestStage();
      addDepsToClassPath(session, testStageName);
    }
    return session;
  }

  @Test
  public void dateTimestamp() {
    TimeZone oldTimeZone = TimeZone.getDefault();
    String sfTimezone =
        getSession().sql("SHOW PARAMETERS LIKE 'TIMEZONE' IN SESSION").collect()[0].getString(1);
    // Test with UTC timezone
    try {
      String crt =
          "create or replace temp table " + tableName + "(d Date, t Time, ts timestamp_ntz)";
      runQuery(crt);
      String insert =
          "insert into "
              + tableName
              + " values "
              + "('2022-01-24', '00:00:00', '2022-01-25 00:00:00.000'),"
              + "('2022-01-25', '12:13:14', '2022-01-25 12:13:14.123'),"
              + "(null, null, null)";
      runQuery(insert);

      TableFunction tableFunction = getSession().udtf().registerTemporary(new UDTFDateTimestamp());

      DataFrame df =
          getSession().table(tableName).join(tableFunction, col("d"), col("t"), col("ts"));

      String schema = TestUtils.treeString(df.schema(), 0);
      String expectedSchema =
          "root\n"
              + " |--D: Date (nullable = true)\n"
              + " |--T: Time (nullable = true)\n"
              + " |--TS: Timestamp (nullable = true)\n"
              + " |--DATE: Date (nullable = true)\n"
              + " |--TIME: Time (nullable = true)\n"
              + " |--TIMESTAMP: Timestamp (nullable = true)";
      assert schema.replaceAll("\\W", "").equals(expectedSchema.replaceAll("\\W", ""));

      Date d1 = Date.valueOf("2022-01-24");
      Date d2 = Date.valueOf("2022-01-25");
      Time t1 = Time.valueOf("00:00:00");
      Time t2 = Time.valueOf("12:13:14");
      Timestamp ts1 = Timestamp.valueOf("2022-01-25 00:00:00.0");
      Timestamp ts2 = Timestamp.valueOf("2022-01-25 12:13:14.123");
      Row row1 = Row.create(d1, t1, ts1, d1, t1, ts1);
      Row row2 = Row.create(d2, t2, ts2, d2, t2, ts2);
      Row row3 = Row.create(null, null, null, null, null, null);
      Row[] expectRows = new Row[] {row1, row1, row2, row2, row3, row3};
      checkAnswer(df, expectRows);
    } finally {
      TimeZone.setDefault(oldTimeZone);
      getSession().sql("alter session set TIMEZONE = '" + sfTimezone + "'").collect();
    }
  }

  @Test
  public void testPermanentUdtf() {
    // Perm register UDTF can only be used in current and another session.
    // Temp register UDTF cannot be used in another session
    String stageName = randomStageName();
    String funcName = randomFunctionName();
    try {
      createStage(stageName, false);
      TableFunction permFunc =
          getSession().udtf().registerPermanent(funcName, new MyJavaUDTFOf2(), "@" + stageName);
      // use perm UDTF in current session
      Row[] rows1 = getSession().tableFunction(permFunc, lit(1), lit(2)).collect();
      assert rows1.length == 1 && rows1[0].size() == 1 && rows1[0].getInt(0) == 3;
      // use perm UDTF in another session
      Row[] rows2 = getAnotherSession().tableFunction(permFunc, lit(3), lit(4)).collect();
      assert rows2.length == 1 && rows2[0].size() == 1 && rows2[0].getInt(0) == 7;

      getSession().sql("drop function " + permFunc.funcName() + "(int, int)").collect();

      TableFunction tempFunc = getSession().udtf().registerTemporary(new MyJavaUDTFOf2());
      // use temp UDTF in current session
      Row[] rows3 = getSession().tableFunction(tempFunc, lit(11), lit(22)).collect();
      assert rows3.length == 1 && rows3[0].size() == 1 && rows3[0].getInt(0) == 33;
      // Cannot use temp UDF in another session
      Exception ex = null;
      try {
        getAnotherSession().tableFunction(tempFunc, lit(3), lit(4)).collect();
      } catch (Exception e) {
        ex = e;
      }
      assert ex != null
          && (ex.getMessage().contains("does not exist or not authorized")
              || ex.getMessage().contains("Unknown user-defined table function"));
    } finally {
      dropStage(stageName);
    }
  }

  // Test for JavaUDTF0 is created manually
  @Test
  public void testJavaUDTFOf0() {
    TableFunction tableFunction = getSession().udtf().registerTemporary(new MyJavaUDTFOf0());
    Row[] rows = getSession().tableFunction(tableFunction).collect();
    assert rows.length == 1 && rows[0].size() == 1 && rows[0].getInt(0) == -1;
  }

  /*
  // Generate test class and function for JavaUDTF1 - JavaUDTF22
  @Test
  public void generateJavaUDTFXTests() {
    int count = 22;
    for (int i = 1; i <= count; i++) {
      StringBuilder types = new StringBuilder("Integer");
      StringBuilder processArgs = new StringBuilder("Integer i0");
      StringBuilder sum = new StringBuilder("i0");
      for (int idx = 1; idx < i; idx++) {
        types.append(", Integer");
        processArgs.append(", Integer i" + idx);
        sum.append(" + i" + idx);
      }
      String className =
          "\nclass MyJavaUDTFOf" + i + " implements JavaUDTF" + i + "<" + types + "> {\n";
      String testClass =
          className
              + "  @Override\n"
              + "  public StructType outputSchema() {\n"
              + "    return StructType.create(new StructField(\"sum\", DataTypes.IntegerType));\n"
              + "  }\n"
              + "  @Override\n"
              + "  public Stream<Row> endPartition() {\n"
              + "    return Stream.empty();\n"
              + "  }\n"
              + "  @Override\n"
              + "  public Stream<Row> process("
              + processArgs
              + ") {\n"
              + "    int sum = "
              + sum
              + ";\n"
              + "    return Stream.of(Row.create(sum));\n"
              + "  }\n"
              + "}";
      System.out.println(testClass);
    }

    for (int i = 1; i <= count; i++) {
      StringBuilder processArgs = new StringBuilder(", lit(1)");
      for (int idx = 1; idx < i; idx++) {
        processArgs.append(", lit(" + (idx + 1) + ")");
      }
      int sum = i * (i + 1) / 2;

      String className = "MyJavaUDTFOf" + i;
      String testFunction =
          "\n  @Test\n"
              + "  public void testJavaUDTFOf"
              + i
              + "() {\n"
              + "    TableFunction tableFunction = getSession().udtf().registerTemporary(new "
              + className
              + "());\n"
              + "    Row[] rows = getSession().tableFunction(tableFunction"
              + processArgs
              + ").collect();\n"
              + "    assert rows.length == 1 && rows[0].size() == 1 && rows[0].getInt(0) == "
              + sum
              + ";\n"
              + "  }";
      System.out.println(testFunction);
    }
  }
  */

  @Test
  public void testJavaUDTFOf1() {
    TableFunction tableFunction = getSession().udtf().registerTemporary(new MyJavaUDTFOf1());
    Row[] rows = getSession().tableFunction(tableFunction, lit(1)).collect();
    assert rows.length == 1 && rows[0].size() == 1 && rows[0].getInt(0) == 1;
  }

  @Test
  public void testJavaUDTFOf2() {
    TableFunction tableFunction = getSession().udtf().registerTemporary(new MyJavaUDTFOf2());
    Row[] rows = getSession().tableFunction(tableFunction, lit(1), lit(2)).collect();
    assert rows.length == 1 && rows[0].size() == 1 && rows[0].getInt(0) == 3;
  }

  @Test
  public void testJavaUDTFOf3() {
    TableFunction tableFunction = getSession().udtf().registerTemporary(new MyJavaUDTFOf3());
    Row[] rows = getSession().tableFunction(tableFunction, lit(1), lit(2), lit(3)).collect();
    assert rows.length == 1 && rows[0].size() == 1 && rows[0].getInt(0) == 6;
  }

  @Test
  public void testJavaUDTFOf4() {
    TableFunction tableFunction = getSession().udtf().registerTemporary(new MyJavaUDTFOf4());
    Row[] rows =
        getSession().tableFunction(tableFunction, lit(1), lit(2), lit(3), lit(4)).collect();
    assert rows.length == 1 && rows[0].size() == 1 && rows[0].getInt(0) == 10;
  }

  @Test
  public void testJavaUDTFOf5() {
    TableFunction tableFunction = getSession().udtf().registerTemporary(new MyJavaUDTFOf5());
    Row[] rows =
        getSession().tableFunction(tableFunction, lit(1), lit(2), lit(3), lit(4), lit(5)).collect();
    assert rows.length == 1 && rows[0].size() == 1 && rows[0].getInt(0) == 15;
  }

  @Test
  public void testJavaUDTFOf6() {
    TableFunction tableFunction = getSession().udtf().registerTemporary(new MyJavaUDTFOf6());
    Row[] rows =
        getSession()
            .tableFunction(tableFunction, lit(1), lit(2), lit(3), lit(4), lit(5), lit(6))
            .collect();
    assert rows.length == 1 && rows[0].size() == 1 && rows[0].getInt(0) == 21;
  }

  @Test
  public void testJavaUDTFOf7() {
    TableFunction tableFunction = getSession().udtf().registerTemporary(new MyJavaUDTFOf7());
    Row[] rows =
        getSession()
            .tableFunction(tableFunction, lit(1), lit(2), lit(3), lit(4), lit(5), lit(6), lit(7))
            .collect();
    assert rows.length == 1 && rows[0].size() == 1 && rows[0].getInt(0) == 28;
  }

  @Test
  public void testJavaUDTFOf8() {
    TableFunction tableFunction = getSession().udtf().registerTemporary(new MyJavaUDTFOf8());
    Row[] rows =
        getSession()
            .tableFunction(
                tableFunction, lit(1), lit(2), lit(3), lit(4), lit(5), lit(6), lit(7), lit(8))
            .collect();
    assert rows.length == 1 && rows[0].size() == 1 && rows[0].getInt(0) == 36;
  }

  @Test
  public void testJavaUDTFOf9() {
    TableFunction tableFunction = getSession().udtf().registerTemporary(new MyJavaUDTFOf9());
    Row[] rows =
        getSession()
            .tableFunction(
                tableFunction,
                lit(1),
                lit(2),
                lit(3),
                lit(4),
                lit(5),
                lit(6),
                lit(7),
                lit(8),
                lit(9))
            .collect();
    assert rows.length == 1 && rows[0].size() == 1 && rows[0].getInt(0) == 45;
  }

  @Test
  public void testJavaUDTFOf10() {
    TableFunction tableFunction = getSession().udtf().registerTemporary(new MyJavaUDTFOf10());
    Row[] rows =
        getSession()
            .tableFunction(
                tableFunction,
                lit(1),
                lit(2),
                lit(3),
                lit(4),
                lit(5),
                lit(6),
                lit(7),
                lit(8),
                lit(9),
                lit(10))
            .collect();
    assert rows.length == 1 && rows[0].size() == 1 && rows[0].getInt(0) == 55;
  }

  @Test
  public void testJavaUDTFOf11() {
    TableFunction tableFunction = getSession().udtf().registerTemporary(new MyJavaUDTFOf11());
    Row[] rows =
        getSession()
            .tableFunction(
                tableFunction,
                lit(1),
                lit(2),
                lit(3),
                lit(4),
                lit(5),
                lit(6),
                lit(7),
                lit(8),
                lit(9),
                lit(10),
                lit(11))
            .collect();
    assert rows.length == 1 && rows[0].size() == 1 && rows[0].getInt(0) == 66;
  }

  @Test
  public void testJavaUDTFOf12() {
    TableFunction tableFunction = getSession().udtf().registerTemporary(new MyJavaUDTFOf12());
    Row[] rows =
        getSession()
            .tableFunction(
                tableFunction,
                lit(1),
                lit(2),
                lit(3),
                lit(4),
                lit(5),
                lit(6),
                lit(7),
                lit(8),
                lit(9),
                lit(10),
                lit(11),
                lit(12))
            .collect();
    assert rows.length == 1 && rows[0].size() == 1 && rows[0].getInt(0) == 78;
  }

  @Test
  public void testJavaUDTFOf13() {
    TableFunction tableFunction = getSession().udtf().registerTemporary(new MyJavaUDTFOf13());
    Row[] rows =
        getSession()
            .tableFunction(
                tableFunction,
                lit(1),
                lit(2),
                lit(3),
                lit(4),
                lit(5),
                lit(6),
                lit(7),
                lit(8),
                lit(9),
                lit(10),
                lit(11),
                lit(12),
                lit(13))
            .collect();
    assert rows.length == 1 && rows[0].size() == 1 && rows[0].getInt(0) == 91;
  }

  @Test
  public void testJavaUDTFOf14() {
    TableFunction tableFunction = getSession().udtf().registerTemporary(new MyJavaUDTFOf14());
    Row[] rows =
        getSession()
            .tableFunction(
                tableFunction,
                lit(1),
                lit(2),
                lit(3),
                lit(4),
                lit(5),
                lit(6),
                lit(7),
                lit(8),
                lit(9),
                lit(10),
                lit(11),
                lit(12),
                lit(13),
                lit(14))
            .collect();
    assert rows.length == 1 && rows[0].size() == 1 && rows[0].getInt(0) == 105;
  }

  @Test
  public void testJavaUDTFOf15() {
    TableFunction tableFunction = getSession().udtf().registerTemporary(new MyJavaUDTFOf15());
    Row[] rows =
        getSession()
            .tableFunction(
                tableFunction,
                lit(1),
                lit(2),
                lit(3),
                lit(4),
                lit(5),
                lit(6),
                lit(7),
                lit(8),
                lit(9),
                lit(10),
                lit(11),
                lit(12),
                lit(13),
                lit(14),
                lit(15))
            .collect();
    assert rows.length == 1 && rows[0].size() == 1 && rows[0].getInt(0) == 120;
  }

  @Test
  public void testJavaUDTFOf16() {
    TableFunction tableFunction = getSession().udtf().registerTemporary(new MyJavaUDTFOf16());
    Row[] rows =
        getSession()
            .tableFunction(
                tableFunction,
                lit(1),
                lit(2),
                lit(3),
                lit(4),
                lit(5),
                lit(6),
                lit(7),
                lit(8),
                lit(9),
                lit(10),
                lit(11),
                lit(12),
                lit(13),
                lit(14),
                lit(15),
                lit(16))
            .collect();
    assert rows.length == 1 && rows[0].size() == 1 && rows[0].getInt(0) == 136;
  }

  @Test
  public void testJavaUDTFOf17() {
    TableFunction tableFunction = getSession().udtf().registerTemporary(new MyJavaUDTFOf17());
    Row[] rows =
        getSession()
            .tableFunction(
                tableFunction,
                lit(1),
                lit(2),
                lit(3),
                lit(4),
                lit(5),
                lit(6),
                lit(7),
                lit(8),
                lit(9),
                lit(10),
                lit(11),
                lit(12),
                lit(13),
                lit(14),
                lit(15),
                lit(16),
                lit(17))
            .collect();
    assert rows.length == 1 && rows[0].size() == 1 && rows[0].getInt(0) == 153;
  }

  @Test
  public void testJavaUDTFOf18() {
    TableFunction tableFunction = getSession().udtf().registerTemporary(new MyJavaUDTFOf18());
    Row[] rows =
        getSession()
            .tableFunction(
                tableFunction,
                lit(1),
                lit(2),
                lit(3),
                lit(4),
                lit(5),
                lit(6),
                lit(7),
                lit(8),
                lit(9),
                lit(10),
                lit(11),
                lit(12),
                lit(13),
                lit(14),
                lit(15),
                lit(16),
                lit(17),
                lit(18))
            .collect();
    assert rows.length == 1 && rows[0].size() == 1 && rows[0].getInt(0) == 171;
  }

  @Test
  public void testJavaUDTFOf19() {
    TableFunction tableFunction = getSession().udtf().registerTemporary(new MyJavaUDTFOf19());
    Row[] rows =
        getSession()
            .tableFunction(
                tableFunction,
                lit(1),
                lit(2),
                lit(3),
                lit(4),
                lit(5),
                lit(6),
                lit(7),
                lit(8),
                lit(9),
                lit(10),
                lit(11),
                lit(12),
                lit(13),
                lit(14),
                lit(15),
                lit(16),
                lit(17),
                lit(18),
                lit(19))
            .collect();
    assert rows.length == 1 && rows[0].size() == 1 && rows[0].getInt(0) == 190;
  }

  @Test
  public void testJavaUDTFOf20() {
    TableFunction tableFunction = getSession().udtf().registerTemporary(new MyJavaUDTFOf20());
    Row[] rows =
        getSession()
            .tableFunction(
                tableFunction,
                lit(1),
                lit(2),
                lit(3),
                lit(4),
                lit(5),
                lit(6),
                lit(7),
                lit(8),
                lit(9),
                lit(10),
                lit(11),
                lit(12),
                lit(13),
                lit(14),
                lit(15),
                lit(16),
                lit(17),
                lit(18),
                lit(19),
                lit(20))
            .collect();
    assert rows.length == 1 && rows[0].size() == 1 && rows[0].getInt(0) == 210;
  }

  @Test
  public void testJavaUDTFOf21() {
    TableFunction tableFunction = getSession().udtf().registerTemporary(new MyJavaUDTFOf21());
    Row[] rows =
        getSession()
            .tableFunction(
                tableFunction,
                lit(1),
                lit(2),
                lit(3),
                lit(4),
                lit(5),
                lit(6),
                lit(7),
                lit(8),
                lit(9),
                lit(10),
                lit(11),
                lit(12),
                lit(13),
                lit(14),
                lit(15),
                lit(16),
                lit(17),
                lit(18),
                lit(19),
                lit(20),
                lit(21))
            .collect();
    assert rows.length == 1 && rows[0].size() == 1 && rows[0].getInt(0) == 231;
  }

  @Test
  public void testJavaUDTFOf22() {
    TableFunction tableFunction = getSession().udtf().registerTemporary(new MyJavaUDTFOf22());
    Row[] rows =
        getSession()
            .tableFunction(
                tableFunction,
                lit(1),
                lit(2),
                lit(3),
                lit(4),
                lit(5),
                lit(6),
                lit(7),
                lit(8),
                lit(9),
                lit(10),
                lit(11),
                lit(12),
                lit(13),
                lit(14),
                lit(15),
                lit(16),
                lit(17),
                lit(18),
                lit(19),
                lit(20),
                lit(21),
                lit(22))
            .collect();
    assert rows.length == 1 && rows[0].size() == 1 && rows[0].getInt(0) == 253;
  }
}

class UDTFDateTimestamp implements JavaUDTF3<Date, Time, Timestamp> {
  @Override
  public Stream<Row> process(Date d, Time t, Timestamp ts) {
    Row row = Row.create(d, t, ts);
    return Stream.of(row, row);
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public StructType outputSchema() {
    return StructType.create(
        new StructField("date", DataTypes.DateType),
        new StructField("time", DataTypes.TimeType),
        new StructField("timestamp", DataTypes.TimestampType));
  }
}

class MyJavaUDTFOf0 implements JavaUDTF0 {

  @Override
  public StructType outputSchema() {
    return StructType.create(new StructField("sum", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process() {
    return Stream.of(Row.create(-1));
  }
}

class MyJavaUDTFOf1 implements JavaUDTF1<Integer> {
  @Override
  public StructType outputSchema() {
    return StructType.create(new StructField("sum", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(Integer i0) {
    int sum = i0;
    return Stream.of(Row.create(sum));
  }
}

class MyJavaUDTFOf2 implements JavaUDTF2<Integer, Integer> {
  @Override
  public StructType outputSchema() {
    return StructType.create(new StructField("sum", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(Integer i0, Integer i1) {
    int sum = i0 + i1;
    return Stream.of(Row.create(sum));
  }
}

class MyJavaUDTFOf3 implements JavaUDTF3<Integer, Integer, Integer> {
  @Override
  public StructType outputSchema() {
    return StructType.create(new StructField("sum", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(Integer i0, Integer i1, Integer i2) {
    int sum = i0 + i1 + i2;
    return Stream.of(Row.create(sum));
  }
}

class MyJavaUDTFOf4 implements JavaUDTF4<Integer, Integer, Integer, Integer> {
  @Override
  public StructType outputSchema() {
    return StructType.create(new StructField("sum", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(Integer i0, Integer i1, Integer i2, Integer i3) {
    int sum = i0 + i1 + i2 + i3;
    return Stream.of(Row.create(sum));
  }
}

class MyJavaUDTFOf5 implements JavaUDTF5<Integer, Integer, Integer, Integer, Integer> {
  @Override
  public StructType outputSchema() {
    return StructType.create(new StructField("sum", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(Integer i0, Integer i1, Integer i2, Integer i3, Integer i4) {
    int sum = i0 + i1 + i2 + i3 + i4;
    return Stream.of(Row.create(sum));
  }
}

class MyJavaUDTFOf6 implements JavaUDTF6<Integer, Integer, Integer, Integer, Integer, Integer> {
  @Override
  public StructType outputSchema() {
    return StructType.create(new StructField("sum", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(
      Integer i0, Integer i1, Integer i2, Integer i3, Integer i4, Integer i5) {
    int sum = i0 + i1 + i2 + i3 + i4 + i5;
    return Stream.of(Row.create(sum));
  }
}

class MyJavaUDTFOf7
    implements JavaUDTF7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> {
  @Override
  public StructType outputSchema() {
    return StructType.create(new StructField("sum", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(
      Integer i0, Integer i1, Integer i2, Integer i3, Integer i4, Integer i5, Integer i6) {
    int sum = i0 + i1 + i2 + i3 + i4 + i5 + i6;
    return Stream.of(Row.create(sum));
  }
}

class MyJavaUDTFOf8
    implements JavaUDTF8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> {
  @Override
  public StructType outputSchema() {
    return StructType.create(new StructField("sum", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(
      Integer i0,
      Integer i1,
      Integer i2,
      Integer i3,
      Integer i4,
      Integer i5,
      Integer i6,
      Integer i7) {
    int sum = i0 + i1 + i2 + i3 + i4 + i5 + i6 + i7;
    return Stream.of(Row.create(sum));
  }
}

class MyJavaUDTFOf9
    implements JavaUDTF9<
        Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> {
  @Override
  public StructType outputSchema() {
    return StructType.create(new StructField("sum", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(
      Integer i0,
      Integer i1,
      Integer i2,
      Integer i3,
      Integer i4,
      Integer i5,
      Integer i6,
      Integer i7,
      Integer i8) {
    int sum = i0 + i1 + i2 + i3 + i4 + i5 + i6 + i7 + i8;
    return Stream.of(Row.create(sum));
  }
}

class MyJavaUDTFOf10
    implements JavaUDTF10<
        Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> {
  @Override
  public StructType outputSchema() {
    return StructType.create(new StructField("sum", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(
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
    int sum = i0 + i1 + i2 + i3 + i4 + i5 + i6 + i7 + i8 + i9;
    return Stream.of(Row.create(sum));
  }
}

class MyJavaUDTFOf11
    implements JavaUDTF11<
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
  public StructType outputSchema() {
    return StructType.create(new StructField("sum", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(
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
    int sum = i0 + i1 + i2 + i3 + i4 + i5 + i6 + i7 + i8 + i9 + i10;
    return Stream.of(Row.create(sum));
  }
}

class MyJavaUDTFOf12
    implements JavaUDTF12<
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
  public StructType outputSchema() {
    return StructType.create(new StructField("sum", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(
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
    int sum = i0 + i1 + i2 + i3 + i4 + i5 + i6 + i7 + i8 + i9 + i10 + i11;
    return Stream.of(Row.create(sum));
  }
}

class MyJavaUDTFOf13
    implements JavaUDTF13<
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
  public StructType outputSchema() {
    return StructType.create(new StructField("sum", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(
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
    int sum = i0 + i1 + i2 + i3 + i4 + i5 + i6 + i7 + i8 + i9 + i10 + i11 + i12;
    return Stream.of(Row.create(sum));
  }
}

class MyJavaUDTFOf14
    implements JavaUDTF14<
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
  public StructType outputSchema() {
    return StructType.create(new StructField("sum", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(
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
    int sum = i0 + i1 + i2 + i3 + i4 + i5 + i6 + i7 + i8 + i9 + i10 + i11 + i12 + i13;
    return Stream.of(Row.create(sum));
  }
}

class MyJavaUDTFOf15
    implements JavaUDTF15<
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
  public StructType outputSchema() {
    return StructType.create(new StructField("sum", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(
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
    int sum = i0 + i1 + i2 + i3 + i4 + i5 + i6 + i7 + i8 + i9 + i10 + i11 + i12 + i13 + i14;
    return Stream.of(Row.create(sum));
  }
}

class MyJavaUDTFOf16
    implements JavaUDTF16<
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
  public StructType outputSchema() {
    return StructType.create(new StructField("sum", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(
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
    int sum = i0 + i1 + i2 + i3 + i4 + i5 + i6 + i7 + i8 + i9 + i10 + i11 + i12 + i13 + i14 + i15;
    return Stream.of(Row.create(sum));
  }
}

class MyJavaUDTFOf17
    implements JavaUDTF17<
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
  public StructType outputSchema() {
    return StructType.create(new StructField("sum", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(
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
    int sum =
        i0 + i1 + i2 + i3 + i4 + i5 + i6 + i7 + i8 + i9 + i10 + i11 + i12 + i13 + i14 + i15 + i16;
    return Stream.of(Row.create(sum));
  }
}

class MyJavaUDTFOf18
    implements JavaUDTF18<
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
  public StructType outputSchema() {
    return StructType.create(new StructField("sum", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(
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
    int sum =
        i0 + i1 + i2 + i3 + i4 + i5 + i6 + i7 + i8 + i9 + i10 + i11 + i12 + i13 + i14 + i15 + i16
            + i17;
    return Stream.of(Row.create(sum));
  }
}

class MyJavaUDTFOf19
    implements JavaUDTF19<
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
  public StructType outputSchema() {
    return StructType.create(new StructField("sum", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(
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
    int sum =
        i0 + i1 + i2 + i3 + i4 + i5 + i6 + i7 + i8 + i9 + i10 + i11 + i12 + i13 + i14 + i15 + i16
            + i17 + i18;
    return Stream.of(Row.create(sum));
  }
}

class MyJavaUDTFOf20
    implements JavaUDTF20<
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
  public StructType outputSchema() {
    return StructType.create(new StructField("sum", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(
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
    int sum =
        i0 + i1 + i2 + i3 + i4 + i5 + i6 + i7 + i8 + i9 + i10 + i11 + i12 + i13 + i14 + i15 + i16
            + i17 + i18 + i19;
    return Stream.of(Row.create(sum));
  }
}

class MyJavaUDTFOf21
    implements JavaUDTF21<
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
  public StructType outputSchema() {
    return StructType.create(new StructField("sum", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(
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
    int sum =
        i0 + i1 + i2 + i3 + i4 + i5 + i6 + i7 + i8 + i9 + i10 + i11 + i12 + i13 + i14 + i15 + i16
            + i17 + i18 + i19 + i20;
    return Stream.of(Row.create(sum));
  }
}

class MyJavaUDTFOf22
    implements JavaUDTF22<
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
  public StructType outputSchema() {
    return StructType.create(new StructField("sum", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(
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
    int sum =
        i0 + i1 + i2 + i3 + i4 + i5 + i6 + i7 + i8 + i9 + i10 + i11 + i12 + i13 + i14 + i15 + i16
            + i17 + i18 + i19 + i20 + i21;
    return Stream.of(Row.create(sum));
  }
}
