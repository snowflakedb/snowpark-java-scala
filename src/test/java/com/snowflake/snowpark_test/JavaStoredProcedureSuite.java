package com.snowflake.snowpark_test;

import com.snowflake.snowpark.SnowparkClientException;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.StoredProcedure;
import com.snowflake.snowpark_java.sproc.*;
import com.snowflake.snowpark_java.types.DataType;
import com.snowflake.snowpark_java.types.DataTypes;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import org.junit.Test;

public class JavaStoredProcedureSuite extends UDFTestBase {
  public JavaStoredProcedureSuite() {}

  private boolean dependencyAdded = false;

  @Override
  public Session getSession() {
    Session session = super.getSession();
    if (!dependencyAdded) {
      dependencyAdded = true;
      addDepsToClassPath(session);
    }
    return session;
  }

  @Test
  public void call() {
    String name = randomName();
    String query =
        "create or replace procedure "
            + name
            + "(str STRING)\n"
            + "returns STRING\n"
            + "language scala\n"
            + "runtime_version=2.12\n"
            + "packages=('com.snowflake:snowpark:latest')\n"
            + "handler='Test.run'\n"
            + "as\n"
            + "$$\n"
            + "object Test {\n"
            + "def run(session: com.snowflake.snowpark.Session, str: String): String = {\n"
            + "str\n"
            + "}\n"
            + "}\n"
            + "$$";

    try {
      getSession().sql(query).show();
      checkAnswer(getSession().storedProcedure(name, "test"), new Row[] {Row.create("test")});
    } finally {
      getSession().sql("drop procedure if exists " + name + " (STRING)");
    }
  }

  @Test
  public void multipleInputTypes() {
    StoredProcedure sp =
        getSession()
            .sproc()
            .registerTemporary(
                (Session session,
                    Integer num1,
                    Long num2,
                    Short num3,
                    Float num4,
                    Double num5,
                    Boolean bool) -> {
                  long num = num1 + num2 + num3;
                  double floats = Math.ceil(num4 + num5);
                  return num + ", " + floats + ", " + bool;
                },
                new DataType[] {
                  DataTypes.IntegerType,
                  DataTypes.LongType,
                  DataTypes.ShortType,
                  DataTypes.FloatType,
                  DataTypes.DoubleType,
                  DataTypes.BooleanType
                },
                DataTypes.StringType);
    checkAnswer(
        getSession().storedProcedure(sp, 1, 2L, (short) 3, 4.4f, 5.5, false),
        new Row[] {Row.create("6, 10.0, false")});
  }

  @Test
  public void decimalInput() {
    StoredProcedure sp =
        getSession()
            .sproc()
            .registerTemporary(
                (Session session, BigDecimal num) -> num,
                DataTypes.createDecimalType(10, 0),
                DataTypes.createDecimalType(10, 0));
    checkAnswer(
        getSession().storedProcedure(sp, BigDecimal.valueOf(123)), new Row[] {Row.create(123)});
  }

  @Test
  public void binaryType() {
    StoredProcedure toBinary =
        getSession()
            .sproc()
            .registerTemporary(
                (Session session, String str) -> str.getBytes(),
                DataTypes.StringType,
                DataTypes.BinaryType);

    StoredProcedure fromBinary =
        getSession()
            .sproc()
            .registerTemporary(
                (Session session, byte[] bytes) -> new String(bytes),
                DataTypes.BinaryType,
                DataTypes.StringType);

    checkAnswer(
        getSession().storedProcedure(toBinary, "hello"),
        new Row[] {Row.create((Object) "hello".getBytes())});
    checkAnswer(
        getSession().storedProcedure(fromBinary, (Object) "hello".getBytes()),
        new Row[] {Row.create("hello")});
  }

  @Test
  public void timestamp() {
    Timestamp time = Timestamp.valueOf("2019-01-01 00:00:00");
    Date date = Date.valueOf("2019-01-01");
    StoredProcedure d =
        getSession()
            .sproc()
            .registerTemporary(
                (Session session, Date date1) -> date1, DataTypes.DateType, DataTypes.DateType);
    StoredProcedure t =
        getSession()
            .sproc()
            .registerTemporary(
                (Session session, Timestamp time1) -> time1,
                DataTypes.TimestampType,
                DataTypes.TimestampType);
    checkAnswer(getSession().storedProcedure(d, date), new Row[] {Row.create(date)});
    checkAnswer(getSession().storedProcedure(t, time), new Row[] {Row.create(time)});
  }

  @Test
  public void runLocallyArgsNumberChecker() {
    JavaSProc0<Integer> sp0 = session -> 1;
    assert checkIncorrectArgsNumberError(0, 1, () -> getSession().sproc().runLocally(sp0, 1));

    JavaSProc1<Integer, Integer> sp1 = (session, num1) -> num1 + 1;
    assert checkIncorrectArgsNumberError(1, 2, () -> getSession().sproc().runLocally(sp1, 1, 2));

    JavaSProc2<Integer, Integer, Integer> sp2 = (session, num1, num2) -> num1 + num2 + 1;
    assert checkIncorrectArgsNumberError(2, 1, () -> getSession().sproc().runLocally(sp2, 1));

    JavaSProc3<Integer, Integer, Integer, Integer> sp3 =
        (session, num1, num2, num3) -> num1 + num2 + num3 + 1;
    assert checkIncorrectArgsNumberError(3, 1, () -> getSession().sproc().runLocally(sp3, 1));

    JavaSProc4<Integer, Integer, Integer, Integer, Integer> sp4 =
        (session, num1, num2, num3, num4) -> num1 + num2 + num3 + num4 + 1;
    assert checkIncorrectArgsNumberError(4, 1, () -> getSession().sproc().runLocally(sp4, 1));

    JavaSProc5<Integer, Integer, Integer, Integer, Integer, Integer> sp5 =
        (session, num1, num2, num3, num4, num5) -> num1 + num2 + num3 + num4 + num5 + 1;
    assert checkIncorrectArgsNumberError(5, 1, () -> getSession().sproc().runLocally(sp5, 1));

    JavaSProc6<Integer, Integer, Integer, Integer, Integer, Integer, Integer> sp6 =
        (session, num1, num2, num3, num4, num5, num6) ->
            num1 + num2 + num3 + num4 + num5 + num6 + 1;
    assert checkIncorrectArgsNumberError(6, 1, () -> getSession().sproc().runLocally(sp6, 1));

    JavaSProc7<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> sp7 =
        (session, num1, num2, num3, num4, num5, num6, num7) ->
            num1 + num2 + num3 + num4 + num5 + num6 + num7 + 1;
    assert checkIncorrectArgsNumberError(7, 1, () -> getSession().sproc().runLocally(sp7, 1));

    JavaSProc8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>
        sp8 =
            (session, num1, num2, num3, num4, num5, num6, num7, num8) ->
                num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + 1;
    assert checkIncorrectArgsNumberError(8, 1, () -> getSession().sproc().runLocally(sp8, 1));

    JavaSProc9<
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer>
        sp9 =
            (session, num1, num2, num3, num4, num5, num6, num7, num8, num9) ->
                num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + 1;
    assert checkIncorrectArgsNumberError(9, 1, () -> getSession().sproc().runLocally(sp9, 1));

    JavaSProc10<
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
            Integer>
        sp10 =
            (session, num1, num2, num3, num4, num5, num6, num7, num8, num9, num10) ->
                num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + num10 + 1;
    assert checkIncorrectArgsNumberError(10, 1, () -> getSession().sproc().runLocally(sp10, 1));

    JavaSProc11<
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
            Integer>
        sp11 =
            (session, num1, num2, num3, num4, num5, num6, num7, num8, num9, num10, num11) ->
                num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + num10 + num11 + 1;
    assert checkIncorrectArgsNumberError(11, 1, () -> getSession().sproc().runLocally(sp11, 1));

    JavaSProc12<
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
            Integer>
        sp12 =
            (session, num1, num2, num3, num4, num5, num6, num7, num8, num9, num10, num11, num12) ->
                num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + num10 + num11 + num12
                    + 1;
    assert checkIncorrectArgsNumberError(12, 1, () -> getSession().sproc().runLocally(sp12, 1));

    JavaSProc13<
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
            Integer>
        sp13 =
            (session,
                num1,
                num2,
                num3,
                num4,
                num5,
                num6,
                num7,
                num8,
                num9,
                num10,
                num11,
                num12,
                num13) ->
                num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + num10 + num11 + num12
                    + num13 + 1;
    assert checkIncorrectArgsNumberError(13, 1, () -> getSession().sproc().runLocally(sp13, 1));

    JavaSProc14<
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
            Integer>
        sp14 =
            (session,
                num1,
                num2,
                num3,
                num4,
                num5,
                num6,
                num7,
                num8,
                num9,
                num10,
                num11,
                num12,
                num13,
                num14) ->
                num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + num10 + num11 + num12
                    + num13 + num14 + 1;
    assert checkIncorrectArgsNumberError(14, 1, () -> getSession().sproc().runLocally(sp14, 1));

    JavaSProc15<
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
            Integer>
        sp15 =
            (session,
                num1,
                num2,
                num3,
                num4,
                num5,
                num6,
                num7,
                num8,
                num9,
                num10,
                num11,
                num12,
                num13,
                num14,
                num15) ->
                num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + num10 + num11 + num12
                    + num13 + num14 + num15 + 1;
    assert checkIncorrectArgsNumberError(15, 1, () -> getSession().sproc().runLocally(sp15, 1));

    JavaSProc16<
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
            Integer>
        sp16 =
            (session,
                num1,
                num2,
                num3,
                num4,
                num5,
                num6,
                num7,
                num8,
                num9,
                num10,
                num11,
                num12,
                num13,
                num14,
                num15,
                num16) ->
                num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + num10 + num11 + num12
                    + num13 + num14 + num15 + num16 + 1;
    assert checkIncorrectArgsNumberError(16, 1, () -> getSession().sproc().runLocally(sp16, 1));

    JavaSProc17<
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
            Integer>
        sp17 =
            (session,
                num1,
                num2,
                num3,
                num4,
                num5,
                num6,
                num7,
                num8,
                num9,
                num10,
                num11,
                num12,
                num13,
                num14,
                num15,
                num16,
                num17) ->
                num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + num10 + num11 + num12
                    + num13 + num14 + num15 + num16 + num17 + 1;
    assert checkIncorrectArgsNumberError(17, 1, () -> getSession().sproc().runLocally(sp17, 1));

    JavaSProc18<
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
            Integer>
        sp18 =
            (session,
                num1,
                num2,
                num3,
                num4,
                num5,
                num6,
                num7,
                num8,
                num9,
                num10,
                num11,
                num12,
                num13,
                num14,
                num15,
                num16,
                num17,
                num18) ->
                num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + num10 + num11 + num12
                    + num13 + num14 + num15 + num16 + num17 + num18 + 1;
    assert checkIncorrectArgsNumberError(18, 1, () -> getSession().sproc().runLocally(sp18, 1));

    JavaSProc19<
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
            Integer>
        sp19 =
            (session,
                num1,
                num2,
                num3,
                num4,
                num5,
                num6,
                num7,
                num8,
                num9,
                num10,
                num11,
                num12,
                num13,
                num14,
                num15,
                num16,
                num17,
                num18,
                num19) ->
                num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + num10 + num11 + num12
                    + num13 + num14 + num15 + num16 + num17 + num18 + num19 + 1;
    assert checkIncorrectArgsNumberError(19, 1, () -> getSession().sproc().runLocally(sp19, 1));

    JavaSProc20<
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
            Integer>
        sp20 =
            (session,
                num1,
                num2,
                num3,
                num4,
                num5,
                num6,
                num7,
                num8,
                num9,
                num10,
                num11,
                num12,
                num13,
                num14,
                num15,
                num16,
                num17,
                num18,
                num19,
                num20) ->
                num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + num10 + num11 + num12
                    + num13 + num14 + num15 + num16 + num17 + num18 + num19 + num20 + 1;
    assert checkIncorrectArgsNumberError(20, 1, () -> getSession().sproc().runLocally(sp20, 1));

    JavaSProc21<
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
            Integer>
        sp21 =
            (session,
                num1,
                num2,
                num3,
                num4,
                num5,
                num6,
                num7,
                num8,
                num9,
                num10,
                num11,
                num12,
                num13,
                num14,
                num15,
                num16,
                num17,
                num18,
                num19,
                num20,
                num21) ->
                num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + num10 + num11 + num12
                    + num13 + num14 + num15 + num16 + num17 + num18 + num19 + num20 + num21 + 1;
    assert checkIncorrectArgsNumberError(21, 1, () -> getSession().sproc().runLocally(sp21, 1));
  }

  private static boolean checkIncorrectArgsNumberError(
      int expected, int input, ThrowException func) {
    try {
      func.run();
      return false; // no exception
    } catch (SnowparkClientException ex) {
      String msg = ex.message();
      // wrong message
      return msg.contains("Error Code: 0211")
          && msg.contains("Expected: " + expected + ", Found: " + input);
    } catch (Exception other) {
      return false; // wrong exception
    }
  }

  interface ThrowException {
    void run();
  }
}
