package com.snowflake.snowpark_test;

import com.snowflake.snowpark_java.DataFrame;
import com.snowflake.snowpark_java.Row;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class JavaDataFrameNaFunctionsSuite extends TestBase {
  @Test
  public void drop() {
    DataFrame df =
        getSession()
            .sql(
                "select * from values(1.0, 1),('NaN'::Double, 2),(null, 3),"
                    + " (4.0, null), (null, null), ('NaN'::Double, null) as T(a, b)");
    checkAnswer(
        df.na().drop(1, new String[] {"a"}), new Row[] {Row.create(1.0, 1), Row.create(4.0, null)});
  }

  @Test
  public void fill() {
    DataFrame df =
        getSession()
            .sql(
                "select * from values(1.0, 1, true, 'a'),('NaN'::Double, 2, null, 'b'),"
                    + "(null, 3, false, null), (4.0, null, null, 'd'), (null, null, null, null), "
                    + "('NaN'::Double, null, null, null) as T(flo, int, boo, str)");
    Map<String, Object> map = new HashMap<>();
    map.put("flo", 12.3);
    map.put("int", 11);
    map.put("boo", false);
    map.put("str", "f");
    checkAnswer(
        df.na().fill(map),
        new Row[] {
          Row.create(1.0, 1, true, "a"),
          Row.create(12.3, 2, false, "b"),
          Row.create(12.3, 3, false, "f"),
          Row.create(4.0, 11, false, "d"),
          Row.create(12.3, 11, false, "f"),
          Row.create(12.3, 11, false, "f")
        });
  }

  @Test
  public void replace() {
    DataFrame df =
        getSession()
            .sql(
                "select * from values(1.0, 1, true, 'a'),('NaN'::Double, 2, null, 'b'),"
                    + "(null, 3, false, null), (4.0, null, null, 'd'), (null, null, null, null), "
                    + "('NaN'::Double, null, null, null) as T(flo, int, boo, str)");
    Map<Integer, Integer> replacement = new HashMap<>();
    replacement.put(2, 300);
    replacement.put(1, 200);
    checkAnswer(
        df.na().replace("flo", replacement),
        new Row[] {
          Row.create(200.0, 1, true, "a"),
          Row.create(Double.NaN, 2, null, "b"),
          Row.create(null, 3, false, null),
          Row.create(4.0, null, null, "d"),
          Row.create(null, null, null, null),
          Row.create(Double.NaN, null, null, null)
        });

    Map<Object, Object> replacement1 = new HashMap<>();
    replacement1.put(null, true);
    checkAnswer(
        df.na().replace("boo", replacement1),
        new Row[] {
          Row.create(1.0, 1, true, "a"),
          Row.create(Double.NaN, 2, true, "b"),
          Row.create(null, 3, false, null),
          Row.create(4.0, null, true, "d"),
          Row.create(null, null, true, null),
          Row.create(Double.NaN, null, true, null)
        });
  }
}
