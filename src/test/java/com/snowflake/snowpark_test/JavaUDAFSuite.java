package com.snowflake.snowpark_test;

import static com.snowflake.snowpark_java.Functions.*;

import com.snowflake.snowpark_java.*;
import com.snowflake.snowpark_java.types.*;
import com.snowflake.snowpark_java.udaf.*;
import java.math.BigDecimal;
import java.util.*;
import org.junit.Test;

public class JavaUDAFSuite extends UDFTestBase {
  public JavaUDAFSuite() {}

  private boolean dependencyAdded = false;
  private String tableName = randomTableName();
  private String tmpStageName = randomStageName();

  @Override
  public Session getSession() {
    Session session = super.getSession();
    if (!dependencyAdded) {
      dependencyAdded = true;
      addDepsToClassPath(session);
      createStage(tmpStageName, true);
      setupTestData();
    }
    return session;
  }

  private void setupTestData() {
    // Create test table with various data
    String createTable =
        "create or replace temp table "
            + tableName
            + "(category string, amount double, quantity int, flag boolean)";
    runQuery(createTable);

    String insertData =
        "insert into "
            + tableName
            + " values "
            + "('Electronics', 100.0, 10, true),"
            + "('Electronics', 200.0, 20, true),"
            + "('Electronics', 150.0, 15, false),"
            + "('Clothing', 50.0, 5, true),"
            + "('Clothing', 75.0, 10, true),"
            + "('Clothing', null, null, null),"
            + "('Books', 25.5, 3, true),"
            + "('Books', 35.5, 7, false)";
    runQuery(insertData);
  }

  @Test
  public void testBasicSum() {
    String funcName = randomFunctionName();
    getSession().udaf().registerTemporary(funcName, new MySumUDAF());

    DataFrame df = getSession().table(tableName);
    DataFrame result =
        df.groupBy(col("category"))
            .agg(callUDAF(funcName, col("amount")).as("total"))
            .sort(col("category"));

    Row[] expected =
        new Row[] {
          Row.create("Books", 61.0), Row.create("Clothing", 125.0), Row.create("Electronics", 450.0)
        };
    checkAnswer(result, expected);
  }

  @Test
  public void testStaticMethodSum() {
    // Test using static method pattern (recommended by Snowflake docs)
    String funcName = randomFunctionName();
    getSession().udaf().registerTemporary(funcName, new MyStaticSumUDAF());

    DataFrame df = getSession().table(tableName);
    DataFrame result = df.agg(callUDAF(funcName, col("amount")).as("total"));

    Row[] expected = new Row[] {Row.create(636.0)};
    checkAnswer(result, expected);
  }

  @Test
  public void testCustomAverage() {
    String funcName = randomFunctionName();
    getSession().udaf().registerTemporary(funcName, new MyAvgUDAF());

    DataFrame df = getSession().table(tableName);
    DataFrame result =
        df.groupBy(col("category"))
            .agg(callUDAF(funcName, col("amount")).as("average"))
            .sort(col("category"));

    Row[] expected =
        new Row[] {
          Row.create("Books", 30.5), // (25.5 + 35.5) / 2
          Row.create("Clothing", 62.5), // (50 + 75) / 2
          Row.create("Electronics", 150.0) // (100 + 200 + 150) / 3
        };
    checkAnswer(result, expected);
  }

  @Test
  public void testCountAll() {
    String funcName = randomFunctionName();
    getSession().udaf().registerTemporary(funcName, new MyCountUDAF());

    DataFrame df = getSession().table(tableName);
    DataFrame result =
        df.groupBy(col("category")).agg(callUDAF(funcName).as("count")).sort(col("category"));

    Row[] expected =
        new Row[] {
          Row.create("Books", 2L), Row.create("Clothing", 3L), Row.create("Electronics", 3L)
        };
    checkAnswer(result, expected);
  }

  @Test
  public void testCountDistinct() {
    // Create test data with duplicates
    String testTable = randomTableName();
    runQuery(
        "create or replace temp table "
            + testTable
            + "(category string, item string) as "
            + "select * from values "
            + "('A', 'item1'), ('A', 'item1'), ('A', 'item2'), "
            + "('B', 'item3'), ('B', 'item3'), ('B', 'item3')");

    String funcName = randomFunctionName();
    getSession().udaf().registerTemporary(funcName, new MyCountDistinctUDAF());

    DataFrame df = getSession().table(testTable);
    DataFrame result =
        df.groupBy(col("category"))
            .agg(callUDAF(funcName, col("item")).as("distinct_count"))
            .sort(col("category"));

    Row[] expected = new Row[] {Row.create("A", 2L), Row.create("B", 1L)};
    checkAnswer(result, expected);
  }

  @Test
  public void testMax() {
    String funcName = randomFunctionName();
    getSession().udaf().registerTemporary(funcName, new MyMaxUDAF());

    DataFrame df = getSession().table(tableName);
    DataFrame result =
        df.groupBy(col("category"))
            .agg(callUDAF(funcName, col("amount")).as("max"))
            .sort(col("category"));

    Row[] expected =
        new Row[] {
          Row.create("Books", 35.5), Row.create("Clothing", 75.0), Row.create("Electronics", 200.0)
        };
    checkAnswer(result, expected);
  }

  @Test
  public void testWeightedAverage() {
    String funcName = randomFunctionName();
    getSession().udaf().registerTemporary(funcName, new MyWeightedAvgUDAF());

    DataFrame df = getSession().table(tableName);
    // Weighted average: amount weighted by quantity
    DataFrame result =
        df.groupBy(col("category"))
            .agg(callUDAF(funcName, col("amount"), col("quantity")).as("weighted_avg"))
            .sort(col("category"));

    // Books: (25.5*3 + 35.5*7) / (3+7) = 325 / 10 = 32.5
    // Clothing: (50*5 + 75*10) / (5+10) = 1000 / 15 = 66.667
    // Electronics: (100*10 + 200*20 + 150*15) / (10+20+15) = 7350 / 45 = 163.333
    Row[] results = result.collect();
    assert results.length == 3;
    assert results[0].getString(0).equals("Books");
    assert Math.abs(results[0].getDouble(1) - 32.5) < 0.01;
  }

  @Test
  public void testListAgg() {
    String testTable = randomTableName();
    runQuery(
        "create or replace temp table "
            + testTable
            + "(id int, word string) as "
            + "select * from values (1, 'hello'), (1, 'world'), (2, 'foo'), (2, 'bar')");

    String funcName = randomFunctionName();
    getSession().udaf().registerTemporary(funcName, new MyListAggUDAF());

    DataFrame df = getSession().table(testTable);
    DataFrame result =
        df.groupBy(col("id")).agg(callUDAF(funcName, col("word")).as("words")).sort(col("id"));

    Row[] expected = new Row[] {Row.create(1, "hello,world"), Row.create(2, "foo,bar")};
    checkAnswer(result, expected);
  }

  @Test
  public void testRangeUDAF() {
    String funcName = randomFunctionName();
    getSession().udaf().registerTemporary(funcName, new MyRangeUDAF());

    DataFrame df = getSession().table(tableName);
    DataFrame result =
        df.groupBy(col("category"))
            .agg(callUDAF(funcName, col("amount")).as("range"))
            .sort(col("category"));

    Row[] expected =
        new Row[] {
          Row.create("Books", "25.50-35.50"),
          Row.create("Clothing", "50.00-75.00"),
          Row.create("Electronics", "100.00-200.00")
        };
    checkAnswer(result, expected);
  }

  @Test
  public void testAggregateWithoutGroupBy() {
    String funcName = randomFunctionName();
    getSession().udaf().registerTemporary(funcName, new MySumUDAF());

    DataFrame df = getSession().table(tableName);
    DataFrame result = df.agg(callUDAF(funcName, col("amount")).as("grand_total"));

    // Total: 100 + 200 + 150 + 50 + 75 + 25.5 + 35.5 = 636.0
    Row[] expected = new Row[] {Row.create(636.0)};
    checkAnswer(result, expected);
  }

  @Test
  public void testMultipleAggregations() {
    String sumFunc = randomFunctionName();
    String avgFunc = randomFunctionName();
    String maxFunc = randomFunctionName();

    getSession().udaf().registerTemporary(sumFunc, new MySumUDAF());
    getSession().udaf().registerTemporary(avgFunc, new MyAvgUDAF());
    getSession().udaf().registerTemporary(maxFunc, new MyMaxUDAF());

    DataFrame df = getSession().table(tableName);
    DataFrame result =
        df.groupBy(col("category"))
            .agg(
                callUDAF(sumFunc, col("amount")).as("total"),
                callUDAF(avgFunc, col("amount")).as("average"),
                callUDAF(maxFunc, col("amount")).as("max"))
            .sort(col("category"));

    Row[] results = result.collect();
    assert results.length == 3;

    // Books
    assert results[0].getString(0).equals("Books");
    assert Math.abs(results[0].getDouble(1) - 61.0) < 0.01;
    assert Math.abs(results[0].getDouble(2) - 30.5) < 0.01;
    assert Math.abs(results[0].getDouble(3) - 35.5) < 0.01;
  }

  @Test
  public void testNullHandling() {
    String funcName = randomFunctionName();
    getSession().udaf().registerTemporary(funcName, new MySumUDAF());

    DataFrame df = getSession().table(tableName).filter(col("category").equal_to(lit("Clothing")));
    DataFrame result = df.agg(callUDAF(funcName, col("amount")).as("total"));

    // Clothing: 50.0 + 75.0 + null = 125.0 (null should be ignored)
    Row[] expected = new Row[] {Row.create(125.0)};
    checkAnswer(result, expected);
  }

  @Test
  public void testComplexTypes() {
    // Test with decimal type
    String testTable = randomTableName();
    runQuery(
        "create or replace temp table "
            + testTable
            + "(category string, amount number(10,2)) as "
            + "select * from values ('A', 10.50), ('A', 20.75), ('B', 5.25)");

    String funcName = randomFunctionName();
    getSession().udaf().registerTemporary(funcName, new MyDecimalSumUDAF());

    DataFrame df = getSession().table(testTable);
    DataFrame result =
        df.groupBy(col("category"))
            .agg(callUDAF(funcName, col("amount")).as("total"))
            .sort(col("category"));

    Row[] results = result.collect();
    assert results.length == 2;
    assert results[0].getString(0).equals("A");
    assert results[0].getDecimal(1).compareTo(new BigDecimal("31.25")) == 0;
  }

  @Test
  public void testPermanentUDAF() {
    String funcName = randomFunctionName();
    try {
      getSession().udaf().registerPermanent(funcName, new MySumUDAF(), tmpStageName);

      DataFrame df = getSession().table(tableName);
      DataFrame result =
          df.groupBy(col("category"))
              .agg(callUDAF(funcName, col("amount")).as("total"))
              .sort(col("category"));

      Row[] expected =
          new Row[] {
            Row.create("Books", 61.0),
            Row.create("Clothing", 125.0),
            Row.create("Electronics", 450.0)
          };
      checkAnswer(result, expected);
    } finally {
      // Cleanup
      getSession().sql("DROP FUNCTION IF EXISTS " + funcName + "(DOUBLE)").collect();
    }
  }

  @Test
  public void testNegativeCases() {
    Exception ex = null;

    // Case 1: UDAF with unsupported type
    ex = null;
    try {
      getSession().udaf().registerTemporary(new NegativeUnsupportedTypeUDAF());
    } catch (Exception e) {
      ex = e;
    }
    assert ex != null && ex.getMessage().contains("Unsupported");
  }
}

// ============================================================
// Test UDAF Implementations
// ============================================================

/** Custom SUM using instance methods. Handler class is stateless (no class members stored). */
class MySumUDAF implements JavaUDAF1<Double, Double, Double> {
  @Override
  public Double initialize() {
    return 0.0;
  }

  @Override
  public Double accumulate(Double state, Double value) {
    if (value != null) {
      return state + value;
    }
    return state;
  }

  @Override
  public Double merge(Double state1, Double state2) {
    return state1 + state2;
  }

  @Override
  public Double finish(Double state) {
    return state;
  }
}

/**
 * Custom SUM using static helper methods (recommended pattern per Snowflake docs). Static methods
 * make it explicit that the handler is stateless. This pattern matches the official Snowflake
 * documentation examples where all logic is static.
 */
class MyStaticSumUDAF implements JavaUDAF1<Double, Double, Double> {
  @Override
  public Double initialize() {
    return initializeState();
  }

  @Override
  public Double accumulate(Double state, Double value) {
    return accumulateValue(state, value);
  }

  @Override
  public Double merge(Double state1, Double state2) {
    return mergeStates(state1, state2);
  }

  @Override
  public Double finish(Double state) {
    return finishAggregation(state);
  }

  public static double initializeState() {
    return 0.0;
  }

  public static double accumulateValue(double partialSum, Double input) {
    return (input != null) ? partialSum + input : partialSum;
  }

  public static double mergeStates(double partialSum1, double partialSum2) {
    return partialSum1 + partialSum2;
  }

  public static double finishAggregation(double partialSum) {
    return partialSum;
  }
}

class MyAvgUDAF implements JavaUDAF1<MyAvgUDAF.AvgState, Double, Double> {
  static class AvgState implements java.io.Serializable {
    double sum;
    long count;

    AvgState() {
      this.sum = 0.0;
      this.count = 0;
    }

    AvgState(double sum, long count) {
      this.sum = sum;
      this.count = count;
    }
  }

  @Override
  public AvgState initialize() {
    return new AvgState();
  }

  @Override
  public AvgState accumulate(AvgState state, Double value) {
    if (value != null) {
      return new AvgState(state.sum + value, state.count + 1);
    }
    return state;
  }

  @Override
  public AvgState merge(AvgState state1, AvgState state2) {
    return new AvgState(state1.sum + state2.sum, state1.count + state2.count);
  }

  @Override
  public Double finish(AvgState state) {
    if (state.count == 0) {
      return null;
    }
    return state.sum / state.count;
  }
}

class MyCountUDAF implements JavaUDAF0<Long, Long> {
  @Override
  public Long initialize() {
    return 0L;
  }

  @Override
  public Long accumulate(Long state) {
    return state + 1;
  }

  @Override
  public Long merge(Long state1, Long state2) {
    return state1 + state2;
  }

  @Override
  public Long finish(Long state) {
    return state;
  }
}

class MyCountDistinctUDAF implements JavaUDAF1<Set<String>, String, Long> {
  @Override
  public Set<String> initialize() {
    return new HashSet<>();
  }

  @Override
  public Set<String> accumulate(Set<String> state, String value) {
    if (value != null) {
      state.add(value);
    }
    return state;
  }

  @Override
  public Set<String> merge(Set<String> state1, Set<String> state2) {
    state1.addAll(state2);
    return state1;
  }

  @Override
  public Long finish(Set<String> state) {
    return (long) state.size();
  }
}

class MyMaxUDAF implements JavaUDAF1<Double, Double, Double> {
  @Override
  public Double initialize() {
    return null;
  }

  @Override
  public Double accumulate(Double state, Double value) {
    if (value == null) {
      return state;
    }
    if (state == null) {
      return value;
    }
    return Math.max(state, value);
  }

  @Override
  public Double merge(Double state1, Double state2) {
    if (state1 == null) return state2;
    if (state2 == null) return state1;
    return Math.max(state1, state2);
  }

  @Override
  public Double finish(Double state) {
    return state;
  }
}

class MyWeightedAvgUDAF implements JavaUDAF2<MyWeightedAvgUDAF.State, Double, Double, Double> {
  static class State implements java.io.Serializable {
    double weightedSum;
    double totalWeight;

    State() {
      this.weightedSum = 0.0;
      this.totalWeight = 0.0;
    }

    State(double weightedSum, double totalWeight) {
      this.weightedSum = weightedSum;
      this.totalWeight = totalWeight;
    }
  }

  @Override
  public State initialize() {
    return new State();
  }

  @Override
  public State accumulate(State state, Double value, Double weight) {
    if (value != null && weight != null) {
      return new State(state.weightedSum + (value * weight), state.totalWeight + weight);
    }
    return state;
  }

  @Override
  public State merge(State state1, State state2) {
    return new State(
        state1.weightedSum + state2.weightedSum, state1.totalWeight + state2.totalWeight);
  }

  @Override
  public Double finish(State state) {
    if (state.totalWeight == 0) {
      return null;
    }
    return state.weightedSum / state.totalWeight;
  }
}

/**
 * Custom LISTAGG aggregate function. Note: Handler class must be stateless (no class members) per
 * Snowflake requirements. Using fixed delimiter "," instead of configurable one.
 */
class MyListAggUDAF implements JavaUDAF1<StringBuilder, String, String> {
  private static final String DELIMITER = ",";

  @Override
  public StringBuilder initialize() {
    return new StringBuilder();
  }

  @Override
  public StringBuilder accumulate(StringBuilder state, String value) {
    if (value != null) {
      if (state.length() > 0) {
        state.append(DELIMITER);
      }
      state.append(value);
    }
    return state;
  }

  @Override
  public StringBuilder merge(StringBuilder state1, StringBuilder state2) {
    if (state1.length() > 0 && state2.length() > 0) {
      state1.append(DELIMITER);
    }
    state1.append(state2);
    return state1;
  }

  @Override
  public String finish(StringBuilder state) {
    return state.toString();
  }
}

class MyRangeUDAF implements JavaUDAF1<MyRangeUDAF.State, Double, String> {
  static class State implements java.io.Serializable {
    Double min;
    Double max;

    State() {
      this.min = null;
      this.max = null;
    }

    State(Double min, Double max) {
      this.min = min;
      this.max = max;
    }
  }

  @Override
  public State initialize() {
    return new State();
  }

  @Override
  public State accumulate(State state, Double value) {
    if (value == null) {
      return state;
    }

    Double newMin = (state.min == null) ? value : Math.min(state.min, value);
    Double newMax = (state.max == null) ? value : Math.max(state.max, value);

    return new State(newMin, newMax);
  }

  @Override
  public State merge(State state1, State state2) {
    if (state1.min == null && state1.max == null) return state2;
    if (state2.min == null && state2.max == null) return state1;

    Double newMin =
        Math.min(
            state1.min != null ? state1.min : Double.MAX_VALUE,
            state2.min != null ? state2.min : Double.MAX_VALUE);
    Double newMax =
        Math.max(
            state1.max != null ? state1.max : Double.MIN_VALUE,
            state2.max != null ? state2.max : Double.MIN_VALUE);

    return new State(newMin, newMax);
  }

  @Override
  public String finish(State state) {
    if (state.min == null || state.max == null) {
      return null;
    }
    return String.format("%.2f-%.2f", state.min, state.max);
  }
}

class MyDecimalSumUDAF implements JavaUDAF1<BigDecimal, BigDecimal, BigDecimal> {
  @Override
  public BigDecimal initialize() {
    return BigDecimal.ZERO;
  }

  @Override
  public BigDecimal accumulate(BigDecimal state, BigDecimal value) {
    if (value != null) {
      return state.add(value);
    }
    return state;
  }

  @Override
  public BigDecimal merge(BigDecimal state1, BigDecimal state2) {
    return state1.add(state2);
  }

  @Override
  public BigDecimal finish(BigDecimal state) {
    return state;
  }
}

class UdafUnsupportedTestClass {}

class NegativeUnsupportedTypeUDAF
    implements JavaUDAF1<
        UdafUnsupportedTestClass, UdafUnsupportedTestClass, UdafUnsupportedTestClass> {
  @Override
  public UdafUnsupportedTestClass initialize() {
    return new UdafUnsupportedTestClass();
  }

  @Override
  public UdafUnsupportedTestClass accumulate(
      UdafUnsupportedTestClass state, UdafUnsupportedTestClass value) {
    return state;
  }

  @Override
  public UdafUnsupportedTestClass merge(
      UdafUnsupportedTestClass state1, UdafUnsupportedTestClass state2) {
    return state1;
  }

  @Override
  public UdafUnsupportedTestClass finish(UdafUnsupportedTestClass state) {
    return state;
  }
}
