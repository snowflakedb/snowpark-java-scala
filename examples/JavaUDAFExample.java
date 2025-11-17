package com.snowflake.snowpark.examples;

import com.snowflake.snowpark_java.*;
import com.snowflake.snowpark_java.udaf.JavaUDAF1;
import static com.snowflake.snowpark_java.Functions.*;

/**
 * Complete example demonstrating UDAF usage in Snowpark.
 * Shows how UDAF compares to UDF and UDTF.
 */
public class JavaUDAFExample {
  
  /**
   * Custom AVG implementation.
   */
  public static class MyAvgUDAF implements JavaUDAF1<MyAvgUDAF.State, Double, Double> {
    
    public static class State implements java.io.Serializable {
      public double sum;
      public long count;
      
      public State(double sum, long count) {
        this.sum = sum;
        this.count = count;
      }
    }
    
    @Override
    public State initialize() {
      return new State(0.0, 0);
    }
    
    @Override
    public State accumulate(State state, Double value) {
      if (value != null) {
        return new State(state.sum + value, state.count + 1);
      }
      return state;
    }
    
    @Override
    public State merge(State state1, State state2) {
      return new State(state1.sum + state2.sum, state1.count + state2.count);
    }
    
    @Override
    public Double finish(State state) {
      return state.count > 0 ? state.sum / state.count : null;
    }
  }
  
  public static void main(String[] args) {
    // Create session
    Session session = Session.builder()
      .configs(java.util.Map.of(
        "URL", "http://snowflake.reg.local:8082",
        "USER", "admin",
        "PASSWORD", "test",
        "ROLE", "accountadmin",
        "WAREHOUSE", "regress"
      ))
      .create();
    
    try {
      // Create sample data
      DataFrame sales = session.createDataFrame(
        new Row[] {
          Row.create("Electronics", 100.0),
          Row.create("Electronics", 200.0),
          Row.create("Electronics", 150.0),
          Row.create("Clothing", 50.0),
          Row.create("Clothing", 75.0)
        },
        new String[] {"category", "amount"}
      );
      
      // ================================================
      // Comparison: UDF vs UDTF vs UDAF
      // ================================================
      
      // 1. UDF: Transform each row (stateless)
      // Example: Double each amount
      // Input:  [100, 200, 150]
      // Output: [200, 400, 300]
      DataFrame udfResult = sales.withColumn(
        "doubled",
        callUDF("my_double_udf", col("amount"))  // Assumes UDF is registered
      );
      
      // 2. UDTF: Generate multiple rows from one row (optional state)
      // Example: Generate range from amount
      // Input:  [3]
      // Output: [0, 1, 2]
      DataFrame udtfResult = sales.join(
        callUDTF("my_range_udtf", col("amount"))  // Assumes UDTF is registered
      );
      
      // 3. UDAF: Aggregate multiple rows to single value (required state)
      // Example: Calculate average amount per category
      // Input:  [100, 200, 150]
      // Output: [150.0]
      
      // Register UDAF
      Column myAvg = session.udaf().registerTemporary("my_avg", new MyAvgUDAF());
      
      // Use UDAF in aggregation
      DataFrame udafResult = sales
        .groupBy("category")
        .agg(myAvg.apply(col("amount")).as("avg_amount"));
      
      System.out.println("=== UDAF Result: Average by Category ===");
      udafResult.show();
      
      // ================================================
      // More UDAF Examples
      // ================================================
      
      // Example 1: Simple aggregation (no groupBy)
      DataFrame totalAvg = sales.agg(
        myAvg.apply(col("amount")).as("overall_avg")
      );
      System.out.println("\n=== Overall Average ===");
      totalAvg.show();
      
      // Example 2: Multiple aggregations
      Column mySum = session.udaf().registerTemporary("my_sum", new MySumUDAF());
      Column myCount = session.udaf().registerTemporary("my_count", new MyCountUDAF());
      
      DataFrame multiAgg = sales
        .groupBy("category")
        .agg(
          myAvg.apply(col("amount")).as("avg"),
          mySum.apply(col("amount")).as("sum"),
          myCount.apply(col("amount")).as("count")
        );
      System.out.println("\n=== Multiple Aggregations ===");
      multiAgg.show();
      
      // Example 3: Register permanent UDAF
      session.udaf().registerPermanent("my_avg_perm", new MyAvgUDAF(), "@my_stage");
      
      DataFrame permResult = sales
        .groupBy("category")
        .agg(callUDAF("my_avg_perm", col("amount")).as("avg"));
      System.out.println("\n=== Permanent UDAF ===");
      permResult.show();
      
    } finally {
      session.close();
    }
  }
  
  // Additional UDAF examples
  
  static class MySumUDAF implements JavaUDAF1<Double, Double, Double> {
    public Double initialize() { return 0.0; }
    public Double accumulate(Double state, Double value) {
      return (value != null) ? state + value : state;
    }
    public Double merge(Double s1, Double s2) { return s1 + s2; }
    public Double finish(Double state) { return state; }
  }
  
  static class MyCountUDAF implements JavaUDAF1<Long, Double, Long> {
    public Long initialize() { return 0L; }
    public Long accumulate(Long state, Double value) {
      return (value != null) ? state + 1 : state;
    }
    public Long merge(Long s1, Long s2) { return s1 + s2; }
    public Long finish(Long state) { return state; }
  }
}

