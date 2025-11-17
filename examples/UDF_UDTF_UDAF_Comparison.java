package com.snowflake.snowpark.examples;

import com.snowflake.snowpark_java.*;
import com.snowflake.snowpark_java.udf.JavaUDF1;
import com.snowflake.snowpark_java.udtf.JavaUDTF1;
import com.snowflake.snowpark_java.udaf.JavaUDAF1;
import com.snowflake.snowpark_java.types.*;
import java.util.stream.Stream;
import static com.snowflake.snowpark_java.Functions.*;

/**
 * Side-by-side comparison of UDF, UDTF, and UDAF implementations.
 * All three process numbers but with different input/output patterns.
 */
public class UDF_UDTF_UDAF_Comparison {

  // ================================================================
  // UDF: Single Row → Single Value (Stateless)
  // ================================================================
  
  /**
   * UDF Example: Double a number.
   * Input:  10 → Output: 20
   * Input:  20 → Output: 40
   */
  static class DoubleValueUDF implements JavaUDF1<Double, Double> {
    @Override
    public Double call(Double value) {
      return value != null ? value * 2 : null;
    }
  }
  
  // ================================================================
  // UDTF: Single Row → Multiple Rows (Optional State)
  // ================================================================
  
  /**
   * UDTF Example: Generate range [0, value).
   * Input:  3 → Output: [0, 1, 2] (3 rows)
   * Input:  2 → Output: [0, 1] (2 rows)
   */
  static class RangeUDTF implements JavaUDTF1<Integer> {
    @Override
    public StructType outputSchema() {
      return StructType.create(new StructField("number", DataTypes.IntegerType));
    }
    
    @Override
    public Stream<Row> process(Integer count) {
      if (count == null || count <= 0) {
        return Stream.empty();
      }
      
      Stream.Builder<Row> builder = Stream.builder();
      for (int i = 0; i < count; i++) {
        builder.add(Row.create(i));
      }
      return builder.build();
    }
    
    @Override
    public Stream<Row> endPartition() {
      return Stream.empty();
    }
  }
  
  // ================================================================
  // UDAF: Multiple Rows → Single Value (Required State)
  // ================================================================
  
  /**
   * UDAF Example: Calculate SUM.
   * Input:  [10, 20, 30] → Output: 60 (1 value)
   */
  static class SumUDAF implements JavaUDAF1<Double, Double, Double> {
    @Override
    public Double initialize() {
      return 0.0;
    }
    
    @Override
    public Double accumulate(Double state, Double value) {
      return (value != null) ? state + value : state;
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
  
  // ================================================================
  // Complete Example with All Three
  // ================================================================
  
  public static void demonstrateAll(Session session) {
    // Create sample data
    DataFrame df = session.createDataFrame(
      new Row[] {
        Row.create("A", 10.0, 3),
        Row.create("A", 20.0, 2),
        Row.create("B", 30.0, 1)
      },
      new String[] {"category", "amount", "count"}
    );
    
    System.out.println("=== Original Data ===");
    df.show();
    // |category|amount|count|
    // |A       |10.0  |3    |
    // |A       |20.0  |2    |
    // |B       |30.0  |1    |
    
    // -------------------------------------------------------
    // UDF: Transform each row independently
    // -------------------------------------------------------
    UserDefinedFunction doubleUDF = session.udf().registerTemporary(new DoubleValueUDF());
    
    DataFrame udfResult = df.withColumn(
      "doubled_amount",
      doubleUDF.apply(col("amount"))
    );
    
    System.out.println("\n=== UDF Result (doubled amounts) ===");
    udfResult.show();
    // |category|amount|count|doubled_amount|
    // |A       |10.0  |3    |20.0          |
    // |A       |20.0  |2    |40.0          |
    // |B       |30.0  |1    |60.0          |
    
    // -------------------------------------------------------
    // UDTF: Expand each row into multiple rows
    // -------------------------------------------------------
    TableFunction rangeUDTF = session.udtf().registerTemporary(new RangeUDTF());
    
    DataFrame udtfResult = df.join(rangeUDTF, col("count"));
    
    System.out.println("\n=== UDTF Result (expanded ranges) ===");
    udtfResult.show();
    // |category|amount|count|number|
    // |A       |10.0  |3    |0     |
    // |A       |10.0  |3    |1     |
    // |A       |10.0  |3    |2     |
    // |A       |20.0  |2    |0     |
    // |A       |20.0  |2    |1     |
    // |B       |30.0  |1    |0     |
    
    // -------------------------------------------------------
    // UDAF: Aggregate multiple rows into single value
    // -------------------------------------------------------
    Column sumUDAF = session.udaf().registerTemporary("my_sum", new SumUDAF());
    
    DataFrame udafResult = df
      .groupBy("category")
      .agg(sumUDAF.apply(col("amount")).as("total_amount"));
    
    System.out.println("\n=== UDAF Result (aggregated sums) ===");
    udafResult.show();
    // |category|total_amount|
    // |A       |30.0        |
    // |B       |30.0        |
  }
  
  // ================================================================
  // Execution Model Comparison
  // ================================================================
  
  public static void demonstrateExecutionModel() {
    System.out.println("\n=== Execution Model Comparison ===\n");
    
    System.out.println("UDF (Stateless - processes one row at a time):");
    System.out.println("  Row 1: call(10) -> 20");
    System.out.println("  Row 2: call(20) -> 40");
    System.out.println("  Row 3: call(30) -> 60");
    System.out.println("  Result: [20, 40, 60] (3 values)\n");
    
    System.out.println("UDTF (Optional State - expands rows):");
    System.out.println("  Row 1: process(3) -> [0, 1, 2]");
    System.out.println("  Row 2: process(2) -> [0, 1]");
    System.out.println("  endPartition() -> []");
    System.out.println("  Result: [0,1,2,0,1] (5 rows)\n");
    
    System.out.println("UDAF (Required State - aggregates rows):");
    System.out.println("  initialize() -> 0");
    System.out.println("  Row 1: accumulate(0, 10) -> 10");
    System.out.println("  Row 2: accumulate(10, 20) -> 30");
    System.out.println("  Row 3: accumulate(30, 30) -> 60");
    System.out.println("  finish(60) -> 60");
    System.out.println("  Result: 60 (1 value)\n");
  }
  
  // ================================================================
  // When to Use Which?
  // ================================================================
  
  /**
   * Decision Guide:
   * 
   * Use UDF when:
   * - Transforming individual values
   * - No state needed between rows
   * - Examples: UPPER(), ROUND(), custom_parse()
   * 
   * Use UDTF when:
   * - Generating multiple rows from one row
   * - Flattening/expanding data
   * - Examples: SPLIT_TO_TABLE(), FLATTEN(), custom_explode()
   * 
   * Use UDAF when:
   * - Aggregating multiple rows into one value
   * - Computing statistics across rows
   * - Examples: SUM(), AVG(), custom_median(), custom_percentile()
   */
  public static void printDecisionGuide() {
    System.out.println("\n=== When to Use Which Function Type? ===\n");
    System.out.println("┌─────────┬──────────────┬───────────────┬──────────┐");
    System.out.println("│ Type    │ Input        │ Output        │ State    │");
    System.out.println("├─────────┼──────────────┼───────────────┼──────────┤");
    System.out.println("│ UDF     │ 1 row        │ 1 value       │ None     │");
    System.out.println("│ UDTF    │ 1 row        │ N rows        │ Optional │");
    System.out.println("│ UDAF    │ N rows       │ 1 value       │ Required │");
    System.out.println("└─────────┴──────────────┴───────────────┴──────────┘");
  }
}

