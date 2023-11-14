package com.snowflake.snowpark_test;

import static com.snowflake.snowpark_java.Functions.*;

import com.snowflake.snowpark.TestUtils;
import com.snowflake.snowpark.internal.JavaUtils;
import com.snowflake.snowpark_java.*;
import com.snowflake.snowpark_java.types.*;
import com.snowflake.snowpark_java.udtf.*;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.Test;

public class JavaUDTFSuite extends UDFTestBase {
  public JavaUDTFSuite() {}

  private boolean dependencyAdded = false;
  private String tableName = randomTableName();

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
  public void example() {
    String functionName = randomFunctionName();
    TableFunction tableFunction =
        getSession().udtf().registerTemporary(functionName, new MyRangeUdtf());
    // getSession().tableFunction(tableFunction, lit(10), lit(5)).show();
    DataFrame df = getSession().tableFunction(tableFunction, lit(10), lit(5));
    checkAnswer(
        df,
        new Row[] {Row.create(10), Row.create(11), Row.create(12), Row.create(13), Row.create(14)});
  }

  @Test
  public void basicTypes() {
    String crt =
        "create or replace temp table "
            + tableName
            + "(i1 smallint, i2 int, l1 bigint, f1 float, d1 double, "
            + "decimal number(38, 18), b boolean, s string, bi binary)";
    runQuery(crt);
    String insert =
        "insert into "
            + tableName
            + " values "
            + "(-1, -2, -3, -2.2, -4.4, -123.456, false, 'java', '6279746573' :: binary),"
            + "(1, 2, 3, 2.2, 4.4, 123.456, true, 'scala', '6279746573' :: binary),"
            + "(null, null, null, null, null, null, null, null, null)";
    runQuery(insert);
    TableFunction tableFunction = getSession().udtf().registerTemporary(new UDTFBasicTypes());

    DataFrame df =
        getSession()
            .table(tableName)
            .join(
                tableFunction,
                col("i1"),
                col("i2"),
                col("l1"),
                col("f1"),
                col("d1"),
                col("decimal"),
                col("b"),
                col("s"),
                col("bi"));

    String schema = TestUtils.treeString(df.schema(), 0);
    String expectedSchema =
        new StringBuilder()
            .append("root")
            .append("|--I1: Long (nullable = true)")
            .append("|--I2: Long (nullable = true)")
            .append("|--L1: Long (nullable = true)")
            .append("|--F1: Double (nullable = true)")
            .append("|--D1: Double (nullable = true)")
            .append("|--DECIMAL: Decimal(38, 18) (nullable = true)")
            .append("|--B: Boolean (nullable = true)")
            .append("|--S: String (nullable = true)")
            .append("|--BI: Binary (nullable = true)")
            .append("|--SHORT: Long (nullable = true)")
            .append("|--INT: Long (nullable = true)")
            .append("|--LONG: Long (nullable = true)")
            .append("|--FLOAT: Double (nullable = true)")
            .append("|--DOUBLE: Double (nullable = true)")
            .append("|--DECIMAL: Decimal(38, 18) (nullable = true)")
            .append("|--BOOLEAN: Boolean (nullable = true)")
            .append("|--STRING: String (nullable = true)")
            .append("|--BINARY: Binary (nullable = true)")
            .toString();
    assert schema.replaceAll("\\W", "").equals(expectedSchema.replaceAll("\\W", ""));

    Row[] expectRows =
        new Row[] {
          Row.create(
              -1,
              -2,
              -3,
              -2.2,
              -4.4,
              -123.456,
              false,
              "java",
              "bytes".getBytes(),
              -1,
              -2,
              -3,
              -2.2,
              -4.4,
              -123.456,
              false,
              "java",
              "bytes".getBytes()),
          Row.create(
              1,
              2,
              3,
              2.2,
              4.4,
              123.456,
              true,
              "scala",
              "bytes".getBytes(),
              1,
              2,
              3,
              2.2,
              4.4,
              123.456,
              true,
              "scala",
              "bytes".getBytes()),
          Row.create(
              null, null, null, null, null, null, null, null, null, null, null, null, null, null,
              null, null, null, null)
        };
    checkAnswer(df, expectRows);
  }

  // ---------------------------------------------------------------------------------
  // Date Time and Timestamp are tested in JavaUDTFNonStoredProcSuite.dateTimestamp()
  // ---------------------------------------------------------------------------------

  @Test
  public void complexTypes() {
    String crt =
        "create or replace temp table "
            + tableName
            + "(s1 String, v1 Variant, sa1 Array, va1 Array)";
    runQuery(crt);
    String insert =
        "insert into "
            + tableName
            + " select 'key', to_variant('key')"
            + ", array_construct('key1', 'key2'), array_construct('key1', 'key2')";
    runQuery(insert);
    runQuery("insert into " + tableName + " values (null, null, null, null)");

    TableFunction tableFunction = getSession().udtf().registerTemporary(new UDTFComplexTypes());

    DataFrame df =
        getSession()
            .table(tableName)
            .join(tableFunction, col("s1"), col("v1"), col("sa1"), col("va1"));

    String schema = TestUtils.treeString(df.schema(), 0);
    String expectedSchema =
        "root\n"
            + " |--S1: String (nullable = true)\n"
            + " |--V1: Variant (nullable = true)\n"
            + " |--SA1: Array (nullable = true)\n"
            + " |--VA1: Array (nullable = true)\n"
            + " |--KEY: String (nullable = true)\n"
            + " |--VARIANT: Variant (nullable = true)\n"
            + " |--STRING_ARRAY: Array (nullable = true)\n"
            + " |--VARIANT_ARRAY: Array (nullable = true)";
    assert schema.replaceAll("\\W", "").equals(expectedSchema.replaceAll("\\W", ""));

    Row[] expectRows =
        new Row[] {
          Row.create(
              "key",
              "\"key\"",
              "[\n  \"key1\",\n  \"key2\"\n]",
              "[\n  \"key1\",\n  \"key2\"\n]",
              "new_key",
              "\"new_key\"",
              "[\n  \"new_key1\",\n  \"new_key2\"\n]",
              "[\n  \"new_key1\",\n  \"new_key2\"\n]"),
          Row.create(null, null, null, null, null, null, null, null)
        };
    checkAnswer(df, expectRows);
  }

  @Test
  public void testMapTypes() {
    String crt = "create or replace temp table " + tableName + "(sm object, vm object)";
    runQuery(crt);
    runQuery("insert into " + tableName + " values (null, null)");
    String insert =
        "insert into "
            + tableName
            + " select object_construct('key_1', 'value_1'), object_construct('key_2', 'value_2')";
    runQuery(insert);
    TableFunction tableFunction = getSession().udtf().registerTemporary(new UDTFMapTypes());
    DataFrame df = getSession().table(tableName).join(tableFunction, col("sm"), col("vm"));

    String schema = TestUtils.treeString(df.schema(), 0);
    String expectedSchema =
        "root\n"
            + " |--SM: Map (nullable = true)\n"
            + " |--VM: Map (nullable = true)\n"
            + " |--MAPPINGS: String (nullable = true)\n"
            + " |--STRING_MAP_OUTPUT: Map (nullable = true)\n"
            + " |--VARIANT_MAP_OUTPUT: Map (nullable = true)";
    assert schema.replaceAll("\\W", "").equals(expectedSchema.replaceAll("\\W", ""));

    Row[] expectRows =
        new Row[] {
          Row.create(null, null, "key -> new_key", null, null),
          Row.create(
              "{\n  \"key_1\": \"value_1\"\n}",
              "{\n  \"key_2\": \"value_2\"\n}",
              "key -> new_key",
              "{\n  \"new_key_1\": \"new_value_1\"\n}",
              "{\n  \"new_key_2\": \"new_value_2\"\n}")
        };
    checkAnswer(df, expectRows);
  }

  @Test
  public void testLargeUdtfAndEndPartition() {
    JavaUDTF largeUdtf = new LargeJavaUDTF(1024);
    assert JavaUtils.serialize(largeUdtf).length > 8192;
    String funcName = randomFunctionName();
    getSession().udtf().registerTemporary(funcName, largeUdtf);
    DataFrame df = getSession().tableFunction(new TableFunction(funcName), lit(2));
    checkAnswer(df, new Row[] {Row.create(2), Row.create(1024)}, false);

    // Register twice
    String funcName2 = randomFunctionName();
    assert !funcName2.equalsIgnoreCase(funcName);
    TableFunction tableFunction = getSession().udtf().registerTemporary(funcName2, largeUdtf);
    DataFrame df2 = getSession().tableFunction(tableFunction, lit(3));
    checkAnswer(df2, new Row[] {Row.create(3), Row.create(1024)}, false);
  }

  @Test
  public void testNegativeCases() {
    Exception ex = null;
    // case 1: Invalid output column name
    try {
      getSession().udtf().registerTemporary(new NegativeInvalidNameJavaUdtf());
    } catch (Exception e) {
      ex = e;
    }
    assert ex != null && ex.getMessage().contains("Error Code: 0208");

    // case 2: Java UDTF doesn't inherit from JavaUDTFX
    ex = null;
    try {
      getSession().udtf().registerTemporary(new NegativeInvalidJavaUdtf());
    } catch (Exception e) {
      ex = e;
    }
    assert ex != null
        && ex.getMessage().contains("internal error: Java UDTF doesn't inherit from JavaUDTFX");

    // case 3: Cannot infer data types for Map
    ex = null;
    try {
      getSession().udtf().registerTemporary(new NegativeInferMapTypes());
    } catch (Exception e) {
      ex = e;
    }
    assert ex != null && ex.getMessage().contains("Error Code: 0209");

    // case 4: Unsupported type
    ex = null;
    try {
      getSession().udtf().registerTemporary(new NegativeUnsupportedType());
    } catch (Exception e) {
      ex = e;
    }
    assert ex != null
        && ex.getMessage().contains("Unsupported data type: com.snowflake.snowpark_test.TestClass");

    // case 5: Define multiple process() functions type
    ex = null;
    try {
      getSession().udtf().registerTemporary(new NegativeDefineTwoProcessFunctions());
    } catch (Exception e) {
      ex = e;
    }
    assert ex != null && ex.getMessage().contains("Error Code: 0210");
  }

  @Test
  public void partitionBy() {
    JavaUDTF1<String> tableFunc1 = new WordCount();
    DataFrame df =
        getSession()
            .createDataFrame(
                new Row[] {
                  Row.create("a", "b"),
                  Row.create("a", "c"),
                  Row.create("a", "b"),
                  Row.create("d", "e")
                },
                new StructType(
                    new StructField[] {
                      new StructField("a", DataTypes.StringType),
                      new StructField("b", DataTypes.StringType)
                    }));

    TableFunction tf = getSession().udtf().registerTemporary(tableFunc1);
    DataFrame result1 =
        df.join(
            tf, new Column[] {df.col("b")}, new Column[] {df.col("a")}, new Column[] {df.col("b")});

    checkAnswer(
        result1,
        new Row[] {Row.create("a", null, "{b=2, c=1}"), Row.create("d", null, "{e=1}")},
        true);

    DataFrame result2 = df.join(tf, new Column[] {df.col("b")}, new Column[] {}, new Column[] {});
    result2.show();

    Map<String, Column> map = new HashMap<>();
    map.put("arg1", df.col("b"));

    DataFrame result3 = df.join(tf, map, new Column[] {df.col("a")}, new Column[] {df.col("b")});
    checkAnswer(
        result3,
        new Row[] {Row.create("a", null, "{b=2, c=1}"), Row.create("d", null, "{e=1}")},
        true);

    DataFrame result4 = df.join(tf, map, new Column[] {}, new Column[] {});
    result4.show();

    DataFrame result5 = df.join(tf.call(map), new Column[] {df.col("a")}, new Column[] {df.col("b")});
    checkAnswer(
            result5,
            new Row[] {Row.create("a", null, "{b=2, c=1}"), Row.create("d", null, "{e=1}")},
            true);
  }
}

class WordCount implements JavaUDTF1<String> {
  Map<String, Integer> freg = new HashMap<>();

  @Override
  public Stream<Row> process(String colValue) {
    int curValue = freg.getOrDefault(colValue, 0);
    freg.put(colValue, curValue + 1);
    return Stream.empty();
  }

  @Override
  public StructType outputSchema() {
    return new StructType(new StructField[] {new StructField("FREQUENCIES", DataTypes.StringType)});
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.of(Row.create(freg.toString()));
  }
}

class UDTFBasicTypes
    implements JavaUDTF9<
        Short, Integer, Long, Float, Double, java.math.BigDecimal, Boolean, String, byte[]> {
  @Override
  public Stream<Row> process(
      Short si1,
      Integer i1,
      Long li1,
      Float f1,
      Double d1,
      java.math.BigDecimal decimal,
      Boolean b1,
      String str,
      byte[] bytes) {
    Row row = Row.create(si1, i1, li1, f1, d1, decimal, b1, str, bytes);
    return Stream.of(row);
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public StructType outputSchema() {
    return StructType.create(
        new StructField("short", DataTypes.ShortType),
        new StructField("int", DataTypes.IntegerType),
        new StructField("long", DataTypes.LongType),
        new StructField("float", DataTypes.FloatType),
        new StructField("double", DataTypes.DoubleType),
        new StructField("decimal", DataTypes.createDecimalType(38, 18)),
        new StructField("boolean", DataTypes.BooleanType),
        new StructField("string", DataTypes.StringType),
        new StructField("binary", DataTypes.BinaryType));
  }
}

class UDTFComplexTypes implements JavaUDTF4<String, Variant, String[], Variant[]> {
  @Override
  public Stream<Row> process(String s, Variant v, String[] sa, Variant[] va) {
    // Replace "key" as "new_key" for all input values
    String new_s = (s == null) ? null : s.replaceAll("key", "new_key");
    Variant new_v = (v == null) ? null : new Variant(v.toString().replaceAll("key", "new_key"));
    String[] new_sa = null;
    if (sa != null) {
      new_sa = new String[sa.length];
      for (int i = 0; i < sa.length; i++) {
        if (sa[i] == null) {
          new_sa[i] = null;
        } else {
          new_sa[i] = sa[i].replaceAll("key", "new_key");
        }
      }
    }
    Variant[] new_va = null;
    if (va != null) {
      new_va = new Variant[va.length];
      for (int i = 0; i < va.length; i++) {
        if (va[i] == null) {
          new_va[i] = null;
        } else {
          new_va[i] = new Variant(va[i].toString().replaceAll("key", "new_key"));
        }
      }
    }
    Row row = Row.create(new_s, new_v, new_sa, new_va);
    return Stream.of(row);
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public StructType outputSchema() {
    return StructType.create(
        new StructField("key", DataTypes.StringType),
        new StructField("variant", DataTypes.VariantType),
        new StructField("string_array", DataTypes.createArrayType(DataTypes.StringType)),
        new StructField("variant_array", DataTypes.createArrayType(DataTypes.VariantType)));
  }
}

class UDTFMapTypes implements JavaUDTF2<Map<String, String>, Map<String, Variant>> {
  @Override
  public Stream<Row> process(Map<String, String> sm, Map<String, Variant> vm) {
    Map<String, String> new_sm = null;
    if (sm != null) {
      new_sm = new HashMap<>();
      for (Map.Entry<String, String> entry : sm.entrySet()) {
        new_sm.put(
            entry.getKey().replaceAll("key", "new_key"),
            entry.getValue().replaceAll("value", "new_value"));
      }
    }
    Map<String, Variant> new_vm = null;
    if (vm != null) {
      new_vm = new HashMap<>();
      for (Map.Entry<String, Variant> entry : vm.entrySet()) {
        new_vm.put(
            entry.getKey().replaceAll("key", "new_key"),
            new Variant(entry.getValue().toString().replaceAll("value", "new_value")));
      }
    }
    Row row = Row.create("key -> new_key", new_sm, new_vm);
    return Stream.of(row);
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public StructType outputSchema() {
    return StructType.create(
        new StructField("mappings", DataTypes.StringType),
        new StructField(
            "string_map_output",
            DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)),
        new StructField(
            "variant_map_output",
            DataTypes.createMapType(DataTypes.StringType, DataTypes.VariantType)));
  }

  @Override
  public StructType inputSchema() {
    return StructType.create(
        new StructField(
            "string_map_input",
            DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)),
        new StructField(
            "variant_map_input",
            DataTypes.createMapType(DataTypes.StringType, DataTypes.VariantType)));
  }
}

class LargeJavaUDTF implements JavaUDTF1<Integer> {
  private Integer[] largeData;

  public LargeJavaUDTF(int count) {
    largeData = new Integer[count];
    for (int i = 0; i < count; i++) {
      largeData[i] = i;
    }
  }

  @Override
  public StructType outputSchema() {
    return StructType.create(new StructField("echo", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.of(Row.create(largeData.length));
  }

  @Override
  public Stream<Row> process(Integer arg0) {
    return Stream.of(Row.create(arg0));
  }
}

class NegativeInvalidNameJavaUdtf implements JavaUDTF1<String> {
  @Override
  public StructType outputSchema() {
    return StructType.create(new StructField("invalid name", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(String arg0) {
    return Stream.of(Row.create("abc"));
  }
}

class NegativeInvalidJavaUdtf implements JavaUDTF {
  @Override
  public StructType outputSchema() {
    return StructType.create(new StructField("not_inherit_from_JavaUDTFX", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }
}

class NegativeInferMapTypes implements JavaUDTF1<Map<String, String>> {
  @Override
  public StructType outputSchema() {
    return StructType.create(new StructField("name", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(Map<String, String> arg0) {
    return Stream.of(Row.create(arg0));
  }
}

class TestClass {}

class NegativeUnsupportedType implements JavaUDTF1<TestClass> {
  @Override
  public StructType outputSchema() {
    return StructType.create(new StructField("name", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(TestClass arg0) {
    return Stream.of(Row.create(arg0));
  }
}

class NegativeDefineTwoProcessFunctions implements JavaUDTF2<String, String> {
  @Override
  public StructType outputSchema() {
    return StructType.create(new StructField("name", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(String s1, String s2) {
    return Stream.of(Row.create(s1));
  }

  // If user defined multiple process() function with target expected argument count,
  // Snowpark cannot infer schema for it.
  public Stream<Row> process(Integer i1, Integer i2) {
    return Stream.of(Row.create(i1));
  }
}

// import java.util.stream.Stream;
class MyRangeUdtf implements JavaUDTF2<Integer, Integer> {
  @Override
  public StructType outputSchema() {
    return StructType.create(new StructField("C1", DataTypes.IntegerType));
  }

  @Override
  // It is optional in this example
  public StructType inputSchema() {
    return StructType.create(
        new StructField("start_value", DataTypes.IntegerType),
        new StructField("value_count", DataTypes.IntegerType));
  }

  @Override
  public Stream<Row> endPartition() {
    return Stream.empty();
  }

  @Override
  public Stream<Row> process(Integer start, Integer count) {
    Stream.Builder<Row> builder = Stream.builder();
    for (int i = start; i < start + count; i++) {
      builder.add(Row.create(i));
    }
    return builder.build();
  }
}
