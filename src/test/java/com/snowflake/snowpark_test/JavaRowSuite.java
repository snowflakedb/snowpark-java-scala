package com.snowflake.snowpark_test;

import static org.junit.Assert.assertThrows;

import com.snowflake.snowpark_java.DataFrame;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.types.*;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class JavaRowSuite extends TestBase {

  @Test
  public void createList() {
    Row row = Row.create(1, "a");
    List<Object> list = row.toList();
    assert list.size() == 2;
    assert list.get(0).equals(1);
    assert list.get(1).equals("a");

    Object[] arr = {3, 4};
    Row row1 = new Row(arr);
    assert row1.toList().size() == 2;
  }

  @Test
  public void size() {
    assert Row.create(1, 2, 3, 4).size() == 4;
  }

  @Test
  public void cloneTest() {
    Row row = Row.create(1, 2, 3);
    Row cloned = row.clone();

    assert cloned.size() == row.size();
    assert cloned.get(0).equals(1);
    assert cloned.get(1).equals(2);
    assert cloned.get(2).equals(3);
  }

  @Test
  public void equalsTest() {
    Row row1 = Row.create(1, 2, 3);
    Row row2 = Row.create(1, 2, 3);
    Row row3 = Row.create(2, 3);
    assert row1.equals(row2);
    assert !row1.equals(row3);
    assert row1.equals(row1.clone());
    assert row1.equals(row2.clone());
    assert !row1.equals(row3.clone());

    assert !row1.equals("123");
    assert row1.hashCode() == row2.hashCode();
    assert row1.hashCode() != row3.hashCode();
  }

  @Test
  public void getters1() {
    Row row = Row.create(null, true, (byte) 1, (short) 2, 3, 4L, 5.5f, 6.6, "a");

    assert row.isNullAt(0);
    assert !row.isNullAt(1);
    assert row.getBoolean(1);
    assert (boolean) row.get(1);
    assert row.getByte(2) == 1;
    assert row.getShort(3) == 2;
    assert row.getInt(4) == 3;
    assert row.getLong(5) == 4L;
    assert row.getFloat(6) == 5.5f;
    assert row.getDouble(7) == 6.6;
    assert row.getString(8).equals("a");

    assert row.toString().equals("Row[null,true,1,2,3,4,5.5,6.6,\"a\"]");
  }

  @Test
  public void getters2() {
    byte[] binary = {(byte) 1, (byte) 2};
    Row row =
        Row.create(
            binary,
            new Variant(3),
            Geography.fromGeoJSON("{\"type\":\"Point\",\"coordinates\":[30,10]}"),
            new BigDecimal(12345),
            Geometry.fromGeoJSON(
                "{\"coordinates\": [3.000000000000000e+01,1.000000000000000e+01],\"type\":"
                    + " \"Point\"}"));

    assert row.size() == 5;
    assert Arrays.equals(row.getBinary(0), binary);
    assert row.getVariant(1).equals(new Variant(3));
    assert row.getGeography(2)
        .equals(Geography.fromGeoJSON("{\"type\":\"Point\",\"coordinates\":[30,10]}"));
    assert row.getDecimal(3).equals(new BigDecimal(12345));
    assert row.getGeometry(4)
        .equals(
            Geometry.fromGeoJSON(
                "{\"coordinates\": [3.000000000000000e+01,1.000000000000000e+01],\"type\":"
                    + " \"Point\"}"));

    assert row.toString()
        .equals(
            "Row[Binary(1,2),3,{\"type\":\"Point\",\"coordinates\":[30,10]},12345,{\"coordinates\":"
                + " [3.000000000000000e+01,1.000000000000000e+01],\"type\": \"Point\"}]");
  }

  @Test
  public void getter3() {
    Row row = Row.create("[1,2,3]", "{\"a\":1,\"b\":2}");
    assert row.size() == 2;
    List<Variant> list = row.getListOfVariant(0);
    assert list.size() == 3;
    assert list.get(0).asInt() == 1;
    assert list.get(1).asInt() == 2;
    assert list.get(2).asInt() == 3;
    Map<String, Variant> map = row.getMapOfVariant(1);
    assert map.size() == 2;
    assert map.get("a").asInt() == 1;
    assert map.get("b").asInt() == 2;

    assert row.toString().equals("Row[\"[1,2,3]\",\"{\"a\":1,\"b\":2}\"]");
  }

  @Test
  public void getter4() {
    Row row = Row.create(new Time(0), Date.valueOf("2020-11-11"), new Timestamp(0));

    assert row.getTime(0).equals(new Time(0));
    assert row.getDate(1).equals(Date.valueOf("2020-11-11"));
    assert row.getTimestamp(2).equals(new Timestamp(0));
  }

  @Test
  public void getter5() {
    Row row =
        Row.create(
            new com.snowflake.snowpark.types.Variant(1),
            com.snowflake.snowpark.types.Geography.fromGeoJSON(
                "{\"type\":\"Point\",\"coordinates\":[30,10]}"),
            com.snowflake.snowpark.types.Geometry.fromGeoJSON(
                "{\"coordinates\": [2.000000000000000e+01,4.000000000000000e+01],\"type\":"
                    + " \"Point\"}"));

    assert row.get(0) instanceof Variant;
    assert row.get(1) instanceof Geography;
    assert row.get(2) instanceof Geometry;
  }

  @Test
  public void testArray() {
    // String array
    String[] strArray = {"a", "b", null};
    Row row = Row.create((Object) strArray);
    assert row.size() == 1;
    // getVariant
    Variant[] values = row.getVariant(0).asArray();
    assert values.length == 3;
    assert values[0].asString().equals("a")
        && values[1].asString().equals("b")
        && values[2].asString().equals("null");

    // Variant Array
    Variant[] variantArray = {new Variant("a"), new Variant("b"), null};
    Row row2 = Row.create((Object) variantArray);
    assert row2.size() == 1;
    // getVariant
    Variant[] values2 = row2.getVariant(0).asArray();
    assert values2.length == 3;
    assert values2[0].equals(new Variant("a"))
        && values2[1].equals(new Variant("b"))
        && values2[2].asString().equals("null");
    // get()
    Variant[] getValues2 = (Variant[]) row2.get(0);
    assert getValues2.length == 3;
    assert getValues2[0].equals(new Variant("a"))
        && getValues2[1].equals(new Variant("b"))
        && getValues2[2] == null;
  }

  @Test
  public void testEmptyArray() {
    Row row = null;

    // Empty String Array
    String[] emptyStringArray = new String[0];
    row = Row.create((Object) emptyStringArray);
    assert row.getList(0).isEmpty();
    assert row.getVariant(0).asArray().length == 0;

    // Empty Variant Array
    Variant[] emptyVariantArray = new Variant[0];
    row = Row.create((Object) emptyVariantArray);
    assert ((Variant[]) row.get(0)).length == 0;
    assert row.getVariant(0).asArray().length == 0;
  }

  @Test
  public void testSpecialArray() {
    Row row = null;

    // String Array with all values to be null
    String[] stringArrayAllNull = new String[3];
    stringArrayAllNull[0] = null;
    stringArrayAllNull[1] = null;
    stringArrayAllNull[2] = null;
    row = Row.create((Object) stringArrayAllNull);
    // getVariant
    Variant[] values = row.getVariant(0).asArray();
    assert values.length == 3;
    assert values[0].asString().equals("null")
        && values[1].asString().equals("null")
        && values[2].asString().equals("null");
    // Variant Array with all values to be null
    Variant[] variantArrayAllNull = new Variant[3];
    variantArrayAllNull[0] = null;
    variantArrayAllNull[1] = null;
    variantArrayAllNull[2] = null;
    row = Row.create((Object) variantArrayAllNull);
    // getVariant
    Variant[] values2 = row.getVariant(0).asArray();
    assert values2.length == 3;
    assert values2[0].asString().equals("null")
        && values2[1].asString().equals("null")
        && values2[2].asString().equals("null");
    // get()
    Variant[] getValues2 = (Variant[]) row.get(0);
    assert getValues2.length == 3;
    assert getValues2[0] == null && getValues2[1] == null && getValues2[2] == null;
  }

  @Test
  public void testMap() {
    // String Map
    Map<String, String> strMap = new HashMap<>();
    strMap.put("a", "av");
    strMap.put("b", "bv");
    strMap.put("c", null);
    Row row = Row.create(strMap);
    assert row.size() == 1;
    // getVariant
    Map<String, Variant> mapValues = row.getVariant(0).asMap();
    assert mapValues.size() == 3;
    assert mapValues.get("a").asString().equals("av")
        && mapValues.get("b").asString().equals("bv")
        && mapValues.get("c").asString().equals("null");
    // get()
    Map<String, String> getValues = (Map<String, String>) row.get(0);
    assert getValues.size() == 3;
    assert getValues.get("a").equals("av")
        && getValues.get("b").equals("bv")
        && getValues.get("c") == null;

    // Variant Map
    Map<String, Variant> variantMap = new HashMap<>();
    variantMap.put("a", new Variant("av"));
    variantMap.put("b", new Variant("bv"));
    variantMap.put("c", null);
    Row row2 = Row.create(variantMap);
    assert row2.size() == 1;
    // getVariant
    Map<String, Variant> mapValues2 = row2.getVariant(0).asMap();
    assert mapValues2.size() == 3;
    assert mapValues2.get("a").asString().equals("av")
        && mapValues2.get("b").asString().equals("bv")
        && mapValues2.get("c").asString().equals("null");
    // get()
    Map<String, Variant> getValues2 = (Map<String, Variant>) row2.get(0);
    assert getValues2.size() == 3;
    assert getValues2.get("a").equals(new Variant("av"))
        && getValues2.get("b").equals(new Variant("bv"))
        && getValues2.get("c") == null;
  }

  @Test
  public void testEmptyMap() {
    // empty String Map
    Map<String, String> emptyStringMap = new HashMap<>();
    Row row = Row.create(emptyStringMap);
    // getVariant
    Map<String, Variant> mapValues = row.getVariant(0).asMap();
    assert mapValues.size() == 0;
    // get()
    Map<String, String> getValues = (Map<String, String>) row.get(0);
    assert getValues.size() == 0;

    // Variant Map
    Map<String, Variant> emptyVariantMap = new HashMap<>();
    Row row2 = Row.create(emptyVariantMap);
    // getVariant
    Map<String, Variant> mapValues2 = row2.getVariant(0).asMap();
    assert mapValues2.size() == 0;
    // get()
    Map<String, Variant> getValues2 = (Map<String, Variant>) row2.get(0);
    assert getValues2.size() == 0;
  }

  @Test
  public void testSpecialMap() {
    // String Map wth all values to be null
    Map<String, String> strMapAllNull = new HashMap<>();
    strMapAllNull.put("a", null);
    strMapAllNull.put("b", null);
    strMapAllNull.put("c", null);
    Row row = Row.create(strMapAllNull);
    assert row.size() == 1;
    // getVariant
    Map<String, Variant> mapValues = row.getVariant(0).asMap();
    assert mapValues.size() == 3;
    assert mapValues.get("a").asString().equals("null")
        && mapValues.get("b").asString().equals("null")
        && mapValues.get("c").asString().equals("null");
    // get()
    Map<String, String> getValues = (Map<String, String>) row.get(0);
    assert getValues.size() == 3;
    assert getValues.get("a") == null && getValues.get("b") == null && getValues.get("c") == null;

    // Variant Map with all values to be null
    Map<String, Variant> variantMapAllNull = new HashMap<>();
    variantMapAllNull.put("a", null);
    variantMapAllNull.put("b", null);
    variantMapAllNull.put("c", null);
    Row row2 = Row.create(variantMapAllNull);
    assert row2.size() == 1;
    // getVariant
    Map<String, Variant> mapValues2 = row2.getVariant(0).asMap();
    assert mapValues2.size() == 3;
    assert mapValues2.get("a").asString().equals("null")
        && mapValues2.get("b").asString().equals("null")
        && mapValues2.get("c").asString().equals("null");
    // get()
    Map<String, Variant> getValues2 = (Map<String, Variant>) row2.get(0);
    assert getValues2.size() == 3;
    assert getValues2.get("a") == null
        && getValues2.get("b") == null
        && getValues2.get("c") == null;
  }

  @Test
  public void testGetList() {
    structuredTypeTest(
        () -> {
          DataFrame df = getSession().sql("select [[1, 2], [3]]::ARRAY(ARRAY(NUMBER)) AS arr1");
          StructType schema = df.schema();
          assert schema.get(0).dataType() instanceof ArrayType;
          assert ((ArrayType) schema.get(0).dataType()).getElementType() instanceof ArrayType;

          List<?> list = df.collect()[0].getList(0);
          assert list.size() == 2;

          List<?> list1 = (List<?>) list.get(0);
          List<?> list2 = (List<?>) list.get(1);

          assert list1.size() == 2;
          assert list2.size() == 1;

          assert (Long) list1.get(0) == 1;
          assert (Long) list1.get(1) == 2;
          assert (Long) list2.get(0) == 3;
        },
        getSession());
  }

  @Test
  public void testGetMap() {
    structuredTypeTest(
        () -> {
          DataFrame df =
              getSession()
                  .sql(
                      "select {'1':{'a':1,'b':2},'2':{'c':3}} :: MAP(NUMBER, MAP(VARCHAR, NUMBER))"
                          + " as map");

          StructType schema = df.schema();
          assert schema.get(0).dataType() instanceof MapType;
          assert ((MapType) schema.get(0).dataType()).getKeyType() instanceof LongType;
          assert ((MapType) schema.get(0).dataType()).getValueType() instanceof MapType;

          Map<?, ?> map = df.collect()[0].getMap(0);
          Map<?, ?> map1 = (Map<?, ?>) map.get(1L);
          assert map1.size() == 2;
          assert (Long) map1.get("a") == 1;
          assert (Long) map1.get("b") == 2;

          Map<?, ?> map2 = (Map<?, ?>) map.get(2L);
          assert map2.size() == 1;
          assert (Long) map2.get("c") == 3;
        },
        getSession());
  }

  @Test
  public void testGetRow() {
    structuredTypeTest(
        () -> {
          DataFrame df =
              getSession()
                  .sql(
                      "select {'a': {'b': {'d':10,'c': 'txt'}}} :: OBJECT(a OBJECT(b OBJECT(c"
                          + " VARCHAR, d NUMBER))) as obj1");
          StructType schema = df.schema();
          schema.printTreeString();
          assert schema.get(0).dataType() instanceof StructType;
          assert schema.get(0).name().equals("OBJ1");
          StructType sub1 = (StructType) schema.get(0).dataType();
          assert sub1.size() == 1;
          assert sub1.get(0).dataType() instanceof StructType;
          assert sub1.get(0).name().equals("A");
          StructType sub2 = (StructType) sub1.get(0).dataType();
          assert sub2.size() == 1;
          assert sub2.get(0).dataType() instanceof StructType;
          assert sub2.get(0).name().equals("B");
          StructType sub3 = (StructType) sub2.get(0).dataType();
          assert sub3.size() == 2;
          assert sub3.get(0).dataType() instanceof StringType;
          assert sub3.get(0).name().equals("C");
          assert sub3.get(1).dataType() instanceof LongType;
          assert sub3.get(1).name().equals("D");

          Row[] rows1 = df.collect();
          assert rows1.length == 1;
          Row row1 = rows1[0].getObject(0);
          assert row1.size() == 1;
          Row row2 = row1.getObject(0);
          assert row2.size() == 1;
          Row row3 = row2.getObject(0);
          assert row3.size() == 2;
          assert row3.getString(0).equals("txt");
          assert row3.getLong(1) == 10;
        },
        getSession());
  }

  @Test
  public void getAs() {
    withTimeZoneTest(
        () -> {
          long milliseconds = System.currentTimeMillis();

          StructType schema =
              StructType.create(
                  new StructField("c01", DataTypes.BinaryType),
                  new StructField("c02", DataTypes.BooleanType),
                  new StructField("c03", DataTypes.ByteType),
                  new StructField("c04", DataTypes.DateType),
                  new StructField("c05", DataTypes.DoubleType),
                  new StructField("c06", DataTypes.FloatType),
                  new StructField("c07", DataTypes.GeographyType),
                  new StructField("c08", DataTypes.GeometryType),
                  new StructField("c09", DataTypes.IntegerType),
                  new StructField("c10", DataTypes.LongType),
                  new StructField("c11", DataTypes.ShortType),
                  new StructField("c12", DataTypes.StringType),
                  new StructField("c13", DataTypes.TimeType),
                  new StructField("c14", DataTypes.TimestampType),
                  new StructField("c15", DataTypes.VariantType));

          Row[] data = {
            Row.create(
                new byte[] {1, 2},
                true,
                Byte.MIN_VALUE,
                Date.valueOf("2024-01-01"),
                Double.MIN_VALUE,
                Float.MIN_VALUE,
                Geography.fromGeoJSON("POINT(30 10)"),
                Geometry.fromGeoJSON("POINT(20 40)"),
                Integer.MIN_VALUE,
                Long.MIN_VALUE,
                Short.MIN_VALUE,
                "string",
                Time.valueOf("16:23:04"),
                new Timestamp(milliseconds),
                new Variant(1))
          };

          DataFrame df = getSession().createDataFrame(data, schema);
          Row row = df.collect()[0];

          assert Arrays.equals(row.getAs(0, byte[].class), new byte[] {1, 2});
          assert row.getAs(1, Boolean.class);
          assert row.getAs(2, Byte.class) == Byte.MIN_VALUE;
          assert row.getAs(3, Date.class).equals(Date.valueOf("2024-01-01"));
          assert row.getAs(4, Double.class) == Double.MIN_VALUE;
          assert row.getAs(5, Float.class) == Float.MIN_VALUE;
          assert row.getAs(6, Geography.class)
              .equals(
                  Geography.fromGeoJSON(
                      "{\n  \"coordinates\": [\n    30,\n    10\n  ],\n  \"type\": \"Point\"\n}"));
          assert row.getAs(7, Geometry.class)
              .equals(
                  Geometry.fromGeoJSON(
                      "{\n"
                          + "  \"coordinates\": [\n"
                          + "    2.000000000000000e+01,\n"
                          + "    4.000000000000000e+01\n"
                          + "  ],\n"
                          + "  \"type\": \"Point\"\n"
                          + "}"));
          assert row.getAs(8, Integer.class) == Integer.MIN_VALUE;
          assert row.getAs(9, Long.class) == Long.MIN_VALUE;
          assert row.getAs(10, Short.class) == Short.MIN_VALUE;
          assert row.getAs(11, String.class).equals("string");
          assert row.getAs(12, Time.class).equals(Time.valueOf("16:23:04"));
          assert row.getAs(13, Timestamp.class).equals(new Timestamp(milliseconds));
          assert row.getAs(14, Variant.class).equals(new Variant(1));

          Row finalRow = row;
          assertThrows(
              ClassCastException.class,
              () -> {
                Boolean b = finalRow.getAs(0, Boolean.class);
              });
          assertThrows(
              ArrayIndexOutOfBoundsException.class, () -> finalRow.getAs(-1, Boolean.class));

          data =
              new Row[] {
                Row.create(
                    null, null, null, null, null, null, null, null, null, null, null, null, null,
                    null, null)
              };

          df = getSession().createDataFrame(data, schema);
          row = df.collect()[0];

          assert row.getAs(0, byte[].class) == null;
          assert row.getAs(1, Boolean.class) == null;
          assert row.getAs(2, Byte.class) == null;
          assert row.getAs(3, Date.class) == null;
          assert row.getAs(4, Double.class) == null;
          assert row.getAs(5, Float.class) == null;
          assert row.getAs(6, Geography.class) == null;
          assert row.getAs(7, Geometry.class) == null;
          assert row.getAs(8, Integer.class) == null;
          assert row.getAs(9, Long.class) == null;
          assert row.getAs(10, Short.class) == null;
          assert row.getAs(11, String.class) == null;
          assert row.getAs(12, Time.class) == null;
          assert row.getAs(13, Timestamp.class) == null;
          assert row.getAs(14, Variant.class) == null;
        },
        getSession());
  }

  @Test
  public void getAsWithStructuredMap() {
    structuredTypeTest(
        () -> {
          String query =
              "SELECT "
                  + "{'a':1,'b':2}::MAP(VARCHAR, NUMBER) as map1,"
                  + "{'1':'a','2':'b'}::MAP(NUMBER, VARCHAR) as map2,"
                  + "{'1':{'a':1,'b':2},'2':{'c':3}}::MAP(NUMBER, MAP(VARCHAR, NUMBER)) as map3";

          DataFrame df = getSession().sql(query);
          Row row = df.collect()[0];

          Map<?, ?> map1 = row.getAs(0, Map.class);
          assert (Long) map1.get("a") == 1L;
          assert (Long) map1.get("b") == 2L;

          Map<?, ?> map2 = row.getAs(1, Map.class);
          assert map2.get(1L).equals("a");
          assert map2.get(2L).equals("b");

          Map<?, ?> map3 = row.getAs(2, Map.class);
          Map<String, Long> map3ExpectedInnerMap = new HashMap<>();
          map3ExpectedInnerMap.put("a", 1L);
          map3ExpectedInnerMap.put("b", 2L);
          assert map3.get(1L).equals(map3ExpectedInnerMap);
          assert map3.get(2L).equals(Collections.singletonMap("c", 3L));
        },
        getSession());
  }

  @Test
  public void getAsWithStructuredArray() {
    structuredTypeTest(
        () -> {
          withTimeZoneTest(
              () -> {
                String query =
                    "SELECT "
                        + "[1,2,3]::ARRAY(NUMBER) AS arr1,"
                        + "['a','b']::ARRAY(VARCHAR) AS arr2,"
                        + "[parse_json(31000000)::timestamp_ntz]::ARRAY(TIMESTAMP_NTZ) AS arr3,"
                        + "[[1,2]]::ARRAY(ARRAY) AS arr4";

                DataFrame df = getSession().sql(query);
                Row row = df.collect()[0];

                ArrayList<?> array1 = row.getAs(0, ArrayList.class);
                assert array1.equals(Arrays.asList(1L, 2L, 3L));

                ArrayList<?> array2 = row.getAs(1, ArrayList.class);
                assert array2.equals(Arrays.asList("a", "b"));

                ArrayList<?> array3 = row.getAs(2, ArrayList.class);
                assert array3.equals(Collections.singletonList(new Timestamp(31000000000L)));

                ArrayList<?> array4 = row.getAs(3, ArrayList.class);
                assert array4.equals(Collections.singletonList("[\n  1,\n  2\n]"));
              },
              getSession());
        },
        getSession());
  }

  @Test
  public void getAsWithFieldName() {
    StructType schema =
        StructType.create(
            new StructField("EmpName", DataTypes.StringType),
            new StructField("NumVal", DataTypes.IntegerType));

    Row[] data = {Row.create("abcd", 10), Row.create("efgh", 20)};

    DataFrame df = getSession().createDataFrame(data, schema);
    Row row = df.collect()[0];

    assert (row.getAs("EmpName", String.class) == row.getAs(0, String.class));
    assert (row.getAs("EmpName", String.class).charAt(3) == 'd');
    assert (row.getAs("NumVal", Integer.class) == row.getAs(1, Integer.class));

    assert (row.getAs("EMPNAME", String.class) == row.getAs(0, String.class));

    assertThrows(
        IllegalArgumentException.class, () -> row.getAs("NonExistingColumn", Integer.class));

    Row rowWithoutSchema = Row.create(40, "Alice");
    assertThrows(
        UnsupportedOperationException.class,
        () -> rowWithoutSchema.getAs("NonExistingColumn", Integer.class));
  }

  @Test
  public void fieldIndex() {
    StructType schema =
        StructType.create(
            new StructField("EmpName", DataTypes.StringType),
            new StructField("NumVal", DataTypes.IntegerType));

    Row[] data = {Row.create("abcd", 10), Row.create("efgh", 20)};

    DataFrame df = getSession().createDataFrame(data, schema);
    Row row = df.collect()[0];

    assert (row.fieldIndex("EmpName") == 0);
    assert (row.fieldIndex("NumVal") == 1);
    assertThrows(IllegalArgumentException.class, () -> row.fieldIndex("NonExistingColumn"));
  }

  @Test
  public void mkString() {
    Row row1 = Row.create(1, "hello", 3.14, null, true, Collections.singletonMap("key", "value"), Arrays.asList("a", "b"));
    assert (row1.mkString().equals("1hello3.14nulltrueMap(key -> value)[a, b]"));
    assert (row1.mkString(",").equals("1,hello,3.14,null,true,Map(key -> value),[a, b]"));
    assert (row1.mkString("[", " | ", "]").equals("[1 | hello | 3.14 | null | true | Map(key -> value) | [a, b]]"));

    Row row2 = Row.create();
    assert (row2.mkString().isEmpty());
    assert (row2.mkString(",").isEmpty());
    assert (row2.mkString("[", " | ", "]").equals("[]"));

    Row row3 = Row.create("test");
    assert (row3.mkString().equals("test"));
    assert (row3.mkString(",").equals("test"));
    assert (row3.mkString("[", " | ", "]").equals("[test]"));

    Row row4 = Row.create("a", "b", "c");
    assert (row4.mkString("\n").equals("a\nb\nc"));
    assert (row4.mkString("\t").equals("a\tb\tc"));
    assert (row4.mkString("\\").equals("a\\b\\c"));
    assert (row4.mkString("\"").equals("a\"b\"c"));
    assert (row4.mkString("'").equals("a'b'c"));
  }
}
