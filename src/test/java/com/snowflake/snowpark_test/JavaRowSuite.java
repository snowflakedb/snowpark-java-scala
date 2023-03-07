package com.snowflake.snowpark_test;

import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.types.Geography;
import com.snowflake.snowpark_java.types.Variant;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class JavaRowSuite {

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

    assert row.toString().equals("Row[null,true,1,2,3,4,5.5,6.6,a]");
  }

  @Test
  public void getters2() {
    byte[] binary = {(byte) 1, (byte) 2};
    Row row =
        Row.create(
            binary,
            new Variant(3),
            Geography.fromGeoJSON("{\"type\":\"Point\",\"coordinates\":[30,10]}"),
            new BigDecimal(12345));

    assert row.size() == 4;
    assert Arrays.equals(row.getBinary(0), binary);
    assert row.getVariant(1).equals(new Variant(3));
    assert row.getGeography(2)
        .equals(Geography.fromGeoJSON("{\"type\":\"Point\",\"coordinates\":[30,10]}"));
    assert row.getDecimal(3).equals(new BigDecimal(12345));

    assert row.toString()
        .equals("Row[Binary(1,2),3,{\"type\":\"Point\",\"coordinates\":[30,10]},12345]");
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

    assert row.toString().equals("Row[[1,2,3],{\"a\":1,\"b\":2}]");
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
                "{\"type\":\"Point\",\"coordinates\":[30,10]}"));

    assert row.get(0) instanceof Variant;
    assert row.get(1) instanceof Geography;
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
    // get()
    String[] getValues = (String[]) row.get(0);
    assert getValues.length == 3;
    assert getValues[0].equals("a") && getValues[1].equals("b") && getValues[2] == null;

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
    assert ((String[]) row.get(0)).length == 0;
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
    // get()
    String[] getValues = (String[]) row.get(0);
    assert getValues.length == 3;
    assert getValues[0] == null && getValues[1] == null && getValues[2] == null;

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
}
