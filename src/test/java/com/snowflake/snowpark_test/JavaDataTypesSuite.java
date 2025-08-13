package com.snowflake.snowpark_test;

import com.snowflake.snowpark.internal.JavaDataTypeUtils;
import com.snowflake.snowpark_java.types.*;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.junit.Test;

public class JavaDataTypesSuite {

  @Test
  public void javaTypeToScalaTypeConversion() {
    assert JavaDataTypeUtils.javaTypeToScalaType(DataTypes.ByteType)
        .equals(com.snowflake.snowpark.types.ByteType$.MODULE$);
    assert JavaDataTypeUtils.javaTypeToScalaType(DataTypes.ShortType)
        .equals(com.snowflake.snowpark.types.ShortType$.MODULE$);
    assert JavaDataTypeUtils.javaTypeToScalaType(DataTypes.IntegerType)
        .equals(com.snowflake.snowpark.types.IntegerType$.MODULE$);
    assert JavaDataTypeUtils.javaTypeToScalaType(DataTypes.LongType)
        .equals(com.snowflake.snowpark.types.LongType$.MODULE$);
    assert JavaDataTypeUtils.javaTypeToScalaType(DataTypes.FloatType)
        .equals(com.snowflake.snowpark.types.FloatType$.MODULE$);
    assert JavaDataTypeUtils.javaTypeToScalaType(DataTypes.DoubleType)
        .equals(com.snowflake.snowpark.types.DoubleType$.MODULE$);
    assert JavaDataTypeUtils.javaTypeToScalaType(DataTypes.BinaryType)
        .equals(com.snowflake.snowpark.types.BinaryType$.MODULE$);
    assert JavaDataTypeUtils.javaTypeToScalaType(DataTypes.BooleanType)
        .equals(com.snowflake.snowpark.types.BooleanType$.MODULE$);
    assert JavaDataTypeUtils.javaTypeToScalaType(DataTypes.DateType)
        .equals(com.snowflake.snowpark.types.DateType$.MODULE$);
    assert JavaDataTypeUtils.javaTypeToScalaType(DataTypes.GeographyType)
        .equals(com.snowflake.snowpark.types.GeographyType$.MODULE$);
    assert JavaDataTypeUtils.javaTypeToScalaType(DataTypes.GeometryType)
        .equals(com.snowflake.snowpark.types.GeometryType$.MODULE$);
    assert JavaDataTypeUtils.javaTypeToScalaType(DataTypes.StringType)
        .equals(com.snowflake.snowpark.types.StringType$.MODULE$);
    assert JavaDataTypeUtils.javaTypeToScalaType(DataTypes.TimestampType)
        .equals(com.snowflake.snowpark.types.TimestampType$.MODULE$);
    assert JavaDataTypeUtils.javaTypeToScalaType(DataTypes.TimeType)
        .equals(com.snowflake.snowpark.types.TimeType$.MODULE$);
    assert JavaDataTypeUtils.javaTypeToScalaType(DataTypes.VariantType)
        .equals(com.snowflake.snowpark.types.VariantType$.MODULE$);

    assert JavaDataTypeUtils.javaTypeToScalaType(
            DataTypes.createDecimalType(DecimalType.MAX_PRECISION, DecimalType.MAX_SCALE))
        .equals(new com.snowflake.snowpark.types.DecimalType(38, 38));

    assert JavaDataTypeUtils.javaTypeToScalaType(DataTypes.createArrayType(DataTypes.IntegerType))
        .equals(
            new com.snowflake.snowpark.types.ArrayType(
                com.snowflake.snowpark.types.IntegerType$.MODULE$));

    assert JavaDataTypeUtils.javaTypeToScalaType(
            DataTypes.createMapType(DataTypes.StringType, DataTypes.DoubleType))
        .equals(
            new com.snowflake.snowpark.types.MapType(
                com.snowflake.snowpark.types.StringType$.MODULE$,
                com.snowflake.snowpark.types.DoubleType$.MODULE$));

    // negative tests
    assert !JavaDataTypeUtils.javaTypeToScalaType(DataTypes.ByteType)
        .equals(com.snowflake.snowpark.types.BooleanType$.MODULE$);

    assert !JavaDataTypeUtils.javaTypeToScalaType(
            DataTypes.createDecimalType(DecimalType.MAX_PRECISION, DecimalType.MAX_SCALE))
        .equals(new com.snowflake.snowpark.types.DecimalType(18, 10));

    assert !JavaDataTypeUtils.javaTypeToScalaType(DataTypes.createArrayType(DataTypes.DateType))
        .equals(
            new com.snowflake.snowpark.types.ArrayType(
                com.snowflake.snowpark.types.IntegerType$.MODULE$));

    assert !JavaDataTypeUtils.javaTypeToScalaType(
            DataTypes.createMapType(DataTypes.StringType, DataTypes.TimestampType))
        .equals(
            new com.snowflake.snowpark.types.MapType(
                com.snowflake.snowpark.types.StringType$.MODULE$,
                com.snowflake.snowpark.types.DoubleType$.MODULE$));
  }

  @Test
  public void scalaTypeToJavaTypeConversion() {
    assert JavaDataTypeUtils.scalaTypeToJavaType(com.snowflake.snowpark.types.ByteType$.MODULE$)
        .equals(DataTypes.ByteType);
    assert JavaDataTypeUtils.scalaTypeToJavaType(com.snowflake.snowpark.types.ShortType$.MODULE$)
        .equals(DataTypes.ShortType);
    assert JavaDataTypeUtils.scalaTypeToJavaType(com.snowflake.snowpark.types.IntegerType$.MODULE$)
        .equals(DataTypes.IntegerType);
    assert JavaDataTypeUtils.scalaTypeToJavaType(com.snowflake.snowpark.types.LongType$.MODULE$)
        .equals(DataTypes.LongType);
    assert JavaDataTypeUtils.scalaTypeToJavaType(com.snowflake.snowpark.types.FloatType$.MODULE$)
        .equals(DataTypes.FloatType);
    assert JavaDataTypeUtils.scalaTypeToJavaType(com.snowflake.snowpark.types.DoubleType$.MODULE$)
        .equals(DataTypes.DoubleType);
    assert JavaDataTypeUtils.scalaTypeToJavaType(com.snowflake.snowpark.types.BinaryType$.MODULE$)
        .equals(DataTypes.BinaryType);
    assert JavaDataTypeUtils.scalaTypeToJavaType(com.snowflake.snowpark.types.BooleanType$.MODULE$)
        .equals(DataTypes.BooleanType);
    assert JavaDataTypeUtils.scalaTypeToJavaType(com.snowflake.snowpark.types.DateType$.MODULE$)
        .equals(DataTypes.DateType);
    assert JavaDataTypeUtils.scalaTypeToJavaType(
            com.snowflake.snowpark.types.GeographyType$.MODULE$)
        .equals(DataTypes.GeographyType);
    assert JavaDataTypeUtils.scalaTypeToJavaType(com.snowflake.snowpark.types.GeometryType$.MODULE$)
        .equals(DataTypes.GeometryType);
    assert JavaDataTypeUtils.scalaTypeToJavaType(com.snowflake.snowpark.types.StringType$.MODULE$)
        .equals(DataTypes.StringType);
    assert JavaDataTypeUtils.scalaTypeToJavaType(
            com.snowflake.snowpark.types.TimestampType$.MODULE$)
        .equals(DataTypes.TimestampType);
    assert JavaDataTypeUtils.scalaTypeToJavaType(com.snowflake.snowpark.types.TimeType$.MODULE$)
        .equals(DataTypes.TimeType);
    assert JavaDataTypeUtils.scalaTypeToJavaType(com.snowflake.snowpark.types.VariantType$.MODULE$)
        .equals(DataTypes.VariantType);

    assert JavaDataTypeUtils.scalaTypeToJavaType(
            new com.snowflake.snowpark.types.DecimalType(38, 38))
        .equals(DataTypes.createDecimalType(DecimalType.MAX_PRECISION, DecimalType.MAX_SCALE));

    assert JavaDataTypeUtils.scalaTypeToJavaType(
            new com.snowflake.snowpark.types.ArrayType(
                com.snowflake.snowpark.types.TimeType$.MODULE$))
        .equals(DataTypes.createArrayType(DataTypes.TimeType));

    assert JavaDataTypeUtils.scalaTypeToJavaType(
            new com.snowflake.snowpark.types.MapType(
                com.snowflake.snowpark.types.StringType$.MODULE$,
                com.snowflake.snowpark.types.FloatType$.MODULE$))
        .equals(DataTypes.createMapType(DataTypes.StringType, DataTypes.FloatType));

    // negative tests
    assert !JavaDataTypeUtils.scalaTypeToJavaType(com.snowflake.snowpark.types.BooleanType$.MODULE$)
        .equals(DataTypes.TimeType);

    assert !JavaDataTypeUtils.scalaTypeToJavaType(
            new com.snowflake.snowpark.types.DecimalType(20, 18))
        .equals(DataTypes.createDecimalType(DecimalType.MAX_PRECISION, DecimalType.MAX_SCALE));

    assert !JavaDataTypeUtils.scalaTypeToJavaType(
            new com.snowflake.snowpark.types.ArrayType(
                com.snowflake.snowpark.types.TimeType$.MODULE$))
        .equals(DataTypes.createArrayType(DataTypes.ByteType));

    assert !JavaDataTypeUtils.scalaTypeToJavaType(
            new com.snowflake.snowpark.types.MapType(
                com.snowflake.snowpark.types.StringType$.MODULE$,
                com.snowflake.snowpark.types.FloatType$.MODULE$))
        .equals(DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType));
  }

  @Test
  public void typeName() {
    assert DataTypes.ByteType.typeName().equals("ByteType");
    assert DataTypes.ShortType.typeName().equals("ShortType");
    assert DataTypes.IntegerType.typeName().equals("IntegerType");
    assert DataTypes.LongType.typeName().equals("LongType");
    assert DataTypes.FloatType.typeName().equals("FloatType");
    assert DataTypes.DoubleType.typeName().equals("DoubleType");
    assert DataTypes.BinaryType.typeName().equals("BinaryType");
    assert DataTypes.BooleanType.typeName().equals("BooleanType");
    assert DataTypes.DateType.typeName().equals("DateType");
    assert DataTypes.GeographyType.typeName().equals("GeographyType");
    assert DataTypes.StringType.typeName().equals("StringType");
    assert DataTypes.TimestampType.typeName().equals("TimestampType");
    assert DataTypes.TimeType.typeName().equals("TimeType");
    assert DataTypes.VariantType.typeName().equals("VariantType");
    assert DataTypes.createArrayType(DataTypes.IntegerType).typeName().equals("ArrayType");
    assert DataTypes.createMapType(DataTypes.StringType, DataTypes.DoubleType)
        .typeName()
        .equals("MapType");
    assert DataTypes.createDecimalType(18, 10).typeName().equals("DecimalType");
  }

  @Test
  public void toStringTest() {
    assert DataTypes.ByteType.typeName().equals(DataTypes.ByteType.toString());
    assert DataTypes.ShortType.typeName().equals(DataTypes.ShortType.toString());
    assert DataTypes.IntegerType.typeName().equals(DataTypes.IntegerType.toString());
    assert DataTypes.LongType.typeName().equals(DataTypes.LongType.toString());
    assert DataTypes.FloatType.typeName().equals(DataTypes.FloatType.toString());
    assert DataTypes.DoubleType.typeName().equals(DataTypes.DoubleType.toString());
    assert DataTypes.BinaryType.typeName().equals(DataTypes.BinaryType.toString());
    assert DataTypes.BooleanType.typeName().equals(DataTypes.BooleanType.toString());
    assert DataTypes.DateType.typeName().equals(DataTypes.DateType.toString());
    assert DataTypes.GeographyType.typeName().equals(DataTypes.GeographyType.toString());
    assert DataTypes.StringType.typeName().equals(DataTypes.StringType.toString());
    assert DataTypes.TimestampType.typeName().equals(DataTypes.TimestampType.toString());
    assert DataTypes.TimeType.typeName().equals(DataTypes.TimeType.toString());
    assert DataTypes.VariantType.typeName().equals(DataTypes.VariantType.toString());
    assert DataTypes.createDecimalType(20, 10).toString().equals("Decimal(20, 10)");
    assert DataTypes.createArrayType(DataTypes.IntegerType)
        .toString()
        .equals("ArrayType[IntegerType]");
    assert DataTypes.createMapType(DataTypes.StringType, DataTypes.DoubleType)
        .toString()
        .equals("MapType[StringType, DoubleType]");
  }

  @Test
  public void mapType() {
    MapType map = DataTypes.createMapType(DataTypes.StringType, DataTypes.ByteType);
    assert map.getKeyType().equals(DataTypes.StringType);
    assert map.getValueType().equals(DataTypes.ByteType);

    assert !DataTypes.createMapType(DataTypes.StringType, DataTypes.DoubleType)
        .equals(DataTypes.StringType);
    assert DataTypes.createMapType(DataTypes.StringType, DataTypes.DoubleType).hashCode()
        != DataTypes.createMapType(DataTypes.StringType, DataTypes.BinaryType).hashCode();
    assert DataTypes.createMapType(DataTypes.StringType, DataTypes.DoubleType).hashCode()
        == DataTypes.createMapType(DataTypes.StringType, DataTypes.DoubleType).hashCode();
  }

  @Test
  public void arrayType() {
    ArrayType arr = DataTypes.createArrayType(DataTypes.BinaryType);
    assert arr.getElementType().equals(DataTypes.BinaryType);

    assert !DataTypes.createArrayType(DataTypes.BinaryType).equals(DataTypes.DoubleType);
    assert DataTypes.createArrayType(DataTypes.DoubleType).hashCode()
        != DataTypes.createArrayType(DataTypes.IntegerType).hashCode();
    assert DataTypes.createArrayType(DataTypes.DoubleType).hashCode()
        == DataTypes.createArrayType(DataTypes.DoubleType).hashCode();
  }

  @Test
  public void decimalType() {
    DecimalType decimal = DataTypes.createDecimalType(22, 11);
    assert decimal.getPrecision() == 22;
    assert decimal.getScale() == 11;

    assert !DataTypes.createDecimalType(12, 11).equals("abc");
    assert !DataTypes.createDecimalType(10, 5).equals(DataTypes.createDecimalType(10, 15));
    assert !DataTypes.createDecimalType(10, 5).equals(DataTypes.createDecimalType(20, 5));

    assert DataTypes.createDecimalType(10, 5).hashCode()
        == DataTypes.createDecimalType(10, 5).hashCode();
    assert DataTypes.createDecimalType(10, 5).hashCode()
        != DataTypes.createDecimalType(11, 5).hashCode();
  }

  @Test
  public void columnIdentifierTest() {
    ColumnIdentifier ci = new ColumnIdentifier("aBcdEf");
    assert ci.name().equals("ABCDEF");
    assert ci.quotedName().equals("\"ABCDEF\"");
    assert ci.toString().equals(ci.name());
    assert ci.equals(new ColumnIdentifier("ABCDEF"));
    assert !ci.equals(new ColumnIdentifier("\"abcdef\""));

    // compare with other object
    assert !ci.equals("abc");
    ColumnIdentifier ci2 = new ColumnIdentifier("ABCDEF");
    ColumnIdentifier ci3 = new ColumnIdentifier("aBcdEf12");
    assert ci.equals(ci2);
    assert ci.hashCode() == ci2.hashCode();
    assert ci.hashCode() != ci3.hashCode();
    assert !ci.equals(ci3);

    // clone
    ColumnIdentifier ciCloned = ci.clone();
    assert ciCloned.equals(ci);
    assert ciCloned != ci;
    assert ciCloned.quotedName().equals("\"ABCDEF\"");
    assert ciCloned.name().equals("ABCDEF");
  }

  @Test
  public void structField() {
    StructField field1 = new StructField(new ColumnIdentifier("abc"), DataTypes.BinaryType, true);
    StructField field2 = new StructField(new ColumnIdentifier("abc"), DataTypes.BinaryType);
    assert field1.equals(field2);

    assert field1.dataType().equals(DataTypes.BinaryType);
    assert field1.columnIdentifier().equals(new ColumnIdentifier("abc"));
    assert field1.name().equals("ABC");
    assert field1.nullable();

    StructField field3 = new StructField("abc", DataTypes.BinaryType, false);
    StructField field4 = new StructField("abc", DataTypes.BinaryType);
    assert !field1.equals(field3);
    assert field1.equals(field4);
    assert !field3.nullable();
    assert field3.columnIdentifier().equals(new ColumnIdentifier("abc"));
    assert field3.name().equals("ABC");

    assert field3.toString().equals("StructField(ABC, Binary, Nullable = false)");
    assert field4.toString().equals("StructField(ABC, Binary, Nullable = true)");

    assert !field1.equals("123");
    assert field1.hashCode() == field2.hashCode();
    assert field1.hashCode() != field3.hashCode();
  }

  @Test
  public void structType() {
    StructField[] fields = {
      new StructField("c1", DataTypes.ByteType), new StructField("c2", DataTypes.FloatType)
    };
    StructType type1 = new StructType(fields);
    StructType type2 = new StructType(type1);
    StructType type3 =
        StructType.create(
            new StructField("c1", DataTypes.ByteType), new StructField("c2", DataTypes.FloatType));
    assert type2.equals(type1);
    assert type1.equals(type3);

    String[] names = type2.names();
    assert names.length == 2;
    assert names[0].equals("C1");
    assert names[1].equals("C2");
    assert type1.size() == 2;
    assert StructType.create(new StructField("c3", DataTypes.FloatType)).size() == 1;

    Iterator<StructField> it = type1.iterator();
    assert it.hasNext();
    assert it.next().equals(new StructField("c1", DataTypes.ByteType));
    assert it.hasNext();
    assert it.next().equals(new StructField("c2", DataTypes.FloatType));
    assert !it.hasNext();

    boolean hasException = false;
    try {
      it.next();
    } catch (NoSuchElementException e) {
      hasException = true;
    }
    assert hasException;

    assert type1.get(0).equals(new StructField("c1", DataTypes.ByteType));
    assert type1.get(1).equals(new StructField("c2", DataTypes.FloatType));

    assert type1
        .toString()
        .equals(
            "StructType[StructField(C1, Byte, Nullable = true), StructField(C2, Float, Nullable ="
                + " true)]");

    StructType type4 = type1.add(new StructField("c3", DataTypes.ShortType));
    assert !type1.equals(type4);
    assert type4.size() == 3;
    assert type4.get(2).equals(new StructField("c3", DataTypes.ShortType));
    assert type1.size() == 2;
    assert type3.equals(type1);

    StructType type5 = type1.add("c3", DataTypes.ShortType);
    assert type4.equals(type5);

    StructType type6 = type2.add("c3", DataTypes.ShortType, false);
    assert !type6.equals(type5);
    assert type6.get(2).equals(new StructField("c3", DataTypes.ShortType, false));

    Optional<StructField> field = type6.nameToField("c3");
    assert field.isPresent();
    assert field.get().equals(new StructField("c3", DataTypes.ShortType, false));
    field = type1.nameToField("c3");
    assert !field.isPresent();

    assert type1.hashCode() == type3.hashCode();
    assert type1.hashCode() != type4.hashCode();
    assert !type1.equals("11");
  }

  @Test
  public void moreEqualsTest() {
    // compare to other object
    assert !DataTypes.BooleanType.equals("123");

    assert DataTypes.BooleanType.hashCode() != DataTypes.ByteType.hashCode();
    assert DataTypes.BooleanType.hashCode() == DataTypes.BooleanType.hashCode();
  }
}
