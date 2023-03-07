package com.snowflake.snowpark_java.types;

/**
 * This class contains all singleton DataTypes and factory methods.
 *
 * @since 0.9.0
 */
public final class DataTypes {

  // prevent to create instance of this Utils class.
  private DataTypes() {}

  /**
   * Retrieves the ByteType object.
   *
   * @since 0.9.0
   */
  public static final ByteType ByteType = new ByteType();

  /**
   * Retrieves the ShortType object.
   *
   * @since 0.9.0
   */
  public static final ShortType ShortType = new ShortType();

  /**
   * Retrieves the IntegerType object.
   *
   * @since 0.9.0
   */
  public static final IntegerType IntegerType = new IntegerType();

  /**
   * Retrieves the LongType object.
   *
   * @since 0.9.0
   */
  public static final LongType LongType = new LongType();

  /**
   * Retrieves the FloatType object.
   *
   * @since 0.9.0
   */
  public static final FloatType FloatType = new FloatType();

  /**
   * Retrieves the DoubleType object.
   *
   * @since 0.9.0
   */
  public static final DoubleType DoubleType = new DoubleType();

  /**
   * Retrieves the BinaryType object.
   *
   * @since 0.9.0
   */
  public static final BinaryType BinaryType = new BinaryType();

  /**
   * Retrieves the BooleanType object.
   *
   * @since 0.9.0
   */
  public static final BooleanType BooleanType = new BooleanType();

  /**
   * Retrieves the DateType object.
   *
   * @since 0.9.0
   */
  public static final DateType DateType = new DateType();

  /**
   * Retrieves the GeographyType object.
   *
   * @since 0.9.0
   */
  public static final GeographyType GeographyType = new GeographyType();

  /**
   * Retrieves the StringType object.
   *
   * @since 0.9.0
   */
  public static final StringType StringType = new StringType();

  /**
   * Retrieves the TimestampType object.
   *
   * @since 0.9.0
   */
  public static final TimestampType TimestampType = new TimestampType();

  /**
   * Retrieves the TimeType object.
   *
   * @since 0.9.0
   */
  public static final TimeType TimeType = new TimeType();

  /**
   * Retrieves the VariantType object.
   *
   * @since 0.9.0
   */
  public static final VariantType VariantType = new VariantType();

  /**
   * Creates a new DecimalType object.
   *
   * @param precision An int number representing the precision
   * @param scale An int number representing the scale
   * @return A new DecimalType object
   * @since 0.9.0
   */
  public static DecimalType createDecimalType(int precision, int scale) {
    return new DecimalType(precision, scale);
  }

  /**
   * Creates a new ArrayType object.
   *
   * @param elementType The data type of array's element
   * @return A new ArrayType object
   */
  public static ArrayType createArrayType(DataType elementType) {
    return new ArrayType(elementType);
  }

  /**
   * Create a new MapType object.
   *
   * @param keyType The data type of Map's key
   * @param valueType The data type of Map's value
   * @return A new MapType object
   */
  public static MapType createMapType(DataType keyType, DataType valueType) {
    return new MapType(keyType, valueType);
  }
}
