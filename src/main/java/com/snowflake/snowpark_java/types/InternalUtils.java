package com.snowflake.snowpark_java.types;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * @hidden internal utils, excluded from doc
 */
public final class InternalUtils {

  // prevent to create instance of this Utils class
  private InternalUtils() {}

  public static Variant createVariant(com.snowflake.snowpark.types.Variant variant) {
    if (variant == null) {
      return null;
    } else {
      JsonNode value = com.snowflake.snowpark.internal.JavaUtils.getVariantValue(variant);
      String type = com.snowflake.snowpark.internal.JavaUtils.getVariantType(variant);
      return new Variant(value, type);
    }
  }

  public static com.snowflake.snowpark.types.Variant toScalaVariant(Variant variant) {
    if (variant == null) {
      return null;
    } else {
      JsonNode value = variant.getValue();
      VariantTypes type = variant.getType();
      return com.snowflake.snowpark.internal.JavaUtils.createScalaVariant(value, type.name());
    }
  }

  public static StructType createStructType(com.snowflake.snowpark.types.StructType structType) {
    return new StructType(structType);
  }

  public static com.snowflake.snowpark.types.StructType toScalaStructType(StructType structType) {
    return structType.getScalaStructType();
  }
}
