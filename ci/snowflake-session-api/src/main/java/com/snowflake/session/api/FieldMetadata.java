package com.snowflake.session.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A column, or a field nested in a structured column, as described by {@link
 * SnowflakeSession#getColumnFields}.
 */
public final class FieldMetadata {

  private final String name;
  private final String typeName;
  private final int sqlType;
  private final int precision;
  private final int scale;
  private final boolean nullable;
  private final List<FieldMetadata> fields;

  public FieldMetadata(
      String name,
      String typeName,
      int sqlType,
      int precision,
      int scale,
      boolean nullable,
      List<FieldMetadata> fields) {
    this.name = name;
    this.typeName = Objects.requireNonNull(typeName, "typeName");
    this.sqlType = sqlType;
    this.precision = precision;
    this.scale = scale;
    this.nullable = nullable;
    this.fields = Collections.unmodifiableList(new ArrayList<>(fields));
  }

  /** The field name; {@code null} for an ARRAY element or a MAP key or value. */
  public String getName() {
    return name;
  }

  /** The Snowflake type name, for example {@code NUMBER} or {@code OBJECT}. */
  public String getTypeName() {
    return typeName;
  }

  /** The {@link java.sql.Types} code. */
  public int getSqlType() {
    return sqlType;
  }

  public int getPrecision() {
    return precision;
  }

  public int getScale() {
    return scale;
  }

  public boolean isNullable() {
    return nullable;
  }

  /**
   * The nested fields: the element of an ARRAY, the key and value of a MAP, the fields of an OBJECT
   * in order. Empty for other types.
   */
  public List<FieldMetadata> getFields() {
    return fields;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof FieldMetadata)) {
      return false;
    }
    FieldMetadata that = (FieldMetadata) other;
    return Objects.equals(name, that.name)
        && typeName.equals(that.typeName)
        && sqlType == that.sqlType
        && precision == that.precision
        && scale == that.scale
        && nullable == that.nullable
        && fields.equals(that.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, typeName, sqlType, precision, scale, nullable, fields);
  }

  @Override
  public String toString() {
    return "FieldMetadata{"
        + name
        + " "
        + typeName
        + "("
        + precision
        + ","
        + scale
        + ")"
        + (nullable ? "" : " NOT NULL")
        + (fields.isEmpty() ? "" : " " + fields)
        + "}";
  }
}
