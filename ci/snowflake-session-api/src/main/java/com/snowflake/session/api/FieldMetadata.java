package com.snowflake.session.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A column, or a field nested in a structured column, as described by {@link
 * SnowflakeSession#getColumnFields}.
 *
 * <p>Implementations create instances and Snowpark reads them, and the two are released
 * independently, so instances are only created through {@link #builder}. After the first release, a
 * new property is added as a new {@link Builder} method with a default, which keeps an
 * implementation built against an older version linking.
 */
public final class FieldMetadata {

  private final String name;
  private final String typeName;
  private final int sqlType;
  private final int precision;
  private final int scale;
  private final boolean nullable;
  private final List<FieldMetadata> fields;

  private FieldMetadata(Builder builder) {
    this.name = builder.name;
    this.typeName = builder.typeName;
    this.sqlType = builder.sqlType;
    this.precision = builder.precision;
    this.scale = builder.scale;
    this.nullable = builder.nullable;
    this.fields = Collections.unmodifiableList(new ArrayList<>(builder.fields));
  }

  /**
   * Starts a field of Snowflake type {@code typeName} and {@link java.sql.Types} code {@code
   * sqlType}. The other properties default to no name, precision and scale 0, nullable, and no
   * nested fields.
   */
  public static Builder builder(String typeName, int sqlType) {
    return new Builder(typeName, sqlType);
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

  /** Builds a {@link FieldMetadata}. */
  public static final class Builder {

    private final String typeName;
    private final int sqlType;

    @SuppressWarnings("NullAway.Init")
    private String name;

    private int precision;
    private int scale;
    private boolean nullable = true;
    private List<FieldMetadata> fields = Collections.emptyList();

    private Builder(String typeName, int sqlType) {
      this.typeName = Objects.requireNonNull(typeName, "typeName");
      this.sqlType = sqlType;
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder precision(int precision) {
      this.precision = precision;
      return this;
    }

    public Builder scale(int scale) {
      this.scale = scale;
      return this;
    }

    public Builder nullable(boolean nullable) {
      this.nullable = nullable;
      return this;
    }

    /** The nested fields; the list is copied when the field is built. */
    public Builder fields(List<FieldMetadata> fields) {
      this.fields = Objects.requireNonNull(fields, "fields");
      return this;
    }

    public FieldMetadata build() {
      return new FieldMetadata(this);
    }
  }
}
