package com.snowflake.snowpark_java.types;

import java.util.Objects;

/**
 * Represents Column Identifier
 *
 * @since 0.9.0
 */
public class ColumnIdentifier implements Cloneable {
  private final com.snowflake.snowpark.types.ColumnIdentifier identifier;

  /**
   * Creates a ColumnIdentifier object for the giving column name. Identifier Requirement can be
   * found from https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html
   *
   * @since 0.9.0
   * @param name The column name
   */
  public ColumnIdentifier(String name) {
    this(com.snowflake.snowpark.types.ColumnIdentifier.apply(name));
  }

  ColumnIdentifier(com.snowflake.snowpark.types.ColumnIdentifier identifier) {
    this.identifier = identifier;
  }

  /**
   * Returns the name of column. Name format: 1. if the name quoted. a. starts with _A-Z and follows
   * by _A-Z0-9$: remove quotes b. starts with $ and follows by digits: remove quotes c. otherwise,
   * do nothing 2. if not quoted. a. starts with _a-zA-Z and follows by _a-zA-Z0-9$, upper case all
   * letters. b. starts with $ and follows by digits, do nothing c. otherwise, quote name
   *
   * <p>More details can be found from
   * https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html
   *
   * @return A String value representing the name
   * @since 0.9.0
   */
  public String name() {
    return this.identifier.name();
  }

  /**
   * Returns the quoted name of this column Name Format: 1. if quoted, do nothing 2. if not quoted.
   * a. starts with _a-zA-Z and follows by _a-zA-Z0-9$, upper case all letters and then quote b.
   * otherwise, quote name
   *
   * <p>It is same as [[name]], but quotes always added. It is always safe to do String comparisons
   * between quoted column names
   *
   * @return A String value representing the quoted name
   * @since 0.9.0
   */
  public String quotedName() {
    return this.identifier.quotedName();
  }

  /**
   * Compares this ColumnIdentifier with the giving one
   *
   * @return true if these two are equivalent, otherwise, returns false.
   * @since 0.9.0
   */
  @Override
  public boolean equals(Object other) {
    if (other instanceof ColumnIdentifier) {
      return this.identifier.equals(((ColumnIdentifier) other).identifier);
    }
    return false;
  }

  /**
   * Calculates the hash code of this ColumnIdentifier.
   *
   * @return An int number representing the hash code value
   * @since 0.9.0
   */
  @Override
  public int hashCode() {
    return Objects.hash(quotedName(), "ColumnIdentifier");
  }

  /**
   * Generates a String value to represent this Column Identifier.
   *
   * @return A String value
   * @since 0.9.0
   */
  @Override
  public String toString() {
    return identifier.toString();
  }

  /**
   * Creates a clone of this {@code ColumnIdentifier} object.
   *
   * @return A new {@code ColumnIdentifier} object
   * @since 1.2.0
   */
  @Override
  public ColumnIdentifier clone() {
    ColumnIdentifier cloned;
    try {
      cloned = (ColumnIdentifier) super.clone();
    } catch (CloneNotSupportedException e) {
      cloned =
          new ColumnIdentifier(
              (com.snowflake.snowpark.types.ColumnIdentifier) this.identifier.clone());
    }
    return cloned;
  }

  com.snowflake.snowpark.types.ColumnIdentifier toScalaColumnIdentifier() {
    return this.identifier;
  }
}
