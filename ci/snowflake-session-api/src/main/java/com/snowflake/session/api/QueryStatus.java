package com.snowflake.session.api;

import java.util.Objects;

/**
 * The status of a query as reported by {@link SnowflakeSession#getQueryStatus(String)}.
 *
 * <p>As with {@link FieldMetadata}, instances are only created through {@link #builder}, and a new
 * property is added as a new {@link Builder} method with a default.
 */
public final class QueryStatus {

  private final String name;
  private final boolean stillRunning;

  private QueryStatus(Builder builder) {
    this.name = builder.name;
    this.stillRunning = builder.stillRunning;
  }

  /** Starts a status named {@code name}. It defaults to not running. */
  public static Builder builder(String name) {
    return new Builder(name);
  }

  /** The Snowflake status name, for example {@code RUNNING} or {@code FAILED_WITH_ERROR}. */
  public String getName() {
    return name;
  }

  /** Whether the query has not reached a terminal status yet. */
  public boolean isStillRunning() {
    return stillRunning;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof QueryStatus)) {
      return false;
    }
    QueryStatus that = (QueryStatus) other;
    return name.equals(that.name) && stillRunning == that.stillRunning;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, stillRunning);
  }

  @Override
  public String toString() {
    return "QueryStatus{" + name + (stillRunning ? ", running" : "") + "}";
  }

  /** Builds a {@link QueryStatus}. */
  public static final class Builder {

    private final String name;
    private boolean stillRunning;

    private Builder(String name) {
      this.name = Objects.requireNonNull(name, "name");
    }

    public Builder stillRunning(boolean stillRunning) {
      this.stillRunning = stillRunning;
      return this;
    }

    public QueryStatus build() {
      return new QueryStatus(this);
    }
  }
}
