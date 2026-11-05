package com.snowflake.session.api;

import java.util.Objects;

/** The status of a query as reported by {@link SnowflakeSession#getQueryStatus(String)}. */
public final class QueryStatus {

  private final String name;
  private final boolean stillRunning;

  public QueryStatus(String name, boolean stillRunning) {
    this.name = Objects.requireNonNull(name, "name");
    this.stillRunning = stillRunning;
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
}
