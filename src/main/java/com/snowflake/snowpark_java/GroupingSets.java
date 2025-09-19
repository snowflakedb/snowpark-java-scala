package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A Container of grouping sets that you pass to {@code DataFrame.groupByGroupingSets}
 *
 * @since 1.1.0
 */
public class GroupingSets {

  private final com.snowflake.snowpark.GroupingSets groupingSets;

  GroupingSets(com.snowflake.snowpark.GroupingSets groupingSets) {
    this.groupingSets = groupingSets;
  }

  com.snowflake.snowpark.GroupingSets getScalaGroupingSets() {
    return this.groupingSets;
  }

  /**
   * Creates a GroupingSets object from a list of column/expression sets.
   *
   * @param sets a list of arguments
   * @since 1.1.0
   * @return A new GroupingSets Object
   */
  @SafeVarargs
  public static GroupingSets create(Set<Column>... sets) {
    Set<com.snowflake.snowpark.Column>[] arr = new Set[sets.length];
    for (int i = 0; i < sets.length; i++) {
      arr[i] = sets[i].stream().map(column -> column.toScalaColumn()).collect(Collectors.toSet());
    }

    return new GroupingSets(JavaUtils.createGroupingSets(arr));
  }
}
