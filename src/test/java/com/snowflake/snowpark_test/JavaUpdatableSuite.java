package com.snowflake.snowpark_test;

import com.snowflake.snowpark_java.*;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.types.StructType;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class JavaUpdatableSuite extends TestBase {

  @Test
  public void update1() {
    String tableName = randomName();
    try {
      createTestTable(tableName);
      Map<Column, Column> map = new HashMap<>();
      map.put(Functions.col("col1"), Functions.lit(3));
      UpdateResult result = getSession().table(tableName).update(map);
      assert result.getRowsUpdated() == 2;
      assert result.getMultiJoinedRowsUpdated() == 0;
      checkAnswer(
          getSession().table(tableName),
          new Row[] {Row.create(3, "a", true), Row.create(3, "b", false)});
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void update2() {
    String tableName = randomName();
    try {
      createTestTable(tableName);
      Map<String, Column> map = new HashMap<>();
      map.put("col1", Functions.lit(3));
      UpdateResult result = getSession().table(tableName).updateColumn(map);
      assert result.getRowsUpdated() == 2;
      assert result.getMultiJoinedRowsUpdated() == 0;
      checkAnswer(
          getSession().table(tableName),
          new Row[] {Row.create(3, "a", true), Row.create(3, "b", false)});
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void update3() {
    String tableName = randomName();
    try {
      createTestTable(tableName);
      Map<Column, Column> map = new HashMap<>();
      map.put(Functions.col("col1"), Functions.lit(3));
      UpdateResult result =
          getSession()
              .table(tableName)
              .update(map, Functions.col("col3").equal_to(Functions.lit(true)));
      assert result.getRowsUpdated() == 1;
      assert result.getMultiJoinedRowsUpdated() == 0;
      checkAnswer(
          getSession().table(tableName),
          new Row[] {Row.create(3, "a", true), Row.create(2, "b", false)},
          false);
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void update4() {
    String tableName = randomName();
    try {
      createTestTable(tableName);
      Map<String, Column> map = new HashMap<>();
      map.put("col1", Functions.lit(3));
      UpdateResult result =
          getSession()
              .table(tableName)
              .updateColumn(map, Functions.col("col3").equal_to(Functions.lit(true)));
      assert result.getRowsUpdated() == 1;
      assert result.getMultiJoinedRowsUpdated() == 0;
      checkAnswer(
          getSession().table(tableName),
          new Row[] {Row.create(3, "a", true), Row.create(2, "b", false)},
          false);
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void update5() {
    String tableName = randomName();
    DataFrame df = getSession().sql("select * from values(1, 2), (1, 4) as t(a, b)");
    try {
      createTestTable(tableName);
      Map<Column, Column> map = new HashMap<>();
      map.put(Functions.col("col1"), Functions.lit(3));
      UpdateResult result =
          getSession()
              .table(tableName)
              .update(map, Functions.col("col1").equal_to(df.col("a")), df);
      assert result.getRowsUpdated() == 1;
      assert result.getMultiJoinedRowsUpdated() == 1;
      checkAnswer(
          getSession().table(tableName),
          new Row[] {Row.create(3, "a", true), Row.create(2, "b", false)},
          false);
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void update6() {
    String tableName = randomName();
    DataFrame df = getSession().sql("select * from values(1, 2), (1, 4) as t(a, b)");
    try {
      createTestTable(tableName);
      Map<String, Column> map = new HashMap<>();
      map.put("col1", Functions.lit(3));
      UpdateResult result =
          getSession()
              .table(tableName)
              .updateColumn(map, Functions.col("col1").equal_to(df.col("a")), df);
      assert result.getRowsUpdated() == 1;
      assert result.getMultiJoinedRowsUpdated() == 1;
      checkAnswer(
          getSession().table(tableName),
          new Row[] {Row.create(3, "a", true), Row.create(2, "b", false)},
          false);
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void delete1() {
    String tableName = randomName();
    try {
      createTestTable(tableName);
      DeleteResult result = getSession().table(tableName).delete();
      assert result.getRowsDeleted() == 2;
      assert getSession().table(tableName).count() == 0;
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void delete2() {
    String tableName = randomName();
    try {
      createTestTable(tableName);
      DeleteResult result =
          getSession().table(tableName).delete(Functions.col("col1").equal_to(Functions.lit(1)));
      assert result.getRowsDeleted() == 1;
      checkAnswer(getSession().table(tableName), new Row[] {Row.create(2, "b", false)});
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void delete3() {
    String tableName = randomName();
    try {
      createTestTable(tableName);
      DataFrame df = getSession().sql("select * from values(1, 2), (3, 4) as t(a, b)");
      DeleteResult result =
          getSession().table(tableName).delete(Functions.col("col1").equal_to(df.col("a")), df);
      getSession().table(tableName).show();
      assert result.getRowsDeleted() == 1;
      checkAnswer(getSession().table(tableName), new Row[] {Row.create(2, "b", false)});
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void mergeMatchUpdate() {
    String tableName = randomName();
    DataFrame df = getSession().sql("select * from values(1, 2), (3, 4) as t(a, b)");
    try {
      createTestTable(tableName);
      Map<Column, Column> assignments = new HashMap<>();
      assignments.put(Functions.col("col1"), df.col("b"));
      MergeResult result =
          getSession()
              .table(tableName)
              .merge(df, Functions.col("col1").equal_to(df.col("a")))
              .whenMatched()
              .update(assignments)
              .collect();
      assert result.getRowsUpdated() == 1;
      assert result.getRowsDeleted() == 0;
      assert result.getRowsInserted() == 0;
      checkAnswer(
          getSession().table(tableName),
          new Row[] {Row.create(2, "a", true), Row.create(2, "b", false)},
          false);
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void mergeMatchUpdate2() {
    String tableName = randomName();
    DataFrame df = getSession().sql("select * from values(1, 2), (2, 4) as t(a, b)");
    try {
      createTestTable(tableName);
      Map<String, Column> assignments = new HashMap<>();
      assignments.put("col1", df.col("b"));
      MergeResult result =
          getSession()
              .table(tableName)
              .merge(df, Functions.col("col1").equal_to(df.col("a")))
              .whenMatched(Functions.col("col3").equal_to(Functions.lit(false)))
              .updateColumn(assignments)
              .collect();
      assert result.getRowsUpdated() == 1;
      assert result.getRowsDeleted() == 0;
      assert result.getRowsInserted() == 0;
      checkAnswer(
          getSession().table(tableName),
          new Row[] {Row.create(1, "a", true), Row.create(4, "b", false)},
          false);
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void mergeMatchDelete() {
    String tableName = randomName();
    DataFrame df = getSession().sql("select * from values(1, 2), (2, 4) as t(a, b)");
    try {
      createTestTable(tableName);
      MergeResult result =
          getSession()
              .table(tableName)
              .merge(df, Functions.col("col1").equal_to(df.col("a")))
              .whenMatched(Functions.col("col3").equal_to(Functions.lit(true)))
              .delete()
              .collect();
      assert result.getRowsUpdated() == 0;
      assert result.getRowsDeleted() == 1;
      assert result.getRowsInserted() == 0;
      checkAnswer(getSession().table(tableName), new Row[] {Row.create(2, "b", false)}, false);
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void mergeNotMatchInsert1() {
    String tableName = randomName();
    DataFrame df = getSession().sql("select * from values(1, 2), (3, 4) as t(a, b)");
    try {
      createTestTable(tableName);
      MergeResult result =
          getSession()
              .table(tableName)
              .merge(df, Functions.col("col1").equal_to(df.col("a")))
              .whenNotMatched()
              .insert(new Column[] {df.col("b"), Functions.lit("c"), Functions.lit(true)})
              .collect();
      assert result.getRowsUpdated() == 0;
      assert result.getRowsDeleted() == 0;
      assert result.getRowsInserted() == 1;
      checkAnswer(
          getSession().table(tableName),
          new Row[] {Row.create(1, "a", true), Row.create(2, "b", false), Row.create(4, "c", true)},
          true);
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void mergeNotMatchInsert2() {
    String tableName = randomName();
    DataFrame df = getSession().sql("select * from values(-1, 2), (3, 4) as t(a, b)");
    try {
      createTestTable(tableName);
      Map<Column, Column> assignments = new HashMap<>();
      assignments.put(Functions.col("col1"), df.col("b"));
      MergeResult result =
          getSession()
              .table(tableName)
              .merge(df, Functions.col("col1").equal_to(df.col("a")))
              .whenNotMatched(df.col("a").equal_to(Functions.lit(3)))
              .insert(assignments)
              .collect();
      assert result.getRowsUpdated() == 0;
      assert result.getRowsDeleted() == 0;
      assert result.getRowsInserted() == 1;
      checkAnswer(
          getSession().table(tableName),
          new Row[] {
            Row.create(1, "a", true), Row.create(2, "b", false), Row.create(4, null, null)
          },
          true);
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void mergeNotMatchInsert3() {
    String tableName = randomName();
    DataFrame df = getSession().sql("select * from values(-1, 2), (3, 4) as t(a, b)");
    try {
      createTestTable(tableName);
      Map<String, Column> assignments = new HashMap<>();
      assignments.put("col1", df.col("b"));
      MergeResult result =
          getSession()
              .table(tableName)
              .merge(df, Functions.col("col1").equal_to(df.col("a")))
              .whenNotMatched(df.col("a").equal_to(Functions.lit(3)))
              .insertRow(assignments)
              .collect();
      assert result.getRowsUpdated() == 0;
      assert result.getRowsDeleted() == 0;
      assert result.getRowsInserted() == 1;
      checkAnswer(
          getSession().table(tableName),
          new Row[] {
            Row.create(1, "a", true), Row.create(2, "b", false), Row.create(4, null, null)
          },
          true);
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void cloneTest() {
    String tableName = randomName();
    try {
      createTestTable(tableName);
      getSession().table(tableName).clone().delete();
      assert getSession().table(tableName).count() == 0;
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void singleColumnUpdateResult() {
    Map<String, String> params = new HashMap<>();
    params.put("ERROR_ON_NONDETERMINISTIC_UPDATE", "true");
    withSessionParameters(
        params,
        getSession(),
        false,
        () -> {
          String tableName = randomName();
          try {
            createTestTable(tableName);
            Map<Column, Column> map = new HashMap<>();
            map.put(Functions.col("col1"), Functions.lit(3));
            UpdateResult result = getSession().table(tableName).update(map);
            assert result.getRowsUpdated() == 2;
            assert result.getMultiJoinedRowsUpdated() == 0;
          } finally {
            dropTable(tableName);
          }
        });
  }

  private void createTestTable(String name) {
    Row[] data = {Row.create(1, "a", true), Row.create(2, "b", false)};
    StructType schema =
        StructType.create(
            new StructField("col1", DataTypes.IntegerType),
            new StructField("col2", DataTypes.StringType),
            new StructField("col3", DataTypes.BooleanType));

    getSession().createDataFrame(data, schema).write().saveAsTable(name);
  }
}
