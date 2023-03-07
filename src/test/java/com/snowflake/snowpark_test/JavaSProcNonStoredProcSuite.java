package com.snowflake.snowpark_test;

import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.StoredProcedure;
import com.snowflake.snowpark_java.sproc.*;
import com.snowflake.snowpark_java.types.DataType;
import com.snowflake.snowpark_java.types.DataTypes;
import org.junit.Test;

public class JavaSProcNonStoredProcSuite extends UDFTestBase {
  public JavaSProcNonStoredProcSuite() {}

  private boolean dependencyAdded = false;

  @Override
  public Session getSession() {
    Session session = super.getSession();
    if (!dependencyAdded) {
      dependencyAdded = true;
      addDepsToClassPath(session);
    }
    return session;
  }

  @Test
  public void permanent0() {
    String spName = randomName();
    String stageName = randomStageName();
    try {
      createStage(stageName, false);
      StoredProcedure sp =
          getSession()
              .sproc()
              .registerPermanent(
                  spName, session -> "SUCCESS", DataTypes.StringType, stageName, true);
      checkAnswer(getSession().storedProcedure(sp), new Row[] {Row.create("SUCCESS")});
      checkAnswer(getSession().storedProcedure(spName), new Row[] {Row.create("SUCCESS")});
    } finally {
      dropStage(stageName);
      getSession().sql("drop procedure " + spName + "()").show();
    }
  }
  //  This script generates the tests below
  //  (1 to 21).foreach { x =>
  //    val data = (1 to x).map(i => s"$i").mkString(",")
  //    val args = (1 to x).map(i => s"Integer col$i").mkString(", ")
  //    val func = (1 to x).map(i => s"col$i").mkString(""," + ", "+ 100")
  //    val input = (1 to x).map(_ => "DataTypes.IntegerType").mkString("{", ", ", "}")
  //    val types = x match {
  //      case 1 => "DataTypes.IntegerType"
  //      case _ => "input"
  //    }
  //    val argTypes = (1 to x).map(_ => "INT").mkString(",")
  //    val result = 100 + (1 to x).reduce(_ + _)
  //    println(s"""
  //    |@Test
  //    |public void permanent$x() {
  //    | String spName = randomName();
  //    | String stageName = randomStageName();
  //    | DataType[] input = $input;
  //    | try {
  //    |   createStage(stageName, false);
  //    |   StoredProcedure sp =
  //    |     getSession().sproc().registerPermanent(
  //    |       spName,
  //    |       (Session session, $args) -> $func,
  //    |       $types,
  //    |       DataTypes.IntegerType,
  //    |       stageName,
  //    |       true
  //    |     );
  //    |   checkAnswer(getSession().storedProcedure(sp, $data),
  //    |     new Row[] {Row.create($result)});
  //    |   checkAnswer(getSession().storedProcedure(spName, $data),
  //    |     new Row[] {Row.create($result)});
  //    | } finally {
  //    |   dropStage(stageName);
  //    |   getSession().sql("drop procedure " + spName + "($argTypes)").show();
  //    | }
  //    |}""".stripMargin)
  //  }

  @Test
  public void permanent1() {
    String spName = randomName();
    String stageName = randomStageName();
    DataType[] input = {DataTypes.IntegerType};
    try {
      createStage(stageName, false);
      StoredProcedure sp =
          getSession()
              .sproc()
              .registerPermanent(
                  spName,
                  (Session session, Integer col1) -> col1 + 100,
                  DataTypes.IntegerType,
                  DataTypes.IntegerType,
                  stageName,
                  true);
      checkAnswer(getSession().storedProcedure(sp, 1), new Row[] {Row.create(101)});
      checkAnswer(getSession().storedProcedure(spName, 1), new Row[] {Row.create(101)});
    } finally {
      dropStage(stageName);
      getSession().sql("drop procedure " + spName + "(INT)").show();
    }
  }

  @Test
  public void permanent2() {
    String spName = randomName();
    String stageName = randomStageName();
    DataType[] input = {DataTypes.IntegerType, DataTypes.IntegerType};
    try {
      createStage(stageName, false);
      StoredProcedure sp =
          getSession()
              .sproc()
              .registerPermanent(
                  spName,
                  (Session session, Integer col1, Integer col2) -> col1 + col2 + 100,
                  input,
                  DataTypes.IntegerType,
                  stageName,
                  true);
      checkAnswer(getSession().storedProcedure(sp, 1, 2), new Row[] {Row.create(103)});
      checkAnswer(getSession().storedProcedure(spName, 1, 2), new Row[] {Row.create(103)});
    } finally {
      dropStage(stageName);
      getSession().sql("drop procedure " + spName + "(INT,INT)").show();
    }
  }

  @Test
  public void permanent3() {
    String spName = randomName();
    String stageName = randomStageName();
    DataType[] input = {DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType};
    try {
      createStage(stageName, false);
      StoredProcedure sp =
          getSession()
              .sproc()
              .registerPermanent(
                  spName,
                  (Session session, Integer col1, Integer col2, Integer col3) ->
                      col1 + col2 + col3 + 100,
                  input,
                  DataTypes.IntegerType,
                  stageName,
                  true);
      checkAnswer(getSession().storedProcedure(sp, 1, 2, 3), new Row[] {Row.create(106)});
      checkAnswer(getSession().storedProcedure(spName, 1, 2, 3), new Row[] {Row.create(106)});
    } finally {
      dropStage(stageName);
      getSession().sql("drop procedure " + spName + "(INT,INT,INT)").show();
    }
  }

  @Test
  public void permanent4() {
    String spName = randomName();
    String stageName = randomStageName();
    DataType[] input = {
      DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType
    };
    try {
      createStage(stageName, false);
      StoredProcedure sp =
          getSession()
              .sproc()
              .registerPermanent(
                  spName,
                  (Session session, Integer col1, Integer col2, Integer col3, Integer col4) ->
                      col1 + col2 + col3 + col4 + 100,
                  input,
                  DataTypes.IntegerType,
                  stageName,
                  true);
      checkAnswer(getSession().storedProcedure(sp, 1, 2, 3, 4), new Row[] {Row.create(110)});
      checkAnswer(getSession().storedProcedure(spName, 1, 2, 3, 4), new Row[] {Row.create(110)});
    } finally {
      dropStage(stageName);
      getSession().sql("drop procedure " + spName + "(INT,INT,INT,INT)").show();
    }
  }

  @Test
  public void permanent5() {
    String spName = randomName();
    String stageName = randomStageName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    try {
      createStage(stageName, false);
      StoredProcedure sp =
          getSession()
              .sproc()
              .registerPermanent(
                  spName,
                  (Session session,
                      Integer col1,
                      Integer col2,
                      Integer col3,
                      Integer col4,
                      Integer col5) -> col1 + col2 + col3 + col4 + col5 + 100,
                  input,
                  DataTypes.IntegerType,
                  stageName,
                  true);
      checkAnswer(getSession().storedProcedure(sp, 1, 2, 3, 4, 5), new Row[] {Row.create(115)});
      checkAnswer(getSession().storedProcedure(spName, 1, 2, 3, 4, 5), new Row[] {Row.create(115)});
    } finally {
      dropStage(stageName);
      getSession().sql("drop procedure " + spName + "(INT,INT,INT,INT,INT)").show();
    }
  }

  @Test
  public void permanent6() {
    String spName = randomName();
    String stageName = randomStageName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    try {
      createStage(stageName, false);
      StoredProcedure sp =
          getSession()
              .sproc()
              .registerPermanent(
                  spName,
                  (Session session,
                      Integer col1,
                      Integer col2,
                      Integer col3,
                      Integer col4,
                      Integer col5,
                      Integer col6) -> col1 + col2 + col3 + col4 + col5 + col6 + 100,
                  input,
                  DataTypes.IntegerType,
                  stageName,
                  true);
      checkAnswer(getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6), new Row[] {Row.create(121)});
      checkAnswer(
          getSession().storedProcedure(spName, 1, 2, 3, 4, 5, 6), new Row[] {Row.create(121)});
    } finally {
      dropStage(stageName);
      getSession().sql("drop procedure " + spName + "(INT,INT,INT,INT,INT,INT)").show();
    }
  }

  @Test
  public void permanent7() {
    String spName = randomName();
    String stageName = randomStageName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    try {
      createStage(stageName, false);
      StoredProcedure sp =
          getSession()
              .sproc()
              .registerPermanent(
                  spName,
                  (Session session,
                      Integer col1,
                      Integer col2,
                      Integer col3,
                      Integer col4,
                      Integer col5,
                      Integer col6,
                      Integer col7) -> col1 + col2 + col3 + col4 + col5 + col6 + col7 + 100,
                  input,
                  DataTypes.IntegerType,
                  stageName,
                  true);
      checkAnswer(
          getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7), new Row[] {Row.create(128)});
      checkAnswer(
          getSession().storedProcedure(spName, 1, 2, 3, 4, 5, 6, 7), new Row[] {Row.create(128)});
    } finally {
      dropStage(stageName);
      getSession().sql("drop procedure " + spName + "(INT,INT,INT,INT,INT,INT,INT)").show();
    }
  }

  @Test
  public void permanent8() {
    String spName = randomName();
    String stageName = randomStageName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    try {
      createStage(stageName, false);
      StoredProcedure sp =
          getSession()
              .sproc()
              .registerPermanent(
                  spName,
                  (Session session,
                      Integer col1,
                      Integer col2,
                      Integer col3,
                      Integer col4,
                      Integer col5,
                      Integer col6,
                      Integer col7,
                      Integer col8) -> col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + 100,
                  input,
                  DataTypes.IntegerType,
                  stageName,
                  true);
      checkAnswer(
          getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8), new Row[] {Row.create(136)});
      checkAnswer(
          getSession().storedProcedure(spName, 1, 2, 3, 4, 5, 6, 7, 8),
          new Row[] {Row.create(136)});
    } finally {
      dropStage(stageName);
      getSession().sql("drop procedure " + spName + "(INT,INT,INT,INT,INT,INT,INT,INT)").show();
    }
  }

  @Test
  public void permanent9() {
    String spName = randomName();
    String stageName = randomStageName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    try {
      createStage(stageName, false);
      StoredProcedure sp =
          getSession()
              .sproc()
              .registerPermanent(
                  spName,
                  (Session session,
                      Integer col1,
                      Integer col2,
                      Integer col3,
                      Integer col4,
                      Integer col5,
                      Integer col6,
                      Integer col7,
                      Integer col8,
                      Integer col9) ->
                      col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + 100,
                  input,
                  DataTypes.IntegerType,
                  stageName,
                  true);
      checkAnswer(
          getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9), new Row[] {Row.create(145)});
      checkAnswer(
          getSession().storedProcedure(spName, 1, 2, 3, 4, 5, 6, 7, 8, 9),
          new Row[] {Row.create(145)});
    } finally {
      dropStage(stageName);
      getSession().sql("drop procedure " + spName + "(INT,INT,INT,INT,INT,INT,INT,INT,INT)").show();
    }
  }

  @Test
  public void permanent10() {
    String spName = randomName();
    String stageName = randomStageName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    try {
      createStage(stageName, false);
      StoredProcedure sp =
          getSession()
              .sproc()
              .registerPermanent(
                  spName,
                  (Session session,
                      Integer col1,
                      Integer col2,
                      Integer col3,
                      Integer col4,
                      Integer col5,
                      Integer col6,
                      Integer col7,
                      Integer col8,
                      Integer col9,
                      Integer col10) ->
                      col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + 100,
                  input,
                  DataTypes.IntegerType,
                  stageName,
                  true);
      checkAnswer(
          getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
          new Row[] {Row.create(155)});
      checkAnswer(
          getSession().storedProcedure(spName, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
          new Row[] {Row.create(155)});
    } finally {
      dropStage(stageName);
      getSession()
          .sql("drop procedure " + spName + "(INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)")
          .show();
    }
  }

  @Test
  public void permanent11() {
    String spName = randomName();
    String stageName = randomStageName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    try {
      createStage(stageName, false);
      StoredProcedure sp =
          getSession()
              .sproc()
              .registerPermanent(
                  spName,
                  (Session session,
                      Integer col1,
                      Integer col2,
                      Integer col3,
                      Integer col4,
                      Integer col5,
                      Integer col6,
                      Integer col7,
                      Integer col8,
                      Integer col9,
                      Integer col10,
                      Integer col11) ->
                      col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11
                          + 100,
                  input,
                  DataTypes.IntegerType,
                  stageName,
                  true);
      checkAnswer(
          getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
          new Row[] {Row.create(166)});
      checkAnswer(
          getSession().storedProcedure(spName, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
          new Row[] {Row.create(166)});
    } finally {
      dropStage(stageName);
      getSession()
          .sql("drop procedure " + spName + "(INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)")
          .show();
    }
  }

  @Test
  public void permanent12() {
    String spName = randomName();
    String stageName = randomStageName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    try {
      createStage(stageName, false);
      StoredProcedure sp =
          getSession()
              .sproc()
              .registerPermanent(
                  spName,
                  (Session session,
                      Integer col1,
                      Integer col2,
                      Integer col3,
                      Integer col4,
                      Integer col5,
                      Integer col6,
                      Integer col7,
                      Integer col8,
                      Integer col9,
                      Integer col10,
                      Integer col11,
                      Integer col12) ->
                      col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11
                          + col12 + 100,
                  input,
                  DataTypes.IntegerType,
                  stageName,
                  true);
      checkAnswer(
          getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
          new Row[] {Row.create(178)});
      checkAnswer(
          getSession().storedProcedure(spName, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
          new Row[] {Row.create(178)});
    } finally {
      dropStage(stageName);
      getSession()
          .sql("drop procedure " + spName + "(INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)")
          .show();
    }
  }

  @Test
  public void permanent13() {
    String spName = randomName();
    String stageName = randomStageName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    try {
      createStage(stageName, false);
      StoredProcedure sp =
          getSession()
              .sproc()
              .registerPermanent(
                  spName,
                  (Session session,
                      Integer col1,
                      Integer col2,
                      Integer col3,
                      Integer col4,
                      Integer col5,
                      Integer col6,
                      Integer col7,
                      Integer col8,
                      Integer col9,
                      Integer col10,
                      Integer col11,
                      Integer col12,
                      Integer col13) ->
                      col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11
                          + col12 + col13 + 100,
                  input,
                  DataTypes.IntegerType,
                  stageName,
                  true);
      checkAnswer(
          getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13),
          new Row[] {Row.create(191)});
      checkAnswer(
          getSession().storedProcedure(spName, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13),
          new Row[] {Row.create(191)});
    } finally {
      dropStage(stageName);
      getSession()
          .sql("drop procedure " + spName + "(INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)")
          .show();
    }
  }

  @Test
  public void permanent14() {
    String spName = randomName();
    String stageName = randomStageName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    try {
      createStage(stageName, false);
      StoredProcedure sp =
          getSession()
              .sproc()
              .registerPermanent(
                  spName,
                  (Session session,
                      Integer col1,
                      Integer col2,
                      Integer col3,
                      Integer col4,
                      Integer col5,
                      Integer col6,
                      Integer col7,
                      Integer col8,
                      Integer col9,
                      Integer col10,
                      Integer col11,
                      Integer col12,
                      Integer col13,
                      Integer col14) ->
                      col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11
                          + col12 + col13 + col14 + 100,
                  input,
                  DataTypes.IntegerType,
                  stageName,
                  true);
      checkAnswer(
          getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14),
          new Row[] {Row.create(205)});
      checkAnswer(
          getSession().storedProcedure(spName, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14),
          new Row[] {Row.create(205)});
    } finally {
      dropStage(stageName);
      getSession()
          .sql(
              "drop procedure "
                  + spName
                  + "(INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)")
          .show();
    }
  }

  @Test
  public void permanent15() {
    String spName = randomName();
    String stageName = randomStageName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    try {
      createStage(stageName, false);
      StoredProcedure sp =
          getSession()
              .sproc()
              .registerPermanent(
                  spName,
                  (Session session,
                      Integer col1,
                      Integer col2,
                      Integer col3,
                      Integer col4,
                      Integer col5,
                      Integer col6,
                      Integer col7,
                      Integer col8,
                      Integer col9,
                      Integer col10,
                      Integer col11,
                      Integer col12,
                      Integer col13,
                      Integer col14,
                      Integer col15) ->
                      col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11
                          + col12 + col13 + col14 + col15 + 100,
                  input,
                  DataTypes.IntegerType,
                  stageName,
                  true);
      checkAnswer(
          getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15),
          new Row[] {Row.create(220)});
      checkAnswer(
          getSession().storedProcedure(spName, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15),
          new Row[] {Row.create(220)});
    } finally {
      dropStage(stageName);
      getSession()
          .sql(
              "drop procedure "
                  + spName
                  + "(INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)")
          .show();
    }
  }

  @Test
  public void permanent16() {
    String spName = randomName();
    String stageName = randomStageName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    try {
      createStage(stageName, false);
      StoredProcedure sp =
          getSession()
              .sproc()
              .registerPermanent(
                  spName,
                  (Session session,
                      Integer col1,
                      Integer col2,
                      Integer col3,
                      Integer col4,
                      Integer col5,
                      Integer col6,
                      Integer col7,
                      Integer col8,
                      Integer col9,
                      Integer col10,
                      Integer col11,
                      Integer col12,
                      Integer col13,
                      Integer col14,
                      Integer col15,
                      Integer col16) ->
                      col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11
                          + col12 + col13 + col14 + col15 + col16 + 100,
                  input,
                  DataTypes.IntegerType,
                  stageName,
                  true);
      checkAnswer(
          getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16),
          new Row[] {Row.create(236)});
      checkAnswer(
          getSession()
              .storedProcedure(spName, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16),
          new Row[] {Row.create(236)});
    } finally {
      dropStage(stageName);
      getSession()
          .sql(
              "drop procedure "
                  + spName
                  + "(INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)")
          .show();
    }
  }

  @Test
  public void permanent17() {
    String spName = randomName();
    String stageName = randomStageName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    try {
      createStage(stageName, false);
      StoredProcedure sp =
          getSession()
              .sproc()
              .registerPermanent(
                  spName,
                  (Session session,
                      Integer col1,
                      Integer col2,
                      Integer col3,
                      Integer col4,
                      Integer col5,
                      Integer col6,
                      Integer col7,
                      Integer col8,
                      Integer col9,
                      Integer col10,
                      Integer col11,
                      Integer col12,
                      Integer col13,
                      Integer col14,
                      Integer col15,
                      Integer col16,
                      Integer col17) ->
                      col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11
                          + col12 + col13 + col14 + col15 + col16 + col17 + 100,
                  input,
                  DataTypes.IntegerType,
                  stageName,
                  true);
      checkAnswer(
          getSession()
              .storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17),
          new Row[] {Row.create(253)});
      checkAnswer(
          getSession()
              .storedProcedure(spName, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17),
          new Row[] {Row.create(253)});
    } finally {
      dropStage(stageName);
      getSession()
          .sql(
              "drop procedure "
                  + spName
                  + "(INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)")
          .show();
    }
  }

  @Test
  public void permanent18() {
    String spName = randomName();
    String stageName = randomStageName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    try {
      createStage(stageName, false);
      StoredProcedure sp =
          getSession()
              .sproc()
              .registerPermanent(
                  spName,
                  (Session session,
                      Integer col1,
                      Integer col2,
                      Integer col3,
                      Integer col4,
                      Integer col5,
                      Integer col6,
                      Integer col7,
                      Integer col8,
                      Integer col9,
                      Integer col10,
                      Integer col11,
                      Integer col12,
                      Integer col13,
                      Integer col14,
                      Integer col15,
                      Integer col16,
                      Integer col17,
                      Integer col18) ->
                      col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11
                          + col12 + col13 + col14 + col15 + col16 + col17 + col18 + 100,
                  input,
                  DataTypes.IntegerType,
                  stageName,
                  true);
      checkAnswer(
          getSession()
              .storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18),
          new Row[] {Row.create(271)});
      checkAnswer(
          getSession()
              .storedProcedure(
                  spName, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18),
          new Row[] {Row.create(271)});
    } finally {
      dropStage(stageName);
      getSession()
          .sql(
              "drop procedure "
                  + spName
                  + "(INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)")
          .show();
    }
  }

  @Test
  public void permanent19() {
    String spName = randomName();
    String stageName = randomStageName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    try {
      createStage(stageName, false);
      StoredProcedure sp =
          getSession()
              .sproc()
              .registerPermanent(
                  spName,
                  (Session session,
                      Integer col1,
                      Integer col2,
                      Integer col3,
                      Integer col4,
                      Integer col5,
                      Integer col6,
                      Integer col7,
                      Integer col8,
                      Integer col9,
                      Integer col10,
                      Integer col11,
                      Integer col12,
                      Integer col13,
                      Integer col14,
                      Integer col15,
                      Integer col16,
                      Integer col17,
                      Integer col18,
                      Integer col19) ->
                      col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11
                          + col12 + col13 + col14 + col15 + col16 + col17 + col18 + col19 + 100,
                  input,
                  DataTypes.IntegerType,
                  stageName,
                  true);
      checkAnswer(
          getSession()
              .storedProcedure(
                  sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19),
          new Row[] {Row.create(290)});
      checkAnswer(
          getSession()
              .storedProcedure(
                  spName, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19),
          new Row[] {Row.create(290)});
    } finally {
      dropStage(stageName);
      getSession()
          .sql(
              "drop procedure "
                  + spName
                  + "(INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)")
          .show();
    }
  }

  @Test
  public void permanent20() {
    String spName = randomName();
    String stageName = randomStageName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    try {
      createStage(stageName, false);
      StoredProcedure sp =
          getSession()
              .sproc()
              .registerPermanent(
                  spName,
                  (Session session,
                      Integer col1,
                      Integer col2,
                      Integer col3,
                      Integer col4,
                      Integer col5,
                      Integer col6,
                      Integer col7,
                      Integer col8,
                      Integer col9,
                      Integer col10,
                      Integer col11,
                      Integer col12,
                      Integer col13,
                      Integer col14,
                      Integer col15,
                      Integer col16,
                      Integer col17,
                      Integer col18,
                      Integer col19,
                      Integer col20) ->
                      col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11
                          + col12 + col13 + col14 + col15 + col16 + col17 + col18 + col19 + col20
                          + 100,
                  input,
                  DataTypes.IntegerType,
                  stageName,
                  true);
      checkAnswer(
          getSession()
              .storedProcedure(
                  sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
          new Row[] {Row.create(310)});
      checkAnswer(
          getSession()
              .storedProcedure(
                  spName, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
          new Row[] {Row.create(310)});
    } finally {
      dropStage(stageName);
      getSession()
          .sql(
              "drop procedure "
                  + spName
                  + "(INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)")
          .show();
    }
  }

  @Test
  public void permanent21() {
    String spName = randomName();
    String stageName = randomStageName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    try {
      createStage(stageName, false);
      StoredProcedure sp =
          getSession()
              .sproc()
              .registerPermanent(
                  spName,
                  (Session session,
                      Integer col1,
                      Integer col2,
                      Integer col3,
                      Integer col4,
                      Integer col5,
                      Integer col6,
                      Integer col7,
                      Integer col8,
                      Integer col9,
                      Integer col10,
                      Integer col11,
                      Integer col12,
                      Integer col13,
                      Integer col14,
                      Integer col15,
                      Integer col16,
                      Integer col17,
                      Integer col18,
                      Integer col19,
                      Integer col20,
                      Integer col21) ->
                      col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11
                          + col12 + col13 + col14 + col15 + col16 + col17 + col18 + col19 + col20
                          + col21 + 100,
                  input,
                  DataTypes.IntegerType,
                  stageName,
                  true);
      checkAnswer(
          getSession()
              .storedProcedure(
                  sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21),
          new Row[] {Row.create(331)});
      checkAnswer(
          getSession()
              .storedProcedure(
                  spName, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                  21),
          new Row[] {Row.create(331)});
    } finally {
      dropStage(stageName);
      getSession()
          .sql(
              "drop procedure "
                  + spName
                  + "(INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)")
          .show();
    }
  }

  @Test
  public void permIsPerm() {
    String spName = randomName();
    String stageName = randomStageName();
    try {
      createStage(stageName, false);
      StoredProcedure sp =
          getSession()
              .sproc()
              .registerPermanent(
                  spName, session -> "SUCCESS", DataTypes.StringType, stageName, true);
      assert getSession().sql("show procedures like '" + spName + "'").collect().length == 1;
      Session newSession = Session.builder().configFile(defaultProfile).create();
      assert newSession.sql("show procedures like '" + spName + "'").collect().length == 1;
      newSession.close();
    } finally {
      dropStage(stageName);
      getSession().sql("drop procedure " + spName + "()").show();
    }
  }

  @Test
  public void temporaryAnonymous0() {
    JavaSProc0<String> func = session -> "SUCCESS";
    StoredProcedure sp = getSession().sproc().registerTemporary(func, DataTypes.StringType);
    String result = (String) getSession().sproc().runLocally(func);
    checkAnswer(getSession().storedProcedure(sp), new Row[] {Row.create(result)});
    assert result.equals("SUCCESS");
  }

  //  Code below for test 1-21 generated by this script
  // (1 to 21).foreach { x =>
  //    val data = (1 to x).map(i => s"$i").mkString(",")
  //    val args = (1 to x).map(i => s"Integer col$i").mkString(", ")
  //    val func = (1 to x).map(i => s"col$i").mkString(""," + ", "+ 100")
  //    val input = (1 to x).map(_ => "DataTypes.IntegerType").mkString("{", ", ", "}")
  //    val types = x match {
  //      case 1 => "DataTypes.IntegerType"
  //      case _ => "input"
  //    }
  //    val argTypes = (0 to x).map(_ => "Integer").mkString("", ", ", "")
  //    val result = 100 + (1 to x).reduce(_ + _)
  //    println(s"""
  // |@Test
  // |public void temporaryAnonymous$x() {
  // | JavaSProc$x<$argTypes> func = (Session session, $args) -> $func;
  // | DataType[] input = $input;
  // | StoredProcedure sp =
  // |  getSession().sproc().registerTemporary(
  // |   func,
  // |   $types,
  // |   DataTypes.IntegerType
  // |  );
  // | int result = (Integer) getSession().sproc().runLocally(func, $data);
  // | checkAnswer(getSession().storedProcedure(sp, $data),
  // |  new Row[] {Row.create(result)});
  // | assert result == $result;
  // |}""".stripMargin)
  //  }

  @Test
  public void temporaryAnonymous1() {
    JavaSProc1<Integer, Integer> func = (Session session, Integer col1) -> col1 + 100;
    DataType[] input = {DataTypes.IntegerType};
    StoredProcedure sp =
        getSession().sproc().registerTemporary(func, DataTypes.IntegerType, DataTypes.IntegerType);
    int result = (Integer) getSession().sproc().runLocally(func, 1);
    checkAnswer(getSession().storedProcedure(sp, 1), new Row[] {Row.create(result)});
    assert result == 101;
  }

  @Test
  public void temporaryAnonymous2() {
    JavaSProc2<Integer, Integer, Integer> func =
        (Session session, Integer col1, Integer col2) -> col1 + col2 + 100;
    DataType[] input = {DataTypes.IntegerType, DataTypes.IntegerType};
    StoredProcedure sp = getSession().sproc().registerTemporary(func, input, DataTypes.IntegerType);
    int result = (Integer) getSession().sproc().runLocally(func, 1, 2);
    checkAnswer(getSession().storedProcedure(sp, 1, 2), new Row[] {Row.create(result)});
    assert result == 103;
  }

  @Test
  public void temporaryAnonymous3() {
    JavaSProc3<Integer, Integer, Integer, Integer> func =
        (Session session, Integer col1, Integer col2, Integer col3) -> col1 + col2 + col3 + 100;
    DataType[] input = {DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType};
    StoredProcedure sp = getSession().sproc().registerTemporary(func, input, DataTypes.IntegerType);
    int result = (Integer) getSession().sproc().runLocally(func, 1, 2, 3);
    checkAnswer(getSession().storedProcedure(sp, 1, 2, 3), new Row[] {Row.create(result)});
    assert result == 106;
  }

  @Test
  public void temporaryAnonymous4() {
    JavaSProc4<Integer, Integer, Integer, Integer, Integer> func =
        (Session session, Integer col1, Integer col2, Integer col3, Integer col4) ->
            col1 + col2 + col3 + col4 + 100;
    DataType[] input = {
      DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType
    };
    StoredProcedure sp = getSession().sproc().registerTemporary(func, input, DataTypes.IntegerType);
    int result = (Integer) getSession().sproc().runLocally(func, 1, 2, 3, 4);
    checkAnswer(getSession().storedProcedure(sp, 1, 2, 3, 4), new Row[] {Row.create(result)});
    assert result == 110;
  }

  @Test
  public void temporaryAnonymous5() {
    JavaSProc5<Integer, Integer, Integer, Integer, Integer, Integer> func =
        (Session session, Integer col1, Integer col2, Integer col3, Integer col4, Integer col5) ->
            col1 + col2 + col3 + col4 + col5 + 100;
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp = getSession().sproc().registerTemporary(func, input, DataTypes.IntegerType);
    int result = (Integer) getSession().sproc().runLocally(func, 1, 2, 3, 4, 5);
    checkAnswer(getSession().storedProcedure(sp, 1, 2, 3, 4, 5), new Row[] {Row.create(result)});
    assert result == 115;
  }

  @Test
  public void temporaryAnonymous6() {
    JavaSProc6<Integer, Integer, Integer, Integer, Integer, Integer, Integer> func =
        (Session session,
            Integer col1,
            Integer col2,
            Integer col3,
            Integer col4,
            Integer col5,
            Integer col6) -> col1 + col2 + col3 + col4 + col5 + col6 + 100;
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp = getSession().sproc().registerTemporary(func, input, DataTypes.IntegerType);
    int result = (Integer) getSession().sproc().runLocally(func, 1, 2, 3, 4, 5, 6);
    checkAnswer(getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6), new Row[] {Row.create(result)});
    assert result == 121;
  }

  @Test
  public void temporaryAnonymous7() {
    JavaSProc7<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> func =
        (Session session,
            Integer col1,
            Integer col2,
            Integer col3,
            Integer col4,
            Integer col5,
            Integer col6,
            Integer col7) -> col1 + col2 + col3 + col4 + col5 + col6 + col7 + 100;
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp = getSession().sproc().registerTemporary(func, input, DataTypes.IntegerType);
    int result = (Integer) getSession().sproc().runLocally(func, 1, 2, 3, 4, 5, 6, 7);
    checkAnswer(
        getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7), new Row[] {Row.create(result)});
    assert result == 128;
  }

  @Test
  public void temporaryAnonymous8() {
    JavaSProc8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>
        func =
            (Session session,
                Integer col1,
                Integer col2,
                Integer col3,
                Integer col4,
                Integer col5,
                Integer col6,
                Integer col7,
                Integer col8) -> col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + 100;
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp = getSession().sproc().registerTemporary(func, input, DataTypes.IntegerType);
    int result = (Integer) getSession().sproc().runLocally(func, 1, 2, 3, 4, 5, 6, 7, 8);
    checkAnswer(
        getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8), new Row[] {Row.create(result)});
    assert result == 136;
  }

  @Test
  public void temporaryAnonymous9() {
    JavaSProc9<
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer>
        func =
            (Session session,
                Integer col1,
                Integer col2,
                Integer col3,
                Integer col4,
                Integer col5,
                Integer col6,
                Integer col7,
                Integer col8,
                Integer col9) -> col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + 100;
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp = getSession().sproc().registerTemporary(func, input, DataTypes.IntegerType);
    int result = (Integer) getSession().sproc().runLocally(func, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    checkAnswer(
        getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9),
        new Row[] {Row.create(result)});
    assert result == 145;
  }

  @Test
  public void temporaryAnonymous10() {
    JavaSProc10<
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer>
        func =
            (Session session,
                Integer col1,
                Integer col2,
                Integer col3,
                Integer col4,
                Integer col5,
                Integer col6,
                Integer col7,
                Integer col8,
                Integer col9,
                Integer col10) ->
                col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + 100;
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp = getSession().sproc().registerTemporary(func, input, DataTypes.IntegerType);
    int result = (Integer) getSession().sproc().runLocally(func, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    checkAnswer(
        getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
        new Row[] {Row.create(result)});
    assert result == 155;
  }

  @Test
  public void temporaryAnonymous11() {
    JavaSProc11<
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer>
        func =
            (Session session,
                Integer col1,
                Integer col2,
                Integer col3,
                Integer col4,
                Integer col5,
                Integer col6,
                Integer col7,
                Integer col8,
                Integer col9,
                Integer col10,
                Integer col11) ->
                col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11 + 100;
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp = getSession().sproc().registerTemporary(func, input, DataTypes.IntegerType);
    int result = (Integer) getSession().sproc().runLocally(func, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
    checkAnswer(
        getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
        new Row[] {Row.create(result)});
    assert result == 166;
  }

  @Test
  public void temporaryAnonymous12() {
    JavaSProc12<
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer>
        func =
            (Session session,
                Integer col1,
                Integer col2,
                Integer col3,
                Integer col4,
                Integer col5,
                Integer col6,
                Integer col7,
                Integer col8,
                Integer col9,
                Integer col10,
                Integer col11,
                Integer col12) ->
                col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11 + col12
                    + 100;
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp = getSession().sproc().registerTemporary(func, input, DataTypes.IntegerType);
    int result =
        (Integer) getSession().sproc().runLocally(func, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
    checkAnswer(
        getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
        new Row[] {Row.create(result)});
    assert result == 178;
  }

  @Test
  public void temporaryAnonymous13() {
    JavaSProc13<
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer>
        func =
            (Session session,
                Integer col1,
                Integer col2,
                Integer col3,
                Integer col4,
                Integer col5,
                Integer col6,
                Integer col7,
                Integer col8,
                Integer col9,
                Integer col10,
                Integer col11,
                Integer col12,
                Integer col13) ->
                col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11 + col12
                    + col13 + 100;
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp = getSession().sproc().registerTemporary(func, input, DataTypes.IntegerType);
    int result =
        (Integer) getSession().sproc().runLocally(func, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13);
    checkAnswer(
        getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13),
        new Row[] {Row.create(result)});
    assert result == 191;
  }

  @Test
  public void temporaryAnonymous14() {
    JavaSProc14<
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer>
        func =
            (Session session,
                Integer col1,
                Integer col2,
                Integer col3,
                Integer col4,
                Integer col5,
                Integer col6,
                Integer col7,
                Integer col8,
                Integer col9,
                Integer col10,
                Integer col11,
                Integer col12,
                Integer col13,
                Integer col14) ->
                col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11 + col12
                    + col13 + col14 + 100;
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp = getSession().sproc().registerTemporary(func, input, DataTypes.IntegerType);
    int result =
        (Integer)
            getSession().sproc().runLocally(func, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);
    checkAnswer(
        getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14),
        new Row[] {Row.create(result)});
    assert result == 205;
  }

  @Test
  public void temporaryAnonymous15() {
    JavaSProc15<
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer>
        func =
            (Session session,
                Integer col1,
                Integer col2,
                Integer col3,
                Integer col4,
                Integer col5,
                Integer col6,
                Integer col7,
                Integer col8,
                Integer col9,
                Integer col10,
                Integer col11,
                Integer col12,
                Integer col13,
                Integer col14,
                Integer col15) ->
                col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11 + col12
                    + col13 + col14 + col15 + 100;
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp = getSession().sproc().registerTemporary(func, input, DataTypes.IntegerType);
    int result =
        (Integer)
            getSession()
                .sproc()
                .runLocally(func, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
    checkAnswer(
        getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15),
        new Row[] {Row.create(result)});
    assert result == 220;
  }

  @Test
  public void temporaryAnonymous16() {
    JavaSProc16<
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer>
        func =
            (Session session,
                Integer col1,
                Integer col2,
                Integer col3,
                Integer col4,
                Integer col5,
                Integer col6,
                Integer col7,
                Integer col8,
                Integer col9,
                Integer col10,
                Integer col11,
                Integer col12,
                Integer col13,
                Integer col14,
                Integer col15,
                Integer col16) ->
                col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11 + col12
                    + col13 + col14 + col15 + col16 + 100;
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp = getSession().sproc().registerTemporary(func, input, DataTypes.IntegerType);
    int result =
        (Integer)
            getSession()
                .sproc()
                .runLocally(func, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16);
    checkAnswer(
        getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16),
        new Row[] {Row.create(result)});
    assert result == 236;
  }

  @Test
  public void temporaryAnonymous17() {
    JavaSProc17<
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer>
        func =
            (Session session,
                Integer col1,
                Integer col2,
                Integer col3,
                Integer col4,
                Integer col5,
                Integer col6,
                Integer col7,
                Integer col8,
                Integer col9,
                Integer col10,
                Integer col11,
                Integer col12,
                Integer col13,
                Integer col14,
                Integer col15,
                Integer col16,
                Integer col17) ->
                col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11 + col12
                    + col13 + col14 + col15 + col16 + col17 + 100;
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp = getSession().sproc().registerTemporary(func, input, DataTypes.IntegerType);
    int result =
        (Integer)
            getSession()
                .sproc()
                .runLocally(func, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17);
    checkAnswer(
        getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17),
        new Row[] {Row.create(result)});
    assert result == 253;
  }

  @Test
  public void temporaryAnonymous18() {
    JavaSProc18<
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer>
        func =
            (Session session,
                Integer col1,
                Integer col2,
                Integer col3,
                Integer col4,
                Integer col5,
                Integer col6,
                Integer col7,
                Integer col8,
                Integer col9,
                Integer col10,
                Integer col11,
                Integer col12,
                Integer col13,
                Integer col14,
                Integer col15,
                Integer col16,
                Integer col17,
                Integer col18) ->
                col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11 + col12
                    + col13 + col14 + col15 + col16 + col17 + col18 + 100;
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp = getSession().sproc().registerTemporary(func, input, DataTypes.IntegerType);
    int result =
        (Integer)
            getSession()
                .sproc()
                .runLocally(func, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18);
    checkAnswer(
        getSession()
            .storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18),
        new Row[] {Row.create(result)});
    assert result == 271;
  }

  @Test
  public void temporaryAnonymous19() {
    JavaSProc19<
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer>
        func =
            (Session session,
                Integer col1,
                Integer col2,
                Integer col3,
                Integer col4,
                Integer col5,
                Integer col6,
                Integer col7,
                Integer col8,
                Integer col9,
                Integer col10,
                Integer col11,
                Integer col12,
                Integer col13,
                Integer col14,
                Integer col15,
                Integer col16,
                Integer col17,
                Integer col18,
                Integer col19) ->
                col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11 + col12
                    + col13 + col14 + col15 + col16 + col17 + col18 + col19 + 100;
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp = getSession().sproc().registerTemporary(func, input, DataTypes.IntegerType);
    int result =
        (Integer)
            getSession()
                .sproc()
                .runLocally(
                    func, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19);
    checkAnswer(
        getSession()
            .storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19),
        new Row[] {Row.create(result)});
    assert result == 290;
  }

  @Test
  public void temporaryAnonymous20() {
    JavaSProc20<
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer>
        func =
            (Session session,
                Integer col1,
                Integer col2,
                Integer col3,
                Integer col4,
                Integer col5,
                Integer col6,
                Integer col7,
                Integer col8,
                Integer col9,
                Integer col10,
                Integer col11,
                Integer col12,
                Integer col13,
                Integer col14,
                Integer col15,
                Integer col16,
                Integer col17,
                Integer col18,
                Integer col19,
                Integer col20) ->
                col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11 + col12
                    + col13 + col14 + col15 + col16 + col17 + col18 + col19 + col20 + 100;
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp = getSession().sproc().registerTemporary(func, input, DataTypes.IntegerType);
    int result =
        (Integer)
            getSession()
                .sproc()
                .runLocally(
                    func, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20);
    checkAnswer(
        getSession()
            .storedProcedure(
                sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
        new Row[] {Row.create(result)});
    assert result == 310;
  }

  @Test
  public void temporaryAnonymous21() {
    JavaSProc21<
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer>
        func =
            (Session session,
                Integer col1,
                Integer col2,
                Integer col3,
                Integer col4,
                Integer col5,
                Integer col6,
                Integer col7,
                Integer col8,
                Integer col9,
                Integer col10,
                Integer col11,
                Integer col12,
                Integer col13,
                Integer col14,
                Integer col15,
                Integer col16,
                Integer col17,
                Integer col18,
                Integer col19,
                Integer col20,
                Integer col21) ->
                col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11 + col12
                    + col13 + col14 + col15 + col16 + col17 + col18 + col19 + col20 + col21 + 100;
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp = getSession().sproc().registerTemporary(func, input, DataTypes.IntegerType);
    int result =
        (Integer)
            getSession()
                .sproc()
                .runLocally(
                    func, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                    21);
    checkAnswer(
        getSession()
            .storedProcedure(
                sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21),
        new Row[] {Row.create(result)});
    assert result == 331;
  }

  @Test
  public void temporaryNamed0() {
    String name = randomName();
    StoredProcedure sp =
        getSession().sproc().registerTemporary(name, session -> "SUCCESS", DataTypes.StringType);
    checkAnswer(getSession().storedProcedure(sp), new Row[] {Row.create("SUCCESS")});
    checkAnswer(getSession().storedProcedure(name), new Row[] {Row.create("SUCCESS")});
  }

  //  Code below for test 1-21 generated by this script
  // (1 to 21).foreach { x =>
  //    val data = (1 to x).map(i => s"$i").mkString(",")
  //    val args = (1 to x).map(i => s"Integer col$i").mkString(", ")
  //    val func = (1 to x).map(i => s"col$i").mkString(""," + ", "+ 100")
  //    val input = (1 to x).map(_ => "DataTypes.IntegerType").mkString("{", ", ", "}")
  //    val types = x match {
  //      case 1 => "DataTypes.IntegerType"
  //      case _ => "input"
  //    }
  //    val result = 100 + (1 to x).reduce(_ + _)
  //    println(s"""
  //      |@Test
  //      |public void temporaryNamed$x() {
  //      | String name = randomName();
  //      | DataType[] input = $input;
  //      | StoredProcedure sp =
  //      |  getSession().sproc().registerTemporary(
  //      |   name,
  //      |   (Session session, $args) -> $func,
  //      |   $types,
  //      |   DataTypes.IntegerType
  //      |  );
  //      | checkAnswer(getSession().storedProcedure(sp, $data),
  //      |  new Row[] {Row.create($result)});
  //      | checkAnswer(getSession().storedProcedure(name, $data),
  //      |  new Row[] {Row.create($result)});
  //      |}""".stripMargin)
  //  }

  @Test
  public void temporaryNamed1() {
    String name = randomName();
    DataType[] input = {DataTypes.IntegerType};
    StoredProcedure sp =
        getSession()
            .sproc()
            .registerTemporary(
                name,
                (Session session, Integer col1) -> col1 + 100,
                DataTypes.IntegerType,
                DataTypes.IntegerType);
    checkAnswer(getSession().storedProcedure(sp, 1), new Row[] {Row.create(101)});
    checkAnswer(getSession().storedProcedure(name, 1), new Row[] {Row.create(101)});
  }

  @Test
  public void temporaryNamed2() {
    String name = randomName();
    DataType[] input = {DataTypes.IntegerType, DataTypes.IntegerType};
    StoredProcedure sp =
        getSession()
            .sproc()
            .registerTemporary(
                name,
                (Session session, Integer col1, Integer col2) -> col1 + col2 + 100,
                input,
                DataTypes.IntegerType);
    checkAnswer(getSession().storedProcedure(sp, 1, 2), new Row[] {Row.create(103)});
    checkAnswer(getSession().storedProcedure(name, 1, 2), new Row[] {Row.create(103)});
  }

  @Test
  public void temporaryNamed3() {
    String name = randomName();
    DataType[] input = {DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType};
    StoredProcedure sp =
        getSession()
            .sproc()
            .registerTemporary(
                name,
                (Session session, Integer col1, Integer col2, Integer col3) ->
                    col1 + col2 + col3 + 100,
                input,
                DataTypes.IntegerType);
    checkAnswer(getSession().storedProcedure(sp, 1, 2, 3), new Row[] {Row.create(106)});
    checkAnswer(getSession().storedProcedure(name, 1, 2, 3), new Row[] {Row.create(106)});
  }

  @Test
  public void temporaryNamed4() {
    String name = randomName();
    DataType[] input = {
      DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType
    };
    StoredProcedure sp =
        getSession()
            .sproc()
            .registerTemporary(
                name,
                (Session session, Integer col1, Integer col2, Integer col3, Integer col4) ->
                    col1 + col2 + col3 + col4 + 100,
                input,
                DataTypes.IntegerType);
    checkAnswer(getSession().storedProcedure(sp, 1, 2, 3, 4), new Row[] {Row.create(110)});
    checkAnswer(getSession().storedProcedure(name, 1, 2, 3, 4), new Row[] {Row.create(110)});
  }

  @Test
  public void temporaryNamed5() {
    String name = randomName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp =
        getSession()
            .sproc()
            .registerTemporary(
                name,
                (Session session,
                    Integer col1,
                    Integer col2,
                    Integer col3,
                    Integer col4,
                    Integer col5) -> col1 + col2 + col3 + col4 + col5 + 100,
                input,
                DataTypes.IntegerType);
    checkAnswer(getSession().storedProcedure(sp, 1, 2, 3, 4, 5), new Row[] {Row.create(115)});
    checkAnswer(getSession().storedProcedure(name, 1, 2, 3, 4, 5), new Row[] {Row.create(115)});
  }

  @Test
  public void temporaryNamed6() {
    String name = randomName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp =
        getSession()
            .sproc()
            .registerTemporary(
                name,
                (Session session,
                    Integer col1,
                    Integer col2,
                    Integer col3,
                    Integer col4,
                    Integer col5,
                    Integer col6) -> col1 + col2 + col3 + col4 + col5 + col6 + 100,
                input,
                DataTypes.IntegerType);
    checkAnswer(getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6), new Row[] {Row.create(121)});
    checkAnswer(getSession().storedProcedure(name, 1, 2, 3, 4, 5, 6), new Row[] {Row.create(121)});
  }

  @Test
  public void temporaryNamed7() {
    String name = randomName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp =
        getSession()
            .sproc()
            .registerTemporary(
                name,
                (Session session,
                    Integer col1,
                    Integer col2,
                    Integer col3,
                    Integer col4,
                    Integer col5,
                    Integer col6,
                    Integer col7) -> col1 + col2 + col3 + col4 + col5 + col6 + col7 + 100,
                input,
                DataTypes.IntegerType);
    checkAnswer(getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7), new Row[] {Row.create(128)});
    checkAnswer(
        getSession().storedProcedure(name, 1, 2, 3, 4, 5, 6, 7), new Row[] {Row.create(128)});
  }

  @Test
  public void temporaryNamed8() {
    String name = randomName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp =
        getSession()
            .sproc()
            .registerTemporary(
                name,
                (Session session,
                    Integer col1,
                    Integer col2,
                    Integer col3,
                    Integer col4,
                    Integer col5,
                    Integer col6,
                    Integer col7,
                    Integer col8) -> col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + 100,
                input,
                DataTypes.IntegerType);
    checkAnswer(
        getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8), new Row[] {Row.create(136)});
    checkAnswer(
        getSession().storedProcedure(name, 1, 2, 3, 4, 5, 6, 7, 8), new Row[] {Row.create(136)});
  }

  @Test
  public void temporaryNamed9() {
    String name = randomName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp =
        getSession()
            .sproc()
            .registerTemporary(
                name,
                (Session session,
                    Integer col1,
                    Integer col2,
                    Integer col3,
                    Integer col4,
                    Integer col5,
                    Integer col6,
                    Integer col7,
                    Integer col8,
                    Integer col9) ->
                    col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + 100,
                input,
                DataTypes.IntegerType);
    checkAnswer(
        getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9), new Row[] {Row.create(145)});
    checkAnswer(
        getSession().storedProcedure(name, 1, 2, 3, 4, 5, 6, 7, 8, 9), new Row[] {Row.create(145)});
  }

  @Test
  public void temporaryNamed10() {
    String name = randomName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp =
        getSession()
            .sproc()
            .registerTemporary(
                name,
                (Session session,
                    Integer col1,
                    Integer col2,
                    Integer col3,
                    Integer col4,
                    Integer col5,
                    Integer col6,
                    Integer col7,
                    Integer col8,
                    Integer col9,
                    Integer col10) ->
                    col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + 100,
                input,
                DataTypes.IntegerType);
    checkAnswer(
        getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
        new Row[] {Row.create(155)});
    checkAnswer(
        getSession().storedProcedure(name, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
        new Row[] {Row.create(155)});
  }

  @Test
  public void temporaryNamed11() {
    String name = randomName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp =
        getSession()
            .sproc()
            .registerTemporary(
                name,
                (Session session,
                    Integer col1,
                    Integer col2,
                    Integer col3,
                    Integer col4,
                    Integer col5,
                    Integer col6,
                    Integer col7,
                    Integer col8,
                    Integer col9,
                    Integer col10,
                    Integer col11) ->
                    col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11
                        + 100,
                input,
                DataTypes.IntegerType);
    checkAnswer(
        getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
        new Row[] {Row.create(166)});
    checkAnswer(
        getSession().storedProcedure(name, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
        new Row[] {Row.create(166)});
  }

  @Test
  public void temporaryNamed12() {
    String name = randomName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp =
        getSession()
            .sproc()
            .registerTemporary(
                name,
                (Session session,
                    Integer col1,
                    Integer col2,
                    Integer col3,
                    Integer col4,
                    Integer col5,
                    Integer col6,
                    Integer col7,
                    Integer col8,
                    Integer col9,
                    Integer col10,
                    Integer col11,
                    Integer col12) ->
                    col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11
                        + col12 + 100,
                input,
                DataTypes.IntegerType);
    checkAnswer(
        getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
        new Row[] {Row.create(178)});
    checkAnswer(
        getSession().storedProcedure(name, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
        new Row[] {Row.create(178)});
  }

  @Test
  public void temporaryNamed13() {
    String name = randomName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp =
        getSession()
            .sproc()
            .registerTemporary(
                name,
                (Session session,
                    Integer col1,
                    Integer col2,
                    Integer col3,
                    Integer col4,
                    Integer col5,
                    Integer col6,
                    Integer col7,
                    Integer col8,
                    Integer col9,
                    Integer col10,
                    Integer col11,
                    Integer col12,
                    Integer col13) ->
                    col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11
                        + col12 + col13 + 100,
                input,
                DataTypes.IntegerType);
    checkAnswer(
        getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13),
        new Row[] {Row.create(191)});
    checkAnswer(
        getSession().storedProcedure(name, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13),
        new Row[] {Row.create(191)});
  }

  @Test
  public void temporaryNamed14() {
    String name = randomName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp =
        getSession()
            .sproc()
            .registerTemporary(
                name,
                (Session session,
                    Integer col1,
                    Integer col2,
                    Integer col3,
                    Integer col4,
                    Integer col5,
                    Integer col6,
                    Integer col7,
                    Integer col8,
                    Integer col9,
                    Integer col10,
                    Integer col11,
                    Integer col12,
                    Integer col13,
                    Integer col14) ->
                    col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11
                        + col12 + col13 + col14 + 100,
                input,
                DataTypes.IntegerType);
    checkAnswer(
        getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14),
        new Row[] {Row.create(205)});
    checkAnswer(
        getSession().storedProcedure(name, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14),
        new Row[] {Row.create(205)});
  }

  @Test
  public void temporaryNamed15() {
    String name = randomName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp =
        getSession()
            .sproc()
            .registerTemporary(
                name,
                (Session session,
                    Integer col1,
                    Integer col2,
                    Integer col3,
                    Integer col4,
                    Integer col5,
                    Integer col6,
                    Integer col7,
                    Integer col8,
                    Integer col9,
                    Integer col10,
                    Integer col11,
                    Integer col12,
                    Integer col13,
                    Integer col14,
                    Integer col15) ->
                    col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11
                        + col12 + col13 + col14 + col15 + 100,
                input,
                DataTypes.IntegerType);
    checkAnswer(
        getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15),
        new Row[] {Row.create(220)});
    checkAnswer(
        getSession().storedProcedure(name, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15),
        new Row[] {Row.create(220)});
  }

  @Test
  public void temporaryNamed16() {
    String name = randomName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp =
        getSession()
            .sproc()
            .registerTemporary(
                name,
                (Session session,
                    Integer col1,
                    Integer col2,
                    Integer col3,
                    Integer col4,
                    Integer col5,
                    Integer col6,
                    Integer col7,
                    Integer col8,
                    Integer col9,
                    Integer col10,
                    Integer col11,
                    Integer col12,
                    Integer col13,
                    Integer col14,
                    Integer col15,
                    Integer col16) ->
                    col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11
                        + col12 + col13 + col14 + col15 + col16 + 100,
                input,
                DataTypes.IntegerType);
    checkAnswer(
        getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16),
        new Row[] {Row.create(236)});
    checkAnswer(
        getSession().storedProcedure(name, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16),
        new Row[] {Row.create(236)});
  }

  @Test
  public void temporaryNamed17() {
    String name = randomName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp =
        getSession()
            .sproc()
            .registerTemporary(
                name,
                (Session session,
                    Integer col1,
                    Integer col2,
                    Integer col3,
                    Integer col4,
                    Integer col5,
                    Integer col6,
                    Integer col7,
                    Integer col8,
                    Integer col9,
                    Integer col10,
                    Integer col11,
                    Integer col12,
                    Integer col13,
                    Integer col14,
                    Integer col15,
                    Integer col16,
                    Integer col17) ->
                    col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11
                        + col12 + col13 + col14 + col15 + col16 + col17 + 100,
                input,
                DataTypes.IntegerType);
    checkAnswer(
        getSession().storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17),
        new Row[] {Row.create(253)});
    checkAnswer(
        getSession()
            .storedProcedure(name, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17),
        new Row[] {Row.create(253)});
  }

  @Test
  public void temporaryNamed18() {
    String name = randomName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp =
        getSession()
            .sproc()
            .registerTemporary(
                name,
                (Session session,
                    Integer col1,
                    Integer col2,
                    Integer col3,
                    Integer col4,
                    Integer col5,
                    Integer col6,
                    Integer col7,
                    Integer col8,
                    Integer col9,
                    Integer col10,
                    Integer col11,
                    Integer col12,
                    Integer col13,
                    Integer col14,
                    Integer col15,
                    Integer col16,
                    Integer col17,
                    Integer col18) ->
                    col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11
                        + col12 + col13 + col14 + col15 + col16 + col17 + col18 + 100,
                input,
                DataTypes.IntegerType);
    checkAnswer(
        getSession()
            .storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18),
        new Row[] {Row.create(271)});
    checkAnswer(
        getSession()
            .storedProcedure(name, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18),
        new Row[] {Row.create(271)});
  }

  @Test
  public void temporaryNamed19() {
    String name = randomName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp =
        getSession()
            .sproc()
            .registerTemporary(
                name,
                (Session session,
                    Integer col1,
                    Integer col2,
                    Integer col3,
                    Integer col4,
                    Integer col5,
                    Integer col6,
                    Integer col7,
                    Integer col8,
                    Integer col9,
                    Integer col10,
                    Integer col11,
                    Integer col12,
                    Integer col13,
                    Integer col14,
                    Integer col15,
                    Integer col16,
                    Integer col17,
                    Integer col18,
                    Integer col19) ->
                    col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11
                        + col12 + col13 + col14 + col15 + col16 + col17 + col18 + col19 + 100,
                input,
                DataTypes.IntegerType);
    checkAnswer(
        getSession()
            .storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19),
        new Row[] {Row.create(290)});
    checkAnswer(
        getSession()
            .storedProcedure(
                name, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19),
        new Row[] {Row.create(290)});
  }

  @Test
  public void temporaryNamed20() {
    String name = randomName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp =
        getSession()
            .sproc()
            .registerTemporary(
                name,
                (Session session,
                    Integer col1,
                    Integer col2,
                    Integer col3,
                    Integer col4,
                    Integer col5,
                    Integer col6,
                    Integer col7,
                    Integer col8,
                    Integer col9,
                    Integer col10,
                    Integer col11,
                    Integer col12,
                    Integer col13,
                    Integer col14,
                    Integer col15,
                    Integer col16,
                    Integer col17,
                    Integer col18,
                    Integer col19,
                    Integer col20) ->
                    col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11
                        + col12 + col13 + col14 + col15 + col16 + col17 + col18 + col19 + col20
                        + 100,
                input,
                DataTypes.IntegerType);
    checkAnswer(
        getSession()
            .storedProcedure(
                sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
        new Row[] {Row.create(310)});
    checkAnswer(
        getSession()
            .storedProcedure(
                name, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
        new Row[] {Row.create(310)});
  }

  @Test
  public void temporaryNamed21() {
    String name = randomName();
    DataType[] input = {
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    };
    StoredProcedure sp =
        getSession()
            .sproc()
            .registerTemporary(
                name,
                (Session session,
                    Integer col1,
                    Integer col2,
                    Integer col3,
                    Integer col4,
                    Integer col5,
                    Integer col6,
                    Integer col7,
                    Integer col8,
                    Integer col9,
                    Integer col10,
                    Integer col11,
                    Integer col12,
                    Integer col13,
                    Integer col14,
                    Integer col15,
                    Integer col16,
                    Integer col17,
                    Integer col18,
                    Integer col19,
                    Integer col20,
                    Integer col21) ->
                    col1 + col2 + col3 + col4 + col5 + col6 + col7 + col8 + col9 + col10 + col11
                        + col12 + col13 + col14 + col15 + col16 + col17 + col18 + col19 + col20
                        + col21 + 100,
                input,
                DataTypes.IntegerType);
    checkAnswer(
        getSession()
            .storedProcedure(
                sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21),
        new Row[] {Row.create(331)});
    checkAnswer(
        getSession()
            .storedProcedure(
                name, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21),
        new Row[] {Row.create(331)});
  }

  @Test
  public void tempIsTemp() {
    String name1 = randomName();
    getSession().sproc().registerTemporary(name1, session -> "SUCCESS", DataTypes.StringType);
    StoredProcedure sp2 =
        getSession().sproc().registerTemporary(session -> "SUCCESS", DataTypes.StringType);
    String[] names = sp2.getName().get().split("\\.");
    String name2 = names[names.length - 1];
    assert getSession().sql("show procedures like '" + name1 + "'").collect().length == 1;
    assert getSession().sql("show procedures like '" + name2 + "'").collect().length == 1;

    Session newSession = Session.builder().configFile(defaultProfile).create();
    assert newSession.sql("show procedures like '" + name1 + "'").collect().length == 0;
    assert newSession.sql("show procedures like '" + name2 + "'").collect().length == 0;
    newSession.close();
  }
}
