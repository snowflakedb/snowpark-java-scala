package com.snowflake.snowpark

import com.snowflake.snowpark.functions.udf
import com.snowflake.snowpark.internal.ParameterUtils
import com.snowflake.snowpark.internal.Utils.{TempObjectType, randomNameForTempObject}
import com.snowflake.snowpark.internal.analyzer.TempType
import com.snowflake.snowpark.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import junit.framework.TestCase.{assertFalse, assertTrue}

class DropTempObjectsSuite extends SNTestBase {
  import session.implicits._
  val randomSchema: String = randomName()
  val tmpStageName: String = randomStageName()
  private val userSchema: StructType = StructType(
    Seq(StructField("a", IntegerType), StructField("b", StringType), StructField("c", DoubleType))
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    session.runQuery(s"CREATE TEMPORARY STAGE $tmpStageName")
    session.runQuery(s"CREATE SCHEMA $randomSchema")
    if (!session.conn.isStoredProc) {
      TestUtils.addDepsToClassPath(session)
    }
  }

  override def afterAll(): Unit = {
    session.runQuery(s"DROP STAGE IF EXISTS $tmpStageName")
    session.runQuery(s"DROP SCHEMA IF EXISTS $randomSchema")
    super.afterAll()
  }

  test("test session dropAllTempObjects") {
    testWithAlteredSessionParameter(
      () => {
        // temp file format
        val df1 = session.read
          .schema(userSchema)
          .option("COMPRESSION", "none")
          .csv(s"@$tmpStageName/$testFileCsv")
        // temp function & temp stage
        udf((a: Int, b: Int) => a == b)
        // temp view with full qualified name
        val randomView = randomNameForTempObject(TempObjectType.View)
        val df = Seq((1, 1), (2, 2), (3, 4)).toDF("a", "b")
        df.createOrReplaceTempView(Seq(session.getCurrentDatabase.get, randomSchema, randomView))
        // temp table
        val df2 = df.cacheResult()

        // Verify result, only view does not use scoped temp objects
        val dropMap = session.getTempObjectMap
        assert(dropMap.size == 1)
        assert(dropMap.values.toSet.equals(Set(TempObjectType.View)))
        dropMap.keys.foreach(k => {
          // Make sure name is fully qualified
          assert(k.split("\\.")(2).startsWith("SNOWPARK_TEMP_"))
        })

        session.dropAllTempObjects()

      },
      ParameterUtils.SnowparkUseScopedTempObjects,
      "true"
    )
  }

  test("test session dropAllTempObjects with scoped temp object turned off") {
    testWithAlteredSessionParameter(
      () => {
        // temp file format
        val df1 = session.read
          .schema(userSchema)
          .option("COMPRESSION", "none")
          .csv(s"@$tmpStageName/$testFileCsv")
        // temp function & temp stage
        udf((a: Int, b: Int) => a == b)
        // temp view with full qualified name
        val randomView = randomNameForTempObject(TempObjectType.View)
        val df = Seq((1, 1), (2, 2), (3, 4)).toDF("a", "b")
        df.createOrReplaceTempView(Seq(session.getCurrentDatabase.get, randomSchema, randomView))
        // temp table
        val df2 = df.cacheResult()

        // Verify result
        val dropMap = session.getTempObjectMap
        assert(dropMap.size == 5)
        assert(
          dropMap.values.toSet.equals(
            Set(
              TempObjectType.View,
              TempObjectType.Stage,
              TempObjectType.Table,
              TempObjectType.FileFormat,
              TempObjectType.Function
            )
          )
        )
        dropMap.keys.foreach(k => {
          // Make sure name is fully qualified
          assert(k.split("\\.")(2).startsWith("SNOWPARK_TEMP_"))
        })

        session.dropAllTempObjects()
      },
      ParameterUtils.SnowparkUseScopedTempObjects,
      "false"
    )
  }

  test("Test recordTempObjectIfNecessary") {
    session.recordTempObjectIfNecessary(
      TempObjectType.Table,
      "db.schema.tempName1",
      TempType.Temporary
    )
    assertTrue(session.getTempObjectMap.contains("db.schema.tempName1"))
    session.recordTempObjectIfNecessary(
      TempObjectType.Table,
      "db.schema.tempName2",
      TempType.ScopedTemporary
    )
    assertFalse(session.getTempObjectMap.contains("db.schema.tempName2"))
    session.recordTempObjectIfNecessary(
      TempObjectType.Table,
      "db.schema.tempName3",
      TempType.Permanent
    )
    assertFalse(session.getTempObjectMap.contains("db.schema.tempName3"))
  }
}
