package com.snowflake.snowpark

import java.sql.ResultSet

import com.snowflake.snowpark.internal.Utils
import com.snowflake.snowpark.internal.Utils.TempObjectType
import com.snowflake.snowpark.types.StructType
import com.snowflake.snowpark_test.TestFunctions

import scala.util.Random

trait SFTestUtils {
  def randomStageName(): String = TestUtils.randomStageName()

  def randomFunctionName(): String = TestUtils.randomFunctionName()

  def randomViewName(): String = TestUtils.randomViewName()

  def randomTableName(): String = TestUtils.randomTableName()

  // todo: need support StructType schema
  def createTable(name: String, schema: String)(implicit session: Session): Unit =
    TestUtils.createTable(name, schema, session)

  def createStage(name: String, isTemporary: Boolean = true)(implicit session: Session): Unit =
    TestUtils.createStage(name, isTemporary, session)

  def dropStage(name: String)(implicit session: Session): Unit =
    TestUtils.dropStage(name, session)

  def dropTable(name: String)(implicit session: Session): Unit =
    TestUtils.dropTable(name, session)

  def insertIntoTable(name: String, data: Seq[Any])(implicit session: Session): Unit =
    TestUtils.insertIntoTable(name, data, session)

  def uploadFileToStage(stageName: String, fileName: String, compress: Boolean)(implicit
      session: Session
  ): Unit =
    TestUtils.uploadFileToStage(stageName, fileName, compress, session)

  def verifySchema(sql: String, expectedSchema: StructType)(implicit session: Session): Unit =
    TestUtils.verifySchema(sql, expectedSchema, session)

  def dropView(name: String)(implicit session: Session): Unit =
    TestUtils.dropView(name, session)

  def equalSFObjectName(name1: String, name2: String): Boolean =
    TestUtils.equalSFObjectName(name1, name2)
}
