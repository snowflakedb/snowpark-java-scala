package com.snowflake.test

import com.snowflake.snowpark.{SNTestBase, Session}
import com.snowflake.snowpark.internal.Utils

class QueryTagSuite extends SNTestBase {

  test("Test that the right query tag is generated") {
    assert(Utils.getUserCodeMeta().contains("com.snowflake.test.QueryTagSuite"))
  }

  test("Test that query tag is not set if it is already set") {
    // Have to create a new session, else the lazy val `queryTagIsSet` is reused
    val newSession = Session.builder.configFile(defaultProfile).create

    val someTag = randomName()
    super.runQuery(s"alter session set query_tag ='$someTag'", newSession)
    newSession.createDataFrame(Seq(1, 2, 3)).collect()
    val statement = runQueryReturnStatement("show parameters like 'query_tag'", newSession)
    val res = statement.getResultSet
    res.next()
    assert(res.getString("value").equals(someTag))
    statement.close()
  }
}
