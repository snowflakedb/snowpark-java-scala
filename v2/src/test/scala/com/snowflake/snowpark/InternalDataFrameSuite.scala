package com.snowflake.snowpark

import com.snowflake.snowpark.proto.ast.VarId

class InternalDataFrameSuite extends UnitTestBase {
  test("clone") {
    val id = VarId(bitfield1 = 1234L)
    val df = new DataFrame(id)
    assert(df.clone.varId.toProtoString == id.toProtoString)
  }
}
