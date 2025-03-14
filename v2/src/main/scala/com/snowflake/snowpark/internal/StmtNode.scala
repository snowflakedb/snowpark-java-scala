package com.snowflake.snowpark.internal

import com.snowflake.snowpark.proto.ast.VarId

trait StmtNode {
  // Each Assign statement creates a new variable to represent the result of the assignment.
  // This variable is used to reference the result of the assignment in subsequent expressions.
  private[snowpark] val varId: VarId
}
