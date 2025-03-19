package com.snowflake.snowpark.internal

import com.snowflake.snowpark.proto.ast.Expr

trait ExprNode extends NameIndices {
  private[snowpark] val expr: Expr
}
