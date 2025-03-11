package com.snowflake.snowpark.internal

import com.snowflake.snowpark.proto.ast.Expr

trait ExprNode {
  private[snowpark] val expr: Expr
}
