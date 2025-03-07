package com.snowflake.snowpark

import com.snowflake.snowpark.internal.AstNode
import com.snowflake.snowpark.proto.ast.Expr

case class Column(override private[snowpark] val ast: Expr) extends AstNode {}
