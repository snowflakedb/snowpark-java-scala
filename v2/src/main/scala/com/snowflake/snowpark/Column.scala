package com.snowflake.snowpark

import com.snowflake.snowpark.internal.AstFunc
import com.snowflake.snowpark.proto.ast.Expr

case class Column(override private[snowpark] val ast: Expr) extends AstFunc{}
