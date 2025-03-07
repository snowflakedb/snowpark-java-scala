package com.snowflake.snowpark.internal

import scalapb.GeneratedMessage

trait AstNode {
  private[snowpark] val ast: GeneratedMessage
}
