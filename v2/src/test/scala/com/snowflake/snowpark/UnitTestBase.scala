package com.snowflake.snowpark

import com.snowflake.snowpark.internal.{AstUtils, StmtNode}
import scalapb.GeneratedMessage

trait UnitTestBase extends CommonTestBase {

  implicit val mockedSession: Session = new Session()

  def checkAst(expected: GeneratedMessage, actual: StmtNode)(implicit session: Session): Unit = {}
}
