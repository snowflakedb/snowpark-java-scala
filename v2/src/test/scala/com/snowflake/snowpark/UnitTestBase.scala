package com.snowflake.snowpark

import com.snowflake.snowpark.internal.StmtNode
import scalapb.GeneratedMessage

trait UnitTestBase extends CommonTestBase {

  implicit val mockedSession: Session = new Session()

  def checkAst(expected: GeneratedMessage, actual: StmtNode)(implicit session: Session): Unit = {}
}
