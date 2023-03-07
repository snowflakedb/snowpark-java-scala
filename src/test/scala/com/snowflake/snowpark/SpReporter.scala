package com.snowflake.snowpark

import org.scalatest.Reporter
import org.scalatest.events.{Event, SuiteAborted, TestFailed}
import java.util.HashMap;

class TestFailedException(str: String) extends Exception(str)
/*
 * Utility code for StoredProc regression tests
 */
class SPTestsReporter extends Reporter {
  val testReport = new HashMap[String, String]()
  val testExceptions = new HashMap[String, Throwable]()

  def apply(event: Event): Unit = {
    event match {
      case event: TestFailed =>
        testReport.put(event.suiteName + ":" + event.testName, event.message)
        event.throwable.map(t => {
          testExceptions.put(event.suiteName + ":" + event.testName, t)
        })
      case event: SuiteAborted =>
        testReport.put(event.suiteName, event.message)
        event.throwable.map(t => {
          testExceptions.put(event.suiteName, t)
        })
      case _ =>
    }
  }

  def getReport(): HashMap[String, String] = testReport
  def getExceptions(): HashMap[String, Throwable] = testExceptions
}

