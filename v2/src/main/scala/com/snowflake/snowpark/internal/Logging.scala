package com.snowflake.snowpark.internal

import org.slf4j.{Logger, LoggerFactory}

trait Logging {

  lazy private val logName: String = {
    val name = this.getClass.getName
    if (name.endsWith("$")) name.substring(0, name.length - 1)
    else name
  }

  lazy protected val logger: Logger = LoggerFactory.getLogger(logName)
}
