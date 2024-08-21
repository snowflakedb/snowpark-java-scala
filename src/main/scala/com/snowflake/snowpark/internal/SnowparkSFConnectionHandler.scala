package com.snowflake.snowpark.internal

import com.snowflake.snowpark.internal.SnowparkSFConnectionHandler.extractValidVersionNumber
import net.snowflake.client.jdbc.internal.snowflake.common.core.LoginInfoDTO
import net.snowflake.client.jdbc.{DefaultSFConnectionHandler, SnowflakeConnectString}

import java.util.Properties

object SnowparkSFConnectionHandler {
  // Version format copied from GS ClientVersionUtils.java
  private val VERSION_FORMAT_PATTERN = "[0-9]+\\.[0-9]+\\.[0-9]+(?:\\.[0-9]+)?".r

  def extractValidVersionNumber(version: String): String = {
    VERSION_FORMAT_PATTERN
      .findFirstIn(version)
      .getOrElse(throw ErrorMessage.MISC_INVALID_CLIENT_VERSION(version))
  }
}

class SnowparkSFConnectionHandler(conStr: SnowflakeConnectString)
    extends DefaultSFConnectionHandler(conStr) {

  override def initializeConnection(url: String, info: Properties): Unit = {
    val connStr = SnowflakeConnectString.parse(url, info)
    super.initialize(
      connStr,
      LoginInfoDTO.SF_SNOWPARK_APP_ID,
      extractValidVersionNumber(Utils.Version))
  }
}
