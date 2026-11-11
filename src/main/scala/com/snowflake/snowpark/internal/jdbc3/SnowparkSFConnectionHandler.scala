package com.snowflake.snowpark.internal.jdbc3

import com.snowflake.snowpark.internal.Utils
import net.snowflake.client.jdbc.internal.snowflake.common.core.LoginInfoDTO
import net.snowflake.client.jdbc.{DefaultSFConnectionHandler, SnowflakeConnectString}

import java.util.Properties

private[snowpark] class SnowparkSFConnectionHandler(conStr: SnowflakeConnectString)
    extends DefaultSFConnectionHandler(conStr) {

  override def initializeConnection(url: String, info: Properties): Unit = {
    val connStr = SnowflakeConnectString.parse(url, info)
    super.initialize(
      connStr,
      LoginInfoDTO.SF_SNOWPARK_APP_ID,
      Utils.extractValidVersionNumber(Utils.Version))
  }
}
