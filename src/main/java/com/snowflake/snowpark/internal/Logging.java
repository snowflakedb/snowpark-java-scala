package com.snowflake.snowpark.internal;

import java.io.PrintWriter;
import java.io.StringWriter;
import net.snowflake.client.util.SecretDetector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Logging {
  private Logger curLog = null;

  protected final String logName = getClassName();

  private String getClassName() {
    String name = this.getClass().getName();
    if (name.endsWith("$")) {
      return name.substring(0, name.length() - 1);
    }
    return name;
  }

  protected Logger log() {
    // lazy init
    if (curLog == null) {
      curLog = LoggerFactory.getLogger(logName);
    }
    return curLog;
  }

  protected void logInfo(String msg) {
    if (log().isInfoEnabled()) {
      log().info(maskSecrets(msg));
    }
  }

  protected void logDebug(String msg) {
    if (log().isDebugEnabled()) {
      log().debug(maskSecrets(msg));
    }
  }

  protected void logTrace(String msg) {
    if (log().isTraceEnabled()) {
      log().trace(maskSecrets(msg));
    }
  }

  protected void logWarning(String msg) {
    if (log().isWarnEnabled()) {
      log().warn(maskSecrets(msg));
    }
  }

  protected void logError(String msg) {
    if (log().isErrorEnabled()) {
      log().error(maskSecrets(msg));
    }
  }

  protected void logInfo(String msg, Throwable throwable) {
    if (log().isInfoEnabled()) {
      log().info(maskSecrets(msg, throwable));
    }
  }

  protected void logDebug(String msg, Throwable throwable) {
    if (log().isDebugEnabled()) {
      log().debug(maskSecrets(msg, throwable));
    }
  }

  protected void logTrace(String msg, Throwable throwable) {
    if (log().isTraceEnabled()) {
      log().trace(maskSecrets(msg, throwable));
    }
  }

  protected void logWarning(String msg, Throwable throwable) {
    if (log().isWarnEnabled()) {
      log().warn(maskSecrets(msg, throwable));
    }
  }

  protected void logError(String msg, Throwable throwable) {
    if (log().isErrorEnabled()) {
      log().error(maskSecrets(msg, throwable));
    }
  }

  public static String maskSecrets(String msg) {
    if (msg != null) {
      return SecretDetector.maskSecrets(msg);
    }
    return null;
  }

  public static String maskSecrets(String msg, Throwable throwable) {
    StringWriter writer = new StringWriter();
    throwable.printStackTrace(new PrintWriter(writer));
    return maskSecrets(msg) + "\n" + maskSecrets(writer.toString());
  }
}
