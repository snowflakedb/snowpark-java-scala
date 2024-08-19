package com.snowflake.snowpark.internal

import net.snowflake.client.core.SFSessionProperty

import java.security.spec.{PKCS8EncodedKeySpec, RSAPrivateCrtKeySpec}
import java.security.{GeneralSecurityException, KeyFactory, PrivateKey}
import java.util.Properties
import org.apache.commons.codec.binary.Base64
import sun.security.util.DerInputStream

private[snowpark] object ParameterUtils extends Logging {

  private var snowparkSpecificParameters: Set[String] = Set.empty

  private[snowpark] val Url: String = registerParameter("url")
  // convert private key string to private key object before send to jdbc
  private[snowpark] val PrivateKey: String = registerParameter("privatekey")

  // client parameters
  private[snowpark] val SnowparkLazyAnalysis: String = "snowpark_lazy_analysis"
  private[snowpark] val GeographyOutputFormat: String = "geography_output_format"
  private[snowpark] val GeometryOutputFormat: String = "geometry_output_format"
  private[snowpark] val SnowparkUseScopedTempObjects: String = "snowpark_use_scoped_temp_objects"
  private[snowpark] val SnowparkEnableClosureCleaner: String = "snowpark_enable_closure_cleaner"
  private[snowpark] val SnowparkRequestTimeoutInSeconds: String =
    "snowpark_request_timeout_in_seconds"
  private[snowpark] val SnowparkMaxFileUploadRetryCount: String =
    registerParameter("snowpark_max_file_upload_retry_count")
  private[snowpark] val SnowparkMaxFileDownloadRetryCount: String =
    registerParameter("snowpark_max_file_download_retry_count")
  private[snowpark] val SnowparkHideInternalAlias: String = "snowpark_hide_internal_alias"

  // client parameter values
  @inline private[snowpark] val DEFAULT_REQUEST_TIMEOUT_IN_SECONDS: String = "86400" // 24 hours
  @inline private[snowpark] val MAX_REQUEST_TIMEOUT_IN_SECONDS: Int = 604800 // 7 days
  @inline private[snowpark] val MIN_REQUEST_TIMEOUT_IN_SECONDS: Int = 0
  @inline private[snowpark] val DEFAULT_MAX_FILE_UPLOAD_RETRY_COUNT: String = "5"
  @inline private[snowpark] val DEFAULT_MAX_FILE_DOWNLOAD_RETRY_COUNT: String = "5"
  @inline private[snowpark] val DEFAULT_SNOWPARK_USE_SCOPED_TEMP_OBJECTS: String = "false"
  @inline private[snowpark] val DEFAULT_SNOWPARK_HIDE_INTERNAL_ALIAS: String = "true"

  // parameter options
  object ClosureCleanerMode extends Enumeration {
    type ClosureCleanerMode = Value
    val always, never, repl_only, unsupported = Value
    def withNameWithDefault(name: String): Value =
      values.find(_.toString.toLowerCase() == name.toLowerCase()).getOrElse(unsupported)
  }

  def jdbcConfig(options: Map[String, String], isScalaAPI: Boolean): Properties = {
    val config: Properties = new Properties()
    val forwardNameSet: Set[String] = options.keySet -- snowparkSpecificParameters

    val client_memory_limit = "client_memory_limit"

    // Set JDBC memory to 10G by default, it can be override by user config
    config.put(client_memory_limit, "10240")

    options.foreach { case (key, value) =>
      if (forwardNameSet.contains(key)) {
        // directly forward to JDBC
        config.put(key, value)
      } else if (key == PrivateKey) { // parse private key
        config.put(PrivateKey, parsePrivateKey(value))
      }
    }
    /*
     * Add this config so that the JDBC connector validates the user-provided
     * options when intializing the connection.
     */
    config.put("CLIENT_VALIDATE_DEFAULT_PARAMETERS", true.asInstanceOf[Object])

    // Turn on session heart beat
    config.put("CLIENT_SESSION_KEEP_ALIVE", true.asInstanceOf[Object])

    config.put(
      SFSessionProperty.CLIENT_INFO.getPropertyKey,
      s"""{"client_language": "${if (isScalaAPI) "Scala" else "Java"}"}""".stripMargin
    )

    // log JDBC memory limit
    logInfo(s"set JDBC client memory limit to ${config.get(client_memory_limit).toString}")
    config
  }

  private[internal] def parseBoolean(value: String): Boolean = {
    // scalastyle:off
    val lowerCase = value.trim.toLowerCase
    // scalastyle:on
    lowerCase match {
      case "true" | "on" | "yes" => true
      case _                     => false
    }
  }

  private[internal] def parseClosureCleanerParam(value: String): ClosureCleanerMode.Value = {
    // scalastyle:off
    val lowerCase = value.trim.toLowerCase
    // scalastyle:on
    val mode = ClosureCleanerMode.withNameWithDefault(value)
    if (mode == ClosureCleanerMode.unsupported) {
      throw ErrorMessage.MISC_INVALID_CLOSURE_CLEANER_PARAMETER(SnowparkEnableClosureCleaner)
    }
    mode
  }

  private[snowpark] def parsePrivateKey(key: String): PrivateKey = {
    // try to parse pkcs#8 format first,
    // if it fails, then try to parse pkcs#1 format.
    try {
      val decoded = Base64.decodeBase64(key)
      val kf = KeyFactory.getInstance("RSA")
      val keySpec = new PKCS8EncodedKeySpec(decoded)
      kf.generatePrivate(keySpec)
    } catch {
      case pkcs8Exception: Exception =>
        // try to read PKCS#1 key
        try {
          val decoded = Base64.decodeBase64(key)
          val derReader = new DerInputStream(decoded)
          val seq = derReader.getSequence(0)

          if (seq.length < 9) {
            throw new GeneralSecurityException("Could not parse a PKCS1 private key.")
          }

          // seq(0) is version, skip
          val modulus = seq(1).getBigInteger
          val publicExp = seq(2).getBigInteger
          val privateExp = seq(3).getBigInteger
          val prime1 = seq(4).getBigInteger
          val prime2 = seq(5).getBigInteger
          val exp1 = seq(6).getBigInteger
          val exp2 = seq(7).getBigInteger
          val crtCoef = seq(8).getBigInteger
          val keySpec = new RSAPrivateCrtKeySpec(
            modulus,
            publicExp,
            privateExp,
            prime1,
            prime2,
            exp1,
            exp2,
            crtCoef
          )
          val keyFactory = KeyFactory.getInstance("RSA")
          keyFactory.generatePrivate(keySpec)
        } catch {
          case pkcs1Exception: Exception =>
            val errorMessage =
              s"""Failed to parse PKCS#8 RSA Private key
                |${pkcs8Exception.getMessage}
                |Failed to parse PKCS#1 RSA Private key
                |${pkcs1Exception.getMessage}
                |""".stripMargin
            throw ErrorMessage.MISC_INVALID_RSA_PRIVATE_KEY(errorMessage)
        }
    }
  }

  private def registerParameter(name: String): String = {
    // internal check
    require(!snowparkSpecificParameters.contains(name), s"duplicated parameter: $name")
    snowparkSpecificParameters = snowparkSpecificParameters + name
    name
  }
}
