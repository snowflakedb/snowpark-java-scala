import scala.util.Properties

val jacksonVersion = "2.17.2"
val openTelemetryVersion = "1.41.0"
val slf4jVersion = "2.0.4"

lazy val root = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .configs(CodeVerificationTests)
  .configs(JavaAPITests)
  .configs(OtherTests)
  .configs(OpenTelemetryTests)
  .configs(UDFTests)
  .configs(UDTFTests)
  .configs(SprocTests)
  .settings(
    name := "snowpark",
    version := "1.15.0-SNAPSHOT",
    scalaVersion := sys.props.getOrElse("SCALA_VERSION", default = "2.12.18"),
    organization := "com.snowflake",
    javaOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    licenses := Seq("The Apache Software License, Version 2.0" ->
      url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
    // Set up GPG key for release build from environment variable: GPG_HEX_CODE
    // Build jenkins job must have set it, otherwise, the release build will fail.
    credentials += Credentials(
      "GnuPG Key ID",
      "gpg",
      Properties.envOrNone("GPG_HEX_CODE").getOrElse("Jenkins_build_not_set_GPG_HEX_CODE"),
      "ignored" // this field is ignored; passwords are supplied by pinentry
    ),
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-library" % scalaVersion.value,
      "org.scala-lang" % "scala-compiler" % scalaVersion.value,
      "commons-io" % "commons-io" % "2.16.1",
      "javax.xml.bind" % "jaxb-api" % "2.3.1",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-simple" % slf4jVersion,
      "commons-codec" % "commons-codec" % "1.17.0",
      "io.opentelemetry" % "opentelemetry-api" % openTelemetryVersion,
      "net.snowflake" % "snowflake-jdbc" % "3.17.0",
      "com.github.vertical-blank" % "sql-formatter" % "1.0.2",
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      // tests
      "io.opentelemetry" % "opentelemetry-sdk" % openTelemetryVersion % Test,
      "io.opentelemetry" % "opentelemetry-exporters-inmemory" % "0.9.1" % Test,
//      "junit" % "juint" % "4.13.1" % Test,
      "com.github.sbt" % "junit-interface" % "0.13.3" % Test,
      "org.mockito" % "mockito-core" % "2.23.0" % Test,
      "org.scalatest" %% "scalatest" % "3.0.5" % Test,
    ),
    scalafmtOnCompile := true,
    javafmtOnCompile := true,
    Test / testOptions := Seq(Tests.Argument(TestFrameworks.JUnit, "-a", "-v", "-q")),
//    Test / crossPaths := false,
    Test / fork := false,
//    Test / javaOptions ++= Seq("-Xms1024M", "-Xmx4096M"),
//    Test / parallelExecution := false,
    // Test Groups
    inConfig(CodeVerificationTests)(Defaults.testTasks),
    CodeVerificationTests / testOptions += Tests.Filter(isCodeVerification),
    inConfig(JavaAPITests)(Defaults.testTasks),
    JavaAPITests / testOptions += Tests.Filter(isJavaAPITests),
    inConfig(OtherTests)(Defaults.testTasks),
    OtherTests / testOptions += Tests.Filter(isRemainingTest),
    inConfig(OpenTelemetryTests)(Defaults.testTasks),
    OpenTelemetryTests / testOptions += Tests.Filter(isOpenTelemetryTests),
    OpenTelemetryTests / parallelExecution := false,
    inConfig(UDFTests)(Defaults.testTasks),
    UDFTests / testOptions += Tests.Filter(isUDFTests),
    inConfig(UDTFTests)(Defaults.testTasks),
    UDTFTests / testOptions += Tests.Filter(isUDTFTests),
    inConfig(SprocTests)(Defaults.testTasks),
    SprocTests / testOptions += Tests.Filter(isSprocTests),
    // Build Info
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "com.snowflake.snowpark.internal",
    // Release settings
    // usePgpKeyHex(Properties.envOrElse("GPG_SIGNATURE", "12345")),
    Global / pgpPassphrase := Properties.envOrNone("GPG_KEY_PASSPHRASE").map(_.toCharArray),
    publishMavenStyle := true,
    // todo: support Scala 2.13.0
//    releaseCrossBuild := true,

    releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    pomExtra :=
      <developers>
        <developer>
          <name>Snowflake Support Team</name>
          <email>snowflake-java@snowflake.net</email>
          <organization>Snowflake Computing</organization>
          <organizationUrl>https://www.snowflake.com</organizationUrl>
        </developer>
      </developers>
      <scm>
        <connection>scm:git:git://github.com/snowflakedb/snowpark-java-scala</connection>
        <url>https://github.com/snowflakedb/snowpark-java-scala/tree/main</url>
      </scm>,

    publishTo := Some(
      if (isSnapshot.value) {
        Opts.resolver.sonatypeOssSnapshots.head
      } else {
        Opts.resolver.sonatypeStaging
      }
    )
  )

// Test Groups
// Code Verification
def isCodeVerification(name: String): Boolean = {
  name.startsWith("com.snowflake.code_verification")
}
lazy val CodeVerificationTests = config("CodeVerificationTests") extend Test

def isOpenTelemetryTests(name: String): Boolean = {
  name.contains("OpenTelemetry")
}
lazy val OpenTelemetryTests = config("OpenTelemetryTests") extend Test

def isUDFTests(name: String): Boolean = {
  name.contains("UDF")
}
lazy val UDFTests = config("UDFTests") extend Test

def isUDTFTests(name: String): Boolean = {
  name.contains("UDTF")
}
lazy val UDTFTests = config("UDTFTests") extend Test

lazy val sprocNames: Seq[String] = Seq(
  "JavaStoredProcedureSuite", "snowpark_test.StoredProcedureSuite"
)
def isSprocTests(name: String): Boolean = {
  sprocNames.exists(name.endsWith)
}
lazy val SprocTests = config("SprocTests") extend Test

// Java API Tests
def isJavaAPITests(name: String): Boolean = {
  (name.startsWith("com.snowflake.snowpark.Java") ||
    name.startsWith("com.snowflake.snowpark_test.Java")) &&
    !isUDFTests(name) &&
    !isUDTFTests(name) &&
    !isSprocTests(name)
}
lazy val JavaAPITests = config("JavaAPITests") extend Test

// FIPS Tests


// other Tests
def isRemainingTest(name: String): Boolean = {
  ! isCodeVerification(name) &&
    ! isOpenTelemetryTests(name) &&
    ! isUDFTests(name) &&
    ! isUDTFTests(name) &&
    ! isSprocTests(name) &&
    ! isJavaAPITests(name)
}
lazy val OtherTests = config("OtherTests") extend Test
