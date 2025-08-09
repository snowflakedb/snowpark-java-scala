import scala.util.Properties
import java.net.URI
import java.time.Year

lazy val isFipsRelease = {
  val result = sys.env.get("SNOWPARK_FIPS").getOrElse("false").toBoolean
  println(s"FIPS Build: $result")
  result
}
lazy val snowparkName = s"snowpark${if(isFipsRelease) "-fips" else ""}"
lazy val jdbcName = s"snowflake-jdbc${if(isFipsRelease) "-fips" else ""}"
lazy val snowparkVersion = "1.17.0-SNAPSHOT"

lazy val Javadoc = config("genjavadoc") extend Compile

lazy val javadocSettings = inConfig(Javadoc)(Defaults.configSettings) ++ Seq(
  addCompilerPlugin("com.typesafe.genjavadoc" %% "genjavadoc-plugin" % "0.19" cross CrossVersion.full),
  scalacOptions += s"-P:genjavadoc:out=${target.value}/javaDoc",
  Compile / packageDoc := (Javadoc / packageDoc).value,
  Javadoc / apiURL := Some(url("https://docs.snowflake.com/developer-guide/snowpark/reference/java/index.html")),
  Javadoc / sources := (Compile / sources).value.filter(s => (s.getName.endsWith(".java") && !(s.getParent.contains("internal") || s.getParent.contains("Internal")))),
  Javadoc / javacOptions := Seq(
    "--allow-script-in-comments",
    "-windowtitle", s"Snowpark Java API Reference $snowparkVersion",
    "-doctitle", s"Snowpark Java API Reference $snowparkVersion",
    "-header", s"""<div style="margin-top: 14px"><strong>
                  |  Snowpark Java API Reference $snowparkVersion <br/>
                  |  <a style="text-transform: none" href="https://docs.snowflake.com/en/developer-guide/snowpark/java/index.html" target="_top">[Snowpark Developer Guide for Java]</a>
                  |</strong></div>""".stripMargin,
    "-bottom", s"""&#169; ${Year.now.getValue} Snowflake Inc. All Rights Reserved
                   |<!-- Google Analytics Code -->
                   |<script>
                   |  (function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){
                   |  (i[r].q=i[r].q||[]).push(arguments)},i[r].l=1*new Date();a=s.createElement(o),
                   |  m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m)
                   |  })(window,document,'script','//www.google-analytics.com/analytics.js','ga');
                   |
                   |  ga('create', 'UA-48785629-1', 'auto');
                   |  ga('send', 'pageview');
                   |</script>
                   |
                   |<!-- Global site tag (gtag.js) - Google Analytics -->
                   |<script async src="https://www.googletagmanager.com/gtag/js?id=G-00K70YK8HQ"></script>
                   |<script>
                   |  window.dataLayer = window.dataLayer || [];
                   |  function gtag(){dataLayer.push(arguments);}
                   |  gtag('js', new Date());
                   |  gtag('config', 'G-00K70YK8HQ');
                   |</script>
                   |
                   |<script>
                   |if (typeof useModuleDirectories !== 'undefined') {
                   |  useModuleDirectories = false;
                   |}
                   |</script>""".stripMargin),
  Javadoc / packageDoc / artifactName := ((sv, mod, art) =>
    "" + mod.name + "_" + sv.binary + "-" + mod.revision + "-javadoc.jar"),
)

val jacksonVersion = "2.18.0"
val openTelemetryVersion = "1.39.0"
val slf4jVersion = "2.0.16"

lazy val root = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .configs(Javadoc).settings(javadocSettings: _*)
  .configs(CodeVerificationTests)
  .configs(JavaAPITests)
  .configs(OtherTests)
  .configs(NonparallelTests)
  .configs(UDFTests)
  .configs(UDTFTests)
  .configs(SprocTests)
  .settings(
    organization := "com.snowflake",
    organizationName := "Snowflake Computing",
    organizationHomepage := Some(url("https://www.snowflake.com/")),
    name := snowparkName,
    version := snowparkVersion,
    description := "Snowflake's DataFrame API",
    apiURL := Some(url("https://docs.snowflake.com/developer-guide/snowpark/reference/scala/com/snowflake/snowpark/index.html")),
    startYear := Some(2018),
    licenses := Seq("The Apache Software License, Version 2.0" ->
      url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
    developers := List(
      Developer(id="Snowflake Support Team", name="Snowflake Computing", email="snowflake-java@snowflake.net", url=url("http://www.snowflake.com/")),
    ),
    scmInfo := Some(ScmInfo(browseUrl=url("https://github.com/snowflakedb/snowpark-java-scala/tree/main"), connection="scm:git:git://github.com/snowflakedb/snowpark-java-scala")),
    homepage := Some(url("https://github.com/snowflakedb/snowpark-java-scala")),
    scalaVersion := sys.props.getOrElse("SCALA_VERSION", default = "2.13.16"),
    crossScalaVersions := Seq("2.12.20", "2.13.16"),
    javaOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    // Set up GPG key for release build from environment variable: GPG_HEX_CODE
    // Build jenkins job must have set it, otherwise, the release build will fail.
    credentials += Credentials(
      "GnuPG Key ID",
      "gpg",
      Properties.envOrNone("GPG_HEX_CODE").getOrElse("Jenkins_build_not_set_GPG_HEX_CODE"),
      "ignored" // this field is ignored; passwords are supplied by pinentry
    ),
    libraryDependencies ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      "com.github.vertical-blank" % "sql-formatter" % "1.0.2",
      "commons-codec" % "commons-codec" % "1.15",
      "commons-io" % "commons-io" % "2.14.0",
      "io.opentelemetry" % "opentelemetry-api" % openTelemetryVersion,
      "javax.xml.bind" % "jaxb-api" % "2.3.1",
      "net.snowflake" % jdbcName % "3.24.2",
      "org.scala-lang" % "scala-library" % scalaVersion.value,
      "org.scala-lang" % "scala-compiler" % scalaVersion.value,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-simple" % slf4jVersion,
      // tests
      "io.opentelemetry" % "opentelemetry-sdk" % openTelemetryVersion % Test,
      "io.opentelemetry" % "opentelemetry-exporters-inmemory" % "0.9.1" % Test,
//      "junit" % "juint" % "4.13.1" % Test,
      "com.github.sbt" % "junit-interface" % "0.13.3" % Test,
      "org.mockito" % "mockito-core" % "2.23.0" % Test,
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
    ),
    scalafmtOnCompile := true,
    javafmtOnCompile := true,
    Test / testOptions := Seq(Tests.Argument(TestFrameworks.JUnit, "-a", "-v", "-q")),
//    Test / crossPaths := false,
    Test / fork := false,
//    Test / javaOptions ++= Seq("-Xms1024M", "-Xmx4096M"),
    // Test Groups
    inConfig(CodeVerificationTests)(Defaults.testTasks),
    CodeVerificationTests / testOptions += Tests.Filter(isCodeVerification),
    inConfig(JavaAPITests)(Defaults.testTasks),
    JavaAPITests / testOptions += Tests.Filter(isJavaAPITests),
    JavaAPITests / parallelExecution := false,
    inConfig(OtherTests)(Defaults.testTasks),
    OtherTests / testOptions += Tests.Filter(isRemainingTest),
    inConfig(NonparallelTests)(Defaults.testTasks),
    NonparallelTests / testOptions += Tests.Filter(isNonparallelTests),
    NonparallelTests / parallelExecution := false,
    inConfig(UDFTests)(Defaults.testTasks),
    UDFTests / testOptions += Tests.Filter(isUDFTests),
    inConfig(UDTFTests)(Defaults.testTasks),
    UDTFTests / testOptions += Tests.Filter(isUDTFTests),
    inConfig(SprocTests)(Defaults.testTasks),
    SprocTests / testOptions += Tests.Filter(isSprocTests),
    // Build Info
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "com.snowflake.snowpark.internal",
    // doc settings
    Compile / doc / scalacOptions ++= Seq("-skip-packages", "com.snowflake.snowpark_java::com.snowflake.snowpark.internal"),
    // Release settings
    // usePgpKeyHex(Properties.envOrElse("GPG_SIGNATURE", "12345")),
    Global / pgpPassphrase := Properties.envOrNone("GPG_KEY_PASSPHRASE").map(_.toCharArray),
    publishMavenStyle := true,
    // todo: support Scala 2.13.0
//    releaseCrossBuild := true,
    releasePublishArtifactsAction := PgpKeys.publishSigned.value,
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

lazy val nonParallelTestsList = Seq(
  "OpenTelemetry",
  "AsyncJob"
)
// Tests can't be parallely processed
def isNonparallelTests(name: String): Boolean = {
  nonParallelTestsList.exists(name.contains)
}
lazy val NonparallelTests = config("NonparallelTests") extend Test

def isUDFTests(name: String): Boolean = {
  name.contains("UDF")
}
lazy val UDFTests = config("UDFTests") extend Test

def isUDTFTests(name: String): Boolean = {
  name.contains("UDTF")
}
lazy val UDTFTests = config("UDTFTests") extend Test

lazy val sprocNames: Seq[String] = Seq(
  "JavaStoredProcedureSuite",
  "snowpark_test.StoredProcedureSuite",
  "JavaSProcNonStoredProcSuite"
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
    !isSprocTests(name) &&
    !isNonparallelTests(name)
}
lazy val JavaAPITests = config("JavaAPITests") extend Test

// FIPS Tests


// other Tests
def isRemainingTest(name: String): Boolean = {
  ! isCodeVerification(name) &&
    ! isNonparallelTests(name) &&
    ! isUDFTests(name) &&
    ! isUDTFTests(name) &&
    ! isSprocTests(name) &&
    ! isJavaAPITests(name)
}
lazy val OtherTests = config("OtherTests") extend Test
