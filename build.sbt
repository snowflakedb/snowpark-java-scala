import scala.util.Properties
import java.time.Year
import NativePackagerHelper._

lazy val isFipsRelease = {
  val result = sys.env.getOrElse("SNOWPARK_FIPS", "false").toBoolean
  // scalastyle:off println
  println(s"FIPS Build: $result")
  // scalastyle:on println
  result
}
lazy val snowparkName = s"snowpark${if (isFipsRelease) "-fips" else ""}"
lazy val jdbcName = s"snowflake-jdbc${if (isFipsRelease) "-fips" else ""}"
lazy val snowparkVersion = "1.17.0-SNAPSHOT"

lazy val Javadoc = config("genjavadoc") extend Compile

lazy val javadocSettings = inConfig(Javadoc)(Defaults.configSettings) ++ Seq(
  addCompilerPlugin(
    "com.typesafe.genjavadoc" %% "genjavadoc-plugin" % "0.19" cross CrossVersion.full),
  scalacOptions += s"-P:genjavadoc:out=${target.value}/javaDoc",
  Javadoc / apiURL := Some(
    url("https://docs.snowflake.com/developer-guide/snowpark/reference/java/index.html")),
  Javadoc / sources := (Compile / sources).value.filter(
    s => s.getName.endsWith(".java") &&
        !(s.getParent.contains("internal") || s.getParent.contains("Internal"))),
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

val jdbcVersion = "3.24.2"
val jacksonVersion = "2.18.0"
val openTelemetryVersion = "1.39.0"
val slf4jVersion = "2.0.16"

lazy val root = (project in file("."))
  .enablePlugins(BuildInfoPlugin, UniversalPlugin)
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
    // scalastyle:off
    apiURL := Some(url("https://docs.snowflake.com/developer-guide/snowpark/reference/scala/com/snowflake/snowpark/index.html")),
    // scalastyle:on
    startYear := Some(2018),
    licenses := Seq("The Apache Software License, Version 2.0" ->
      url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
    maintainer := "snowflake-java@snowflake.net",
    scmInfo := Some(ScmInfo(
      browseUrl = url("https://github.com/snowflakedb/snowpark-java-scala/tree/main"),
      connection = "scm:git:git://github.com/snowflakedb/snowpark-java-scala")),
    homepage := Some(url("https://github.com/snowflakedb/snowpark-java-scala")),
    scalaVersion := sys.props.getOrElse("SCALA_VERSION", default = "2.13.16"),
    crossScalaVersions := Seq("2.12.20", "2.13.16"),
    javaOptions ++= Seq("-source", "1.8", "-target", "1.8"),
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
      "net.snowflake" % jdbcName % jdbcVersion,
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
    Compile / doc / scalacOptions ++= Seq(
      "-doc-title", "Snowpark Scala API Reference",
      "-doc-version", snowparkVersion,
      "-doc-footer", s"© ${Year.now.getValue} Snowflake Inc. All Rights Reserved",
      "-skip-packages", "com.snowflake.snowpark_java::com.snowflake.snowpark.internal",
    ),
    Compile / packageDoc / artifact := {
      val base = (Compile / packageDoc / artifact).value
      base.withClassifier(Some("scaladoc"))
    },
    Javadoc / packageDoc / artifact := {
      val base = (Javadoc / packageDoc / artifact).value
      base.withClassifier(Some("javadoc"))
    },
    addArtifact(Javadoc / packageDoc / artifact, Javadoc / packageDoc),

    // Release settings

    // Release JAR including compiled test classes
    Test / packageBin / publishArtifact := true,
    // Also publish a test-sources JAR
    Test / packageSrc / publishArtifact := true,
    Test / packageSrc / artifact :=
      (Compile / packageSrc / artifact).value.withClassifier(Some("tests-sources")),
    addArtifact(Test / packageSrc / artifact, Test / packageSrc),

    // Fat JAR settings
    assembly / assemblyJarName :=
      s"${snowparkName}_${
        scalaVersion.value.split("\\.").take(2).mkString(".")
      }-$snowparkVersion-with-dependencies.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case _ => MergeStrategy.preferProject
    },
    assembly / assemblyOption ~= { _.withCacheOutput(false) },
    assemblyPackageScala / assembleArtifact := false, // exclude scala libraries
    assembly / assemblyExcludedJars := { // exclude snowflake jdbc from the fat jar
      val cp = (assembly / fullClasspath).value
      cp filter { _.data.getName == s"$jdbcName-$jdbcVersion.jar" }
    },
    // Release fat JAR including all dependencies except those excluded above
    assembly / artifact := {
      val base = (Compile / packageBin / artifact).value
      base.withClassifier(Some("with-dependencies"))
    },
    addArtifact(assembly / artifact, assembly),

    // Test fat JAR settings (include test classes and dependencies)
    inConfig(Test)(baseAssemblySettings),
    Test / assembly / assemblyJarName := {
      val scalaBin = scalaVersion.value.split("\\.").take(2).mkString(".")
      s"${snowparkName}_${scalaBin}-${snowparkVersion}-tests-with-dependencies.jar"
    },
    Test / assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case _ => MergeStrategy.preferProject
    },
    Test / assembly / assemblyOption ~= { _.withCacheOutput(false) },
    Test / assemblyPackageScala / assembleArtifact := false, // exclude scala libraries
    Test / assembly / assemblyExcludedJars := { // exclude snowflake jdbc from the fat jar
      val cp = (Test / assembly / fullClasspath).value
      cp filter { _.data.getName == s"$jdbcName-$jdbcVersion.jar" }
    },
    Test / assembly / fullClasspath ++= (Test / fullClasspath).value,
    // Publish the fat test JAR alongside normal artifacts
    Test / assembly / artifact := {
      val base = (Test / packageBin / artifact).value
      base.withClassifier(Some("tests-with-dependencies"))
    },
    addArtifact(Test / assembly / artifact, Test / assembly),

    // Define snowpark client bundles
    Universal / mappings ++= Seq(
      (Compile / packageBin).value -> s"$snowparkName-$snowparkVersion.jar",
      assembly.value -> s"$snowparkName-$snowparkVersion-with-dependencies.jar",
      file("preview-tarball/preload.scala") -> "preload.scala",
      file("preview-tarball/run.sh") -> "run.sh",
    ),
    Universal / mappings ++= (Compile / dependencyClasspath).value.map { f =>
      file(f.data.getPath) -> s"lib/${f.data.getName}"
    },
    Universal / mappings ++= contentOf((Compile / doc).value).map { case (f, s) =>
      f -> s"doc/scala/$s"
    },
    Universal / mappings ++= contentOf((Javadoc / doc).value).map { case (f, s) =>
      f -> s"doc/java/$s"
    },
    // Publish zip archive
    addArtifact(
      Artifact(name = snowparkName, `type` = "bundle", extension = "zip", classifier = "bundle"),
      Universal / packageBin),
    // Publish zip tarball archive
    addArtifact(
      Artifact(name = snowparkName, `type` = "bundle", extension = "tar.gz", classifier = "bundle"),
      Universal / packageZipTarball),

    credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),
    // Set up GPG key for release build from environment variable: GPG_HEX_CODE
    // Build jenkins job must have set it, otherwise, the release build will fail.
    credentials += Credentials(
      "GnuPG Key ID",
      "gpg",
      Properties.envOrNone("GPG_HEX_CODE").getOrElse("Jenkins_build_not_set_GPG_HEX_CODE"),
      "ignored" // this field is ignored; passwords are supplied by pinentry
    ),
    resolvers +=
      "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    // usePgpKeyHex(Properties.envOrElse("GPG_SIGNATURE", "12345")),
    Global / pgpPassphrase := Properties.envOrNone("GPG_KEY_PASSPHRASE").map(_.toCharArray),
    publishMavenStyle := true,
    releaseCrossBuild := true,
    releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    publishTo := Some(
      if (isSnapshot.value) {
        Opts.resolver.sonatypeOssSnapshots.head
      } else {
        Opts.resolver.sonatypeStaging
      }
    ),
    pomExtra :=
      <developers>
        <developer>
          <name>Snowflake Support Team</name>
          <email>snowflake-java@snowflake.net</email>
          <organization>Snowflake Computing</organization>
          <organizationUrl>https://www.snowflake.com</organizationUrl>
        </developer>
      </developers>
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
