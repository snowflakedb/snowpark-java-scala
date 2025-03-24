import scala.reflect.runtime.{universe => ru}
import scala.tools.reflect.ToolBox

val snowparkName = s"snowpark"

val commonSettings = Seq(
  version := "2.0.0-SNAPSHOT",
  scalaVersion := "2.13.16",
  crossScalaVersions := Seq("2.12.18", "2.13.16"),
  libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-library" % scalaVersion.value,
    "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  )
)

val jacksonVersion = "2.18.2"
val jdbcVersion = "3.23.0"
val sl4jVersion = "2.0.17"

lazy val macros = (project in file("macros"))
  .settings(
    name := s"${snowparkName}-macros",
    commonSettings,
  )

lazy val root = (project in file("."))
  .dependsOn(macros)
  .enablePlugins(BuildInfoPlugin)
  .aggregate(macros)
  .settings(
    name := snowparkName,
    commonSettings,
    Compile / PB.targets := Seq(scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"),
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion %
        "protobuf",
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
      "commons-codec" % "commons-codec" % "1.18.0",
      "net.snowflake" % "snowflake-jdbc" % jdbcVersion,
      "org.slf4j" % "slf4j-api" % sl4jVersion,
      "org.slf4j" % "slf4j-simple" % sl4jVersion,
      // Tests
      "com.github.sbt" % "junit-interface" % "0.13.3" % Test,
      "org.mockito" % "mockito-core" % "2.23.0" % Test,
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      ),
    // Build Info
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "com.snowflake.snowpark.internal",
    coverageEnabled := true,
    scalafmtOnCompile := true,
    javafmtOnCompile := true,

    Compile / sourceGenerators += generateSourcesTask.taskValue,
    Compile / managedSourceDirectories += (Compile / sourceManaged).value
  )

lazy val generateSourcesTask = Def.task {
  val outputDir = (Compile / sourceManaged).value / "generated"
  val outputFile = outputDir / "GeneratedFunctions.scala"
  IO.createDirectory(outputDir)

  val templatesDir = baseDirectory.value / "src/main/scala/com/snowflake/snowpark/code_templates"
  val templateFiles = templatesDir.listFiles().filter(_.getName.endsWith(".txt")).toList

  val mirror = ru.runtimeMirror(getClass.getClassLoader)
  val toolbox = mirror.mkToolBox()

  val generatedBodies = templateFiles.map { file =>
    val code = IO.read(file)
    val wrappedCode = s"{$code}"
    try {
      val tree = toolbox.parse(wrappedCode)
      val evaluated = toolbox.eval(tree)
      evaluated.toString
    } catch {
      case e: Throwable =>
        sys.error(s"Failed to evaluate template ${file.getName}: ${e.getMessage}")
    }
  }

  val traitHeader =
    """|package com.snowflake.generated
       |
       |import com.snowflake.snowpark._
       |
       |trait GeneratedFunctions {
       |""".stripMargin

  val traitFooter = "\n}"

  val finalCode = traitHeader + generatedBodies.mkString("\n") + traitFooter

  IO.write(outputFile, finalCode)
  Seq(outputFile)
}
