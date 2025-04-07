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
  IO.createDirectory(outputDir)

  val templatesDir = baseDirectory.value / "src/main/generated/code_templates"
  val templateFiles = templatesDir.listFiles().filter(_.getName.endsWith(".scala")).toList

  val mirror = ru.runtimeMirror(getClass.getClassLoader)
  val toolbox = mirror.mkToolBox()

  def fileToClassName(file: File): String = {
    val base = file.getName.stripSuffix(".scala")
    val cleaned = base.replaceAll("[^a-zA-Z0-9]", "_")
    cleaned.head.toUpper + cleaned.tail
  }

  val outputFiles = templateFiles.map { file =>
    val baseName = file.getName.stripSuffix(".scala")
    val className = fileToClassName(file)
    val outputFile = outputDir / s"$className.scala"

    val code = IO.read(file).trim

    val generatedCode: String = try {
      val tree = toolbox.parse(s"{$code}")
      val result = toolbox.eval(tree)
      result match {
        case s: String => s
        case other =>
          sys.error(s"Template ${file.getName}" +
            s"should return a String. Got: ${other.getClass.getSimpleName}")
      }
    } catch {
      case e: Throwable =>
        sys.error(s"Error in evaluating template ${file.getName}: ${e.getMessage}")
    }

    val wrappedOutput =
      s"""|package com.snowflake.snowpark.generated
          |
          |import com.snowflake.snowpark._
          |
          |class $className {
          |$generatedCode
          |}
          |""".stripMargin

    IO.write(outputFile, wrappedOutput)
    outputFile
  }

  outputFiles
}