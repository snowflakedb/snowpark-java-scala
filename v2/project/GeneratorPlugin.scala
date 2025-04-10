import sbt._
import sbt.Keys._
import scala.reflect.runtime.{universe => ru}
import scala.tools.reflect.ToolBox

object GeneratorPlugin extends AutoPlugin {
  override def trigger: PluginTrigger = allRequirements

  object autoImport {
    val generateSourcesTask = taskKey[Seq[File]]("Generates source files from templates")
  }
  import autoImport._

  override def projectSettings: Seq[Setting[_]] = Seq(
    generateSourcesTask := {
      val outputDir: File = (Compile / sourceManaged).value / "generated"
      IO.createDirectory(outputDir)

      val templatesDir: File = baseDirectory.value / "src/main/generated/code_templates"
      val templateFiles: List[File] = Option(templatesDir.listFiles())
        .getOrElse(Array.empty)
        .filter(_.getName.endsWith(".scala"))
        .toList

      val mirror = ru.runtimeMirror(getClass.getClassLoader)
      val toolbox = mirror.mkToolBox()

      def fileToClassName(file: File): String = {
        val base = file.getName.stripSuffix(".scala")
        val cleaned = base.replaceAll("[^a-zA-Z0-9]", "_")
        cleaned.head.toUpper + cleaned.tail
      }

      val outputFiles: List[File] = templateFiles.map { file =>
        val className = fileToClassName(file)
        val outputFile: File = outputDir / s"$className.scala"

        val code: String = IO.read(file).trim

        val generatedCode: String = try {
          val tree = toolbox.parse(s"{$code}")
          val result = toolbox.eval(tree)
          result match {
            case s: String => s
            case other => sys.error(s"Template ${file.getName} is not a String")
          }
        } catch {
          case e: Throwable =>
            sys.error(s"Error in evaluating ${file.getName}: ${e.getMessage}")
        }

        val wrappedOutput: String = {
          s"""|package com.snowflake.snowpark.generated
              |
              |import com.snowflake.snowpark._
              |
              |class $className {
              |  $generatedCode
              |}
              |""".stripMargin
        }
        IO.write(outputFile, wrappedOutput)
        outputFile
      }
      outputFiles
    },
    Compile / sourceGenerators += generateSourcesTask.taskValue
  )
}