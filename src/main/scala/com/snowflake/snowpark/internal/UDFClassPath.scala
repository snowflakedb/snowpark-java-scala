package com.snowflake.snowpark.internal

import com.fasterxml.jackson.annotation.JsonView
import com.fasterxml.jackson.core.TreeNode
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.io.File
import java.net.{URI, URLClassLoader}
import com.snowflake.snowpark.Session

object UDFClassPath extends Logging {

  case class RequiredLibrary(location: Option[String], description: String, clazz: Class[_])

  /*
   * Snowpark jar has to be included in UDF imports because it is not available on XP.
   *
   * Scala jars are packaged in XP tarball so we don't have to include that in UDF imports.
   */

  val snowparkClass = classOf[com.snowflake.snowpark.Session]
  val snowparkJar = RequiredLibrary(getPathForClass(snowparkClass), "snowpark", snowparkClass)

  val jacksonDatabindClass: Class[JsonNode] = classOf[com.fasterxml.jackson.databind.JsonNode]
  val jacksonCoreClass: Class[TreeNode] = classOf[com.fasterxml.jackson.core.TreeNode]
  val jacksonAnnotationClass: Class[JsonView] = classOf[com.fasterxml.jackson.annotation.JsonView]
  val jacksonModuleScalaClass: Class[DefaultScalaModule] =
    classOf[com.fasterxml.jackson.module.scala.DefaultScalaModule]
  val jacksonJarSeq = Seq(
    RequiredLibrary(
      getPathForClass(jacksonDatabindClass),
      "jackson-databind",
      jacksonDatabindClass),
    RequiredLibrary(getPathForClass(jacksonCoreClass), "jackson-core", jacksonCoreClass),
    RequiredLibrary(
      getPathForClass(jacksonAnnotationClass),
      "jackson-annotation",
      jacksonAnnotationClass),
    RequiredLibrary(
      getPathForClass(jacksonModuleScalaClass),
      "jackson-module-scala",
      jacksonModuleScalaClass))

  /*
   * Libraries required to compile java code generated by snowpark for user's lambda.
   */
  val classpath = Seq((classOf[scala.Product], "Scala ")).map { case (c, description) =>
    RequiredLibrary(getPathForClass(c), description, c)
  } ++ Seq(snowparkJar)

  def classDirs(session: Session): collection.Set[File] = {
    val allURIs = session.getLocalFileDependencies
    allURIs.map(new File(_)).filter(_.isDirectory)
  }

  /*
   * Creates a URLClassLoader with the given list of URIs and checks
   * if the class is defined.
   */
  def isClassDefined(uris: Seq[URI], className: String): Boolean = {
    val checkClassLoader = new URLClassLoader(uris.map(_.toURL).toArray, null) {
      def checkClass(name: String): Class[_] = {
        findClass(name)
      }
    }
    try {
      checkClassLoader.checkClass(className)
      true
    } catch {
      case e: ClassNotFoundException => false
    }
  }

  def getPathForClass(clazz: Class[_]): Option[String] = {
    // This method works for repls
    val path = getPathUsingClassLoader(clazz)
    if (path.isDefined) {
      path
    } else {
      // If the above method does not work,
      // try to figure out path using code source
      getPathUsingCodeSource(clazz)
    }
  }

  def getPathUsingClassLoader(clazz: Class[_]): Option[String] = {
    val classFileName = s"/${clazz.getName().replace('.', '/')}.class"
    /*
     * Path for a class is returned in this format:
     * file:/home/kbhatia/scala-2.12.12/lib/scala-library.jar!/scala/collection/immutable/List.class
     * So we strip the content before ":" and after "!"
     */
    Option(clazz.getResource(classFileName)).map { url =>
      var path = url.getPath.replaceAll(classFileName, "")
      if (path.contains("!")) {
        path = path.substring(0, path.indexOf("!"))
      }
      if (path.contains(":")) {
        path = path.substring(path.indexOf(":") + 1)
      }
      new URI(path).getPath
    }
  }

  def getPathUsingCodeSource(clazz: Class[_]): Option[String] = {
    val codeSource = clazz.getProtectionDomain.getCodeSource
    val path = if (codeSource != null && codeSource.getLocation != null) {
      /*
       * The URL in CodeSource location is encoded, so we have to decode it to read the
       * local file.
       */
      Some(new URI(codeSource.getLocation.getPath).getPath)
    } else {
      None
    }
    path
  }
}
