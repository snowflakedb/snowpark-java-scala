package com.snowflake.snowpark.internal

import java.io.{ByteArrayOutputStream, OutputStream}
import java.net.URI

import com.snowflake.snowpark.SnowparkClientException
import javax.lang.model.SourceVersion
import javax.tools.JavaFileManager.Location
import javax.tools.JavaFileObject.Kind
import javax.tools._

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class JavaCodeCompiler {

  val releaseVersionOption = Seq("--release", "11")

  /**
   * Compiles strings of java code and returns class bytes
   *
   * @param classSources
   *   A map of className and its code in java
   * @param classPath
   *   List of paths to include in classpath
   * @return
   *   A list of compiled classes.
   */
  def compile(
      classSources: Map[String, String],
      classPath: List[String] = List.empty): List[InMemoryClassObject] = {
    val list: Iterable[JavaFileObject] =
      classSources.transform((k, v) => new JavaSourceFromString(k, v)).values
    compile(list, classPath)
  }

  def compile(
      files: Iterable[_ <: JavaFileObject],
      classPath: List[String]): List[InMemoryClassObject] = {
    val compiler = ToolProvider.getSystemJavaCompiler
    if (compiler == null) {
      throw ErrorMessage.UDF_CANNOT_FIND_JAVA_COMPILER()
    }
    val diagnostics = new DiagnosticCollector[JavaFileObject]
    val fileManager = new InMemoryClassFilesManager(
      compiler.getStandardFileManager(null, null, null))

    var options = Seq("-classpath", classPath.mkString(System.getProperty("path.separator")))
    if (compiler.getSourceVersions.asScala.map(_.name()).contains("RELEASE_11")) {
      options = releaseVersionOption ++ options
    }

    // compile unit
    val task =
      compiler.getTask(null, fileManager, diagnostics, options.toList.asJava, null, files.asJava)
    // compile
    val success = task.call
    if (success) {
      fileManager.outputFiles.toList
    } else {
      val errors = diagnostics.getDiagnostics.asScala.map { _.toString }
      throw ErrorMessage.UDF_ERROR_IN_COMPILING_CODE(errors.mkString("; "))
    }
  }
}

/**
 * A class that represents a Java source file generated from a string. This is mostly boilerplate
 * for JavaCompiler API
 *
 * @param className
 *   Name of the class
 * @param code
 *   String representation of the class code
 */
class JavaSourceFromString(className: String, code: String)
    extends SimpleJavaFileObject(
      URI.create("string:///" + className.replace(".", "/") + Kind.SOURCE.extension),
      Kind.SOURCE) {
  override def getCharContent(ignoreEncodingErrors: Boolean): CharSequence = code
}

/**
 * A class that represents a compiled class stored in memory. This is mostly boilerplate for
 * JavaCompiler API
 *
 * @param className
 *   Name of class
 * @param kind
 *   of file like .class
 */
class InMemoryClassObject(className: String, kind: Kind)
    extends SimpleJavaFileObject(
      URI.create("mem:///" + className.replace('.', '/') + kind.extension),
      kind) {

  def getClassName: String = className

  private val outputStream = new ByteArrayOutputStream()

  override def openOutputStream(): OutputStream = outputStream

  def getBytes(): Array[Byte] = {
    try {
      outputStream.toByteArray()
    } finally {
      outputStream.close()
    }
  }
}

/**
 * A handler for managing output generated by the compiler task. This is mostly boilerplate for
 * JavaCompiler API
 */
class InMemoryClassFilesManager(fileManager: JavaFileManager)
    extends ForwardingJavaFileManager[JavaFileManager](fileManager) {

  val outputFiles = new ArrayBuffer[InMemoryClassObject]

  override def getJavaFileForOutput(
      location: Location,
      className: String,
      kind: Kind,
      sibling: FileObject): JavaFileObject = {
    val file = new InMemoryClassObject(className, kind)
    outputFiles += file
    file
  }
}
