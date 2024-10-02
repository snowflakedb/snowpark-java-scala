package com.snowflake.snowpark

import java.io.{File, FileOutputStream}
import java.util.jar.{JarFile, JarOutputStream}
import java.util.zip.ZipException
import com.snowflake.snowpark.internal.{FatJarBuilder, JavaCodeCompiler}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class FatJarBuilderSuite extends AnyFunSuite with FileUtils {

  test("Check that fat jar is built correctly") {
    val className = "HelloWorld"
    val dummyCode =
      s"""
         | public class $className {
         |   public static void test() {
         |   }
         | }
         |""".stripMargin

    val compiledClasses = (new JavaCodeCompiler).compile(Map(className -> dummyCode))
    val jarBuilder = new FatJarBuilder
    val tempFile = TestUtils.tempDirWithEscape + "output" + Random.nextInt() + ".jar"
    val jarStream = new JarOutputStream(new FileOutputStream(tempFile))
    jarBuilder.createFatJar(compiledClasses, getClassesDirs, getJars, Map.empty, jarStream)
    // Write File
    jarStream.close()

    // check file contents

    val classesInJar = listClassesInJar(tempFile)
    // Check that the compiled in-memory class is included
    assert(classesInJar.contains("HelloWorld.class"))
    // Check that jars are included
    assert(classesInJar.contains("scala/Function1.class"))
    // Check that classes directories are included
    assert(classesInJar.contains("com/snowflake/snowpark/Session.class"))
  }

  test("Classes in different packages") {
    val className = "HelloWorld"
    val dummyCode =
      s"""
         | package test.com;
         |
         | public class $className {
         |   public static void test() {
         |   }
         | }
         |""".stripMargin

    val compiledClasses = (new JavaCodeCompiler).compile(Map(className -> dummyCode))
    val jarBuilder = new FatJarBuilder
    val tempFile = TestUtils.tempDirWithEscape + "output" + Random.nextInt() + ".jar"
    val jarStream = new JarOutputStream(new FileOutputStream(tempFile))
    jarBuilder.createFatJar(compiledClasses, List.empty, List.empty, Map.empty, jarStream)
    // Write File
    jarStream.close()

    // check file contents

    val classesInJar = listClassesInJar(tempFile)
    // Check that the compiled in-memory class is included
    assert(classesInJar.contains("test/com/HelloWorld.class"))
  }

  test("Duplicate classes fails fat jar creation") {
    val className = "Session"
    val dummyCode =
      s"""
         | package com.snowflake.snowpark;
         |
         | public class $className {
         |   public static void test() {
         |   }
         | }
         |""".stripMargin

    val compiledClasses = (new JavaCodeCompiler).compile(Map(className -> dummyCode))
    val jarBuilder = new FatJarBuilder
    val tempFile = TestUtils.tempDirWithEscape + "output" + Random.nextInt() + ".jar"
    val jarStream = new JarOutputStream(new FileOutputStream(tempFile))
    val exception = intercept[ZipException] {
      jarBuilder.createFatJar(compiledClasses, getClassesDirs, List.empty, Map.empty, jarStream)
    }
    assert(exception.getMessage.contains("duplicate entry"))
    assert(exception.getMessage.contains("Session.class"))
    jarStream.close()
  }

  def getJars: List[JarFile] = {
    val jars = new ArrayBuffer[JarFile]
    val scalaClass = classOf[scala.Product].getProtectionDomain.getCodeSource
    if (scalaClass != null) jars += new JarFile(new File(scalaClass.getLocation.toURI))
    jars.toList
  }

  def getClassesDirs: List[File] = {
    val dirs = new ArrayBuffer[File]
    val snClasses = classOf[Session].getProtectionDomain.getCodeSource
    if (snClasses != null) dirs += new File(snClasses.getLocation.toURI)
    dirs.toList
  }
}
