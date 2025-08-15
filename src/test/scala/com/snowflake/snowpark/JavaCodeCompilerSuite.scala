package com.snowflake.snowpark

import com.snowflake.snowpark.internal.{InMemoryClassObject, JavaCodeCompiler, UDFClassPath}
import org.scalatest.funsuite.AnyFunSuite

class JavaCodeCompilerSuite extends AnyFunSuite {

  test("Compile a class that requires scala in classpath") {
    val className = "HelloWorld"
    val code =
      s"""
         | public class $className {
         |   public static void testScala() {
         |      scala.Function0<Integer> funcImpl = null;
         |   }
         | }
         |""".stripMargin

    val compiler = new JavaCodeCompiler
    val ex = intercept[SnowparkClientException] {
      compiler.compile(Map(className -> code), List.empty)
    }
    assert(ex.message.contains("Error compiling your UDF code"))
    assert(ex.message.contains("package scala does not exist"))
    // Add scala in classpath
    compiler.compile(Map(className -> code), UDFClassPath.classpath.flatMap(_.location).toList)
  }

  test("Compile a simple HelloWorld class") {
    val className = "HelloWorld"
    val methodName = "test"
    val resultString = "Tests that our code to compile java code works!"
    val code =
      s"""
         | public class $className {
         |   public static String $methodName() {
         |      return "$resultString";
         |   }
         | }
         |""".stripMargin

    val compiler = new JavaCodeCompiler
    val compiledClasses = compiler.compile(Map(className -> code))
    val cl = new InMemoryClassLoader(compiledClasses)
    val clazz = cl.loadClass(className)
    val method = clazz.getMethod(methodName)
    val result = method.invoke(null)
    assert(result == resultString)
  }

  test("Error in code fails compilation") {
    val className = "TestClass"
    val code =
      s"""
         | public class $className {
         |   public static String returnsString() {
         |      // Don't return anything, should fail compilation
         |   }
         | }
         |""".stripMargin

    val compiler = new JavaCodeCompiler
    assertThrows[SnowparkClientException] {
      compiler.compile(Map(className -> code))
    }
  }
}

/*
 * This ClassLoader loads classes from an in-memory location.
 * Used only for tests for now to load a programmatically compiled class.
 */
class InMemoryClassLoader(files: List[InMemoryClassObject]) extends ClassLoader {
  val filesMap = files.map(f => (f.getClassName, f)).toMap

  override def findClass(name: String): Class[_] = {
    if (filesMap.contains(name)) {
      val classBytes = filesMap.get(name).get.getBytes() // TODO: scalify this
      super.defineClass(name, classBytes, 0, classBytes.length)
    } else {
      super.findClass(name)
    }
  }
}
