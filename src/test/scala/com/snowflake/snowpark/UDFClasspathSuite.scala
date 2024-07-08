package com.snowflake.snowpark

import java.io.File
import java.net.URLClassLoader
import com.snowflake.snowpark.internal.ScalaFunctions._toUdf
import com.snowflake.snowpark.internal.Utils.clientPackageName
import com.snowflake.snowpark.internal.{UDFClassPath, UDXRegistrationHandler, Utils}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{atLeastOnce, never, reset, spy, times, verify}

import scala.concurrent.duration.{FiniteDuration, MINUTES}
import scala.concurrent.Await
import scala.util.Random

@UDFTest
class UDFClasspathSuite extends SNTestBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    TestUtils.addDepsToClassPath(session)
  }

  test("Test that jars uploaded to different stages") {
    val mockSession = spy(session)
    val jarInClassPath = session.getLocalFileDependencies.filter(new File(_).isFile)

    assert(jarInClassPath.nonEmpty)

    val stageName1 = randomName()
    mockSession.runQuery(s"create or replace temp stage $stageName1")
    mockSession
      .resolveJarDependencies(stageName1)
      .foreach(Await.ready(_, FiniteDuration(1, MINUTES)))
    assert(mockSession.listFilesInStage(stageName1).size == jarInClassPath.size)

    val stageName2 = randomName()
    mockSession.runQuery(s"create or replace temp stage $stageName2")
    mockSession
      .resolveJarDependencies(stageName2)
      .foreach(Await.ready(_, FiniteDuration(1, MINUTES)))
    assert(mockSession.listFilesInStage(stageName2).size == jarInClassPath.size)
    reset(mockSession)

    // try second time
    mockSession
      .resolveJarDependencies(stageName1)
      .foreach(Await.ready(_, FiniteDuration(1, MINUTES)))
    assert(mockSession.listFilesInStage(stageName1).size == jarInClassPath.size)
    // Assert that no dependencies are uploaded in second time
    verify(mockSession, never()).doUpload(any(), any())
    succeed
  }

  test("Test that udf function's class path is automatically added") {
    val path = UDFClassPath.getPathUsingCodeSource(this.getClass).get
    val mockSession = spy(session)
    val udfR = new UDXRegistrationHandler(mockSession)
    val func = "func_" + Random.nextInt().abs
    udfR.registerUDF(Some(func), _toUdf((a: Int) => a + a), None)
    verify(mockSession, atLeastOnce())
      .addDependency(path)
    succeed
  }

  test("Test that snowpark jar is NOT uploaded if stage path is available") {
    val mockSession = spy(session)
    val randomStage = randomStageName()
    session.runQuery(s"CREATE TEMPORARY STAGE $randomStage")
    val udfR = spy(new UDXRegistrationHandler(mockSession))
    val path = UDFClassPath.getPathForClass(classOf[com.snowflake.snowpark.Session]).get
    mockSession.removeDependency(path)
    val jarFileName = "snowpark_0.1.jar"
    // We use scoverage plugin to measure test coverage and scoverage generates instrumented
    // classes. For UDFs though, we want to use the non-instrumented snowpark classes.
    val fixedPath = path.replace("scoverage-", "")
    udfR.createAndUploadJarToStage(
      List(new File(fixedPath)),
      randomStage,
      "",
      jarFileName,
      Map.empty)
    mockSession.addDependency("@" + randomStage + "/" + jarFileName)
    val func = "func_" + Random.nextInt().abs
    udfR.registerUDF(Some(func), _toUdf((a: Int) => a + a), None)
    val expectedPath = new File(path).getAbsolutePath
    verify(mockSession, never())
      .addDependency(expectedPath)
    // createJavaUDF should be only invoked once
    verify(udfR, times(1)).createJavaUDF(any(), any(), any(), any(), any(), any(), any())
    succeed
  }

  test(
    "Test that snowpark jar is automatically added" +
      " if there is classNotFound error in first attempt") {
    val newSession = Session.builder.configFile(defaultProfile).create
    TestUtils.addDepsToClassPath(newSession)
    val udfR = spy(new UDXRegistrationHandler(newSession))
    val path = UDFClassPath.getPathForClass(classOf[com.snowflake.snowpark.Session]).get
    val fixedPath = path.replace("scoverage-", "")
    // Remove snowpark jar from classpath, The code will catch error and add the path and retry
    newSession.removeDependency(fixedPath)
    newSession.removePackage("com.snowflake:snowpark:latest")
    newSession.removePackage(clientPackageName)
    val func = "func_" + Random.nextInt().abs

    ignoreClassNotFoundForScoverageClasses {
      udfR.registerUDF(Some(func), _toUdf((a: Int) => a + a), None)
    }
    // createJavaUDF should be invoked twice as it is retried after fixing classpath
    verify(udfR, times(2)).createJavaUDF(any(), any(), any(), any(), any(), any(), any())
    succeed
  }

  test("Test for getPathForClass") {
    val clazz = classOf[scala.StringBuilder]
    val expectedUrl = clazz.getProtectionDomain.getCodeSource.getLocation.getPath
    val result = UDFClassPath.getPathForClass(clazz)
    assert(result.isDefined && result.get.equals(expectedUrl))
  }

  test("test jar path has special characters") {
    val className = "TestPathIncludeSpace" + Random.nextInt().abs
    val packageName = "com.snowflake.snowpark.test"
    val prefix = "snowpark_test +#@%20_"
    val jarFilePath = TestUtils.createTempJarFile(packageName, className, prefix, "test.jar")
    val child =
      new URLClassLoader(Array(new File(jarFilePath).toURI.toURL), this.getClass.getClassLoader)
    val classToLoad = Class.forName(s"$packageName.$className", true, child)
    val foundJarPath = UDFClassPath.getPathUsingClassLoader(classToLoad)
    assert(foundJarPath.nonEmpty)
    assert(new File(foundJarPath.get).exists())

    val expectedUrl = if (Utils.isWindows) {
      // On windows, the directory format is gotten from getCodeSource() is like:
      // /C:/Users/runneradmin/.../scala-library-2.12.11.jar
      // The directory gotten with UDFClassPath.getPathForClass(clazz) is like:
      // C:\Users\runneradmin\...\scala-library-2.12.11.jar
      // So, need format conversion for result comparison.
      "/" + jarFilePath.replace("\\", "/")
    } else {
      jarFilePath
    }
    assert(foundJarPath.get.equals(expectedUrl))
    assert(foundJarPath.get.contains(prefix))
  }

  private def ignoreClassNotFoundForScoverageClasses[T](func: => Unit): Unit = {
    try {
      func
    } catch {
      case e: Exception =>
        val msg = e.getMessage
        if (!msg.contains("NoClassDefFoundError: scoverage/")) {
          throw e
        }
    }
  }
}
