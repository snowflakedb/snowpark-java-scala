package com.snowflake.snowpark

import com.snowflake.snowpark.internal.Utils.clientPackageName

import java.io.{BufferedOutputStream, File, FileOutputStream}
import java.nio.file.{Files, NoSuchFileException}
import com.snowflake.snowpark.internal.{JavaUtils, UDFClassPath}
import sun.net.www.ParseUtil

import scala.reflect.internal.util.BatchSourceFile
import scala.reflect.io.{AbstractFile, VirtualDirectory}
import scala.tools.nsc.GenericRunnerSettings
import scala.tools.nsc.interpreter.IMain
import scala.util.Random

@UDFTest
class UDFRegistrationSuite extends SNTestBase with FileUtils {
  private val tempStage = this.getClass.getSimpleName + Random.alphanumeric.take(5).mkString("")
  private val stagePrefix = "udfJar"

  override def beforeAll(): Unit = {
    super.beforeAll()
    session.runQuery(s"create or replace temporary stage $tempStage")
  }

  test("Test URL encoding") {
    val inputs = Seq("dbiufwhronr==", "fdeswfirn--", "bsdij++", "sb#i", "bxsj%i@9h(nb)")
    inputs.foreach(str => assert(ParseUtil.decode(ParseUtil.encodePath(str)) == str))
  }

  test("Test that jar files are uploaded to stage correctly") {
    val udfRegistrar = new UDFRegistration(session)
    val classDirs = UDFClassPath.classDirs(session).toList
    val jarFileName = "udfJar" + Random.nextInt() + ".jar"
    val fileName = "Closure.class"
    val funcBytesMap = Map(fileName -> JavaUtils.serialize((x: Int) => x + x))
    udfRegistrar.handler.createAndUploadJarToStage(
      classDirs,
      tempStage,
      stagePrefix,
      jarFileName,
      funcBytesMap)
    val stageFile = "@" + tempStage + "/" + stagePrefix + "/" + jarFileName
    // Download file from stage
    session.runQuery(s"get $stageFile file://${TestUtils.tempDirWithEscape}")
    val classesInJar = listClassesInJar(s"${TestUtils.tempDirWithEscape}$jarFileName")
    // Check that classes in directories in UDFClasspath are included
    assert(
      classesInJar.contains("com/snowflake/snowpark/Session.class") || session.packageNames
        .contains(clientPackageName))
    // Check that classes in jars in UDFClasspath are NOT included
    assert(!classesInJar.contains("scala/Function1.class"))
    // Check that function class is included
    assert(classesInJar.contains(fileName))
  }

  test("negative test for UDFRegistration.createAndUploadJarToStage") {
    val udfRegistrar = new UDFRegistration(session)
    val jarFileName = "udfJar" + Random.nextInt() + ".jar"
    val fileName = "Closure.class"
    val funcBytesMap = Map(fileName -> JavaUtils.serialize((x: Int) => x + x))
    // read un-existed file
    val ex1 = intercept[Exception] {
      udfRegistrar.handler.createAndUploadJarToStage(
        List(new File("not_exist_file")),
        tempStage,
        stagePrefix,
        jarFileName,
        funcBytesMap)
    }
    assert(ex1.isInstanceOf[NoSuchFileException])

    // upload to un-existed stage
    val classDirs = UDFClassPath.classDirs(session).toList
    val ex2 = intercept[Exception] {
      udfRegistrar.handler.createAndUploadJarToStage(
        classDirs,
        "not_exist_stage_name",
        stagePrefix,
        jarFileName,
        funcBytesMap)
    }
    assert(
      ex2.getMessage.contains("Stage") &&
        ex2.getMessage.contains("does not exist or not authorized."))
  }

  // Dynamic Compile scala code
  private def generateDynamicClass(
      packageName: String,
      className: String,
      inMemory: Boolean): Class[_] = {
    // Generate a temp file for the scala code.
    val classContent =
      s"package $packageName\n class $className {\n class InnerClass {}\n}\nclass OuterClass {}\n"
    val tempDir = Files.createTempDirectory(s"snowpark_test_class_gen_").toFile
    val subDir = tempDir.getAbsolutePath + "/" + packageName
    new File(subDir).mkdirs()
    val fileName = subDir + "/" + className + ".scala"
    val writeStream = new BufferedOutputStream(new FileOutputStream(fileName))
    writeStream.write(classContent.getBytes())
    writeStream.close()

    // scalastyle:off println
    val settings: GenericRunnerSettings =
      new GenericRunnerSettings(err => println("Interpretor error: " + err))
    // scalastyle:on println
    settings.usejavacp.value = true
    if (inMemory) {
      settings.outputDirs.setSingleOutput(new VirtualDirectory(s"(memory)", None))
    } else {
      settings.Yreplclassbased.value = true
      val targetDir = Files.createTempDirectory(s"snowpark_test_target_")
      settings.Yreploutdir.value = targetDir.toFile.getAbsolutePath
    }
    val interpreter: IMain = new IMain(settings)
    interpreter.compileSources(new BatchSourceFile(AbstractFile.getFile(new File(fileName))))

    interpreter.classLoader.loadClass(s"$packageName.$className")
  }

  test("Test for addClassToDependencies(cls)") {
    val packageName = "com_snowflake_snowpark_test"

    val inMemoryName = s"DynamicCompile${Random.nextInt().abs}"
    val inMemoryClass = generateDynamicClass(packageName, inMemoryName, true)
    session.udf.handler.addClassToDependencies(inMemoryClass)

    val onDiskName = s"DynamicCompile${Random.nextInt().abs}"
    val onDiskClass = generateDynamicClass(packageName, onDiskName, false)
    session.udf.handler.addClassToDependencies(onDiskClass)
  }

  test("ls file") {
    val stageName = randomName()
    val specialName = s""""$stageName/aa""""
    try {
      createStage(stageName)
      uploadFileToStage(stageName, testFileAvro, compress = false)
      val files = session.listFilesInStage(stageName)
      assert(files.size == 1)
      assert(files.contains(testFileAvro))

      val fullName = session.getFullyQualifiedCurrentSchema + "." + stageName
      val files2 = session.listFilesInStage(fullName)
      assert(files2.size == 1)
      assert(files2.contains(testFileAvro))

      val prefix = "/prefix/prefix2"
      val withPrefix = stageName + prefix
      uploadFileToStage(withPrefix, testFileAvro, compress = false)
      val files3 = session.listFilesInStage(withPrefix)
      assert(files3.size == 1)
      assert(files3.contains(testFileAvro))

      val quotedName = s""""$stageName"$prefix"""
      val files4 = session.listFilesInStage(quotedName)
      assert(files4.size == 1)
      assert(files4.contains(testFileAvro))

      val fullNameWithPrefix = session.getFullyQualifiedCurrentSchema + "." + quotedName
      val files5 = session.listFilesInStage(fullNameWithPrefix)
      assert(files5.size == 1)
      assert(files5.contains(testFileAvro))

      createStage(specialName)
      uploadFileToStage(specialName, testFileCsv, compress = false)
      val files6 = session.listFilesInStage(specialName)
      assert(files6.size == 1)
      assert(files6.contains(testFileCsv))

    } finally {
      dropStage(stageName)
      dropStage(specialName)
    }
  }

}
