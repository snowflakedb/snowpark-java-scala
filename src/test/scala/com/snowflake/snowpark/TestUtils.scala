package com.snowflake.snowpark

import java.io.{BufferedWriter, File, FileInputStream, FileOutputStream, FileWriter}
import java.nio.file.Files
import java.sql.{Date, ResultSet, ResultSetMetaData, Statement, Time, Timestamp}
import java.util.jar.JarOutputStream
import com.snowflake.snowpark.RelationalGroupedDataFrame.GroupType
import com.snowflake.snowpark.internal.{
  FatJarBuilder,
  JavaCodeCompiler,
  Logging,
  ParameterUtils,
  ServerConnection,
  UDFClassPath,
  Utils
}
import com.snowflake.snowpark.internal.UDFClassPath.getPathForClass
import com.snowflake.snowpark.internal.analyzer.{quoteName, quoteNameWithoutUpperCasing}
import com.snowflake.snowpark.types._
import com.snowflake.snowpark_java.types.{InternalUtils, StructType => JavaStructType}
import org.scalatest.{BeforeAndAfterAll, Tag}

import java.util.{Locale, Properties}
import com.snowflake.snowpark.Session.loadConfFromFile
import com.snowflake.snowpark.internal.ParameterUtils.ClosureCleanerMode
import com.snowflake.snowpark.internal.Utils.TempObjectType
import net.snowflake.client.jdbc.{
  DefaultSFConnectionHandler,
  SnowflakeConnectString,
  SnowflakeConnectionV1
}

import java.security.Provider
import scala.collection.JavaConverters._
import scala.reflect.io.Directory
import scala.util.Random

object TestUtils extends Logging {

  val defaultProfile: String = "profile.properties"

  val fileSeparator: String = System.getProperty("file.separator")

  val tempDir: String = {
    val tempDirFile = new File(System.getProperty("java.io.tmpdir"))
    // On windows, the path returned by "java.io.tmpdir" may be shorten path like
    // C:\Users\RUNNER~1\AppData\Local\Temp, the actual path is
    // C:\Users\runneradmin\AppData\Local\Temp
    // There is bug in JDBC to support `~` in path.
    // It is necessary to use getCanonicalPath() to get the actual path.
    val tempDirName = tempDirFile.getCanonicalPath
    // Make sure the temp dir is ended with file.separator
    if (tempDirName.endsWith(fileSeparator)) {
      tempDirName
    } else {
      tempDirName + fileSeparator
    }
  }

  val tempDirWithEscape: String = escapePath(tempDir)

  def getFileName(path: String): String = new File(path).getName

  // If the directory name needs to be put in a SQL command,
  // It is necessary to escape the '\' on windows platform.
  def escapePath(path: String): String =
    if (Utils.isWindows) {
      path.replace("\\", "\\\\")
    } else {
      path
    }

  def randomStageName(): String =
    Utils.randomNameForTempObject(TempObjectType.Stage)

  def randomFunctionName(): String =
    Utils.randomNameForTempObject(TempObjectType.Function)

  def randomViewName(): String =
    Utils.randomNameForTempObject(TempObjectType.View)

  def randomTableName(): String =
    Utils.randomNameForTempObject(TempObjectType.Table)

  def randomString(n: Int): String = Random.alphanumeric.take(n).mkString("")

  def createTable(name: String, schema: String, session: Session): Unit =
    session.runQuery(s"create or replace table $name ($schema)")

  def createStage(name: String, isTemporary: Boolean, session: Session): Unit =
    session.runQuery(s"create or replace ${if (isTemporary) "temporary" else ""} stage $name")

  def dropStage(name: String, session: Session): Unit =
    session.runQuery(s"drop stage if exists $name")

  def dropTable(name: String, session: Session): Unit =
    session.runQuery(s"drop table if exists $name")

  def insertIntoTable(name: String, data: Seq[Any], session: Session): Unit =
    session.runQuery(s"insert into $name values ${data.map("(" + _.toString + ")").mkString(",")}")

  def insertIntoTable(name: String, data: java.util.List[Object], session: Session): Unit =
    insertIntoTable(name, data.asScala.map(_.toString), session)

  def uploadFileToStage(
      stageName: String,
      fileName: String,
      compress: Boolean,
      session: Session): Unit = {
    val input = getClass.getResourceAsStream(s"/$fileName")
    session.conn.uploadStream(stageName, null, input, fileName, compress)
  }

  // use random temporary stage if stageName is empty
  def addDepsToClassPath(
      sess: Session,
      stageName: Option[String],
      usePackages: Boolean = false): Unit = {
    // Initialize the lazy val so the defaultURIs are added to session
    sess.udf
    sess.udtf

    val snClassDir = UDFClassPath.getPathForClass(classOf[Session]).get
    sess.removeDependency(snClassDir)
    if (usePackages) {
      sess.removePackage(Utils.clientPackageName)
      sess.addPackage("com.snowflake:snowpark:latest")
    } else {
      // Replace scoverage classes in classpath because they are instrumented.
      sess.addDependency(snClassDir.replace("scoverage-", ""))
    }

    addTestDepsToClassPath(sess, stageName)
  }

  def addTestDepsToClassPath(sess: Session, stageName: Option[String]): Unit = {
    val stage: String = stageName.getOrElse {
      val name = randomStageName()
      sess.runQuery(s"CREATE TEMPORARY STAGE $name")
      name
    }

    List(
      classOf[BeforeAndAfterAll], // scala test jar
      classOf[org.scalactic.TripleEquals], // scalactic jar
      classOf[io.opentelemetry.exporters.inmemory.InMemorySpanExporter],
      classOf[io.opentelemetry.sdk.trace.export.SpanExporter])
      .flatMap(UDFClassPath.getPathForClass(_))
      .foreach(path => {
        val file = new File(path)
        sess.conn
          .uploadStream(stage, "", new FileInputStream(file), file.getName, compressData = false)
        sess.addDependency("@" + stage + "/" + file.getName)
      })
  }

  def addDepsToClassPathJava(sess: com.snowflake.snowpark_java.Session, stageName: String): Unit = {
    val stage: String =
      if (stageName != null) stageName
      else {
        val name = randomStageName()
        sess.sql(s"CREATE TEMPORARY STAGE $name").collect()
        name
      }
    // Initialize the lazy val so the defaultURIs are added to session
    sess.udf
    // Replace scoverage classes in classpath because they are instrumented.
    val snClassDir = UDFClassPath.getPathForClass(classOf[Session]).get
    sess.removeDependency(snClassDir)
    sess.addDependency(snClassDir.replace("scoverage-", ""))

    List(
      classOf[BeforeAndAfterAll], // scala test jar
      classOf[org.scalactic.TripleEquals], // scalactic jar
      classOf[io.opentelemetry.exporters.inmemory.InMemorySpanExporter],
      classOf[io.opentelemetry.sdk.trace.export.SpanExporter])
      .flatMap(UDFClassPath.getPathForClass(_))
      .foreach(path => {
        val file = new File(path)
        sess.sql(s"put file://$path @$stage/  AUTO_COMPRESS = FALSE").collect()
        sess.addDependency("@" + stage + "/" + file.getName)
      })
  }

  def addDepsToClassPath(sess: Session): Unit = addDepsToClassPath(sess, None)

  def runQueryReturnStatement(sql: String, session: Session): Statement = {
    val statement = session.conn.connection.createStatement()
    statement.executeQuery(sql)
    statement
  }

  def verifySchema(sql: String, expectedSchema: StructType, session: Session): Unit = {
    val statement = TestUtils.runQueryReturnStatement(sql, session)
    val resultMeta = statement.getResultSet.getMetaData
    val columnCount = resultMeta.getColumnCount

    assert(columnCount == expectedSchema.size)
    (0 until columnCount).foreach(index => {
      assert(
        quoteNameWithoutUpperCasing(resultMeta.getColumnLabel(index + 1)) == expectedSchema(
          index).columnIdentifier.quotedName)
      assert(
        (resultMeta.isNullable(index + 1) != ResultSetMetaData.columnNoNulls) == expectedSchema(
          index).nullable)
      assert(
        ServerConnection.getDataType(
          resultMeta.getColumnType(index + 1),
          resultMeta.getColumnTypeName(index + 1),
          resultMeta.getPrecision(index + 1),
          resultMeta.getScale(index + 1),
          resultMeta.isSigned(index + 1)) == expectedSchema(index).dataType)
    })
    statement.close()
  }

  def dropView(name: String, session: Session): Unit =
    session.runQuery(s"drop view if exists ${quoteName(name)}")

  def equalSFObjectName(name1: String, name2: String): Boolean =
    quoteName(name1) == quoteName(name2)

  def getSessionFromDataFrame(df: DataFrame): Session = df.session

  def getGroupTypeFromRGDF(rgdf: RelationalGroupedDataFrame): GroupType = rgdf.groupType

  def compare(obj1: Any, obj2: Any): Boolean = {
    val res = (obj1, obj2) match {
      case (null, null) => true
      case (null, _) => false
      case (_, null) => false
      case (a: Array[_], b: Array[_]) =>
        a.length == b.length && a.zip(b).forall { case (l, r) => compare(l, r) }
      case (a: Map[_, _], b: Map[_, _]) =>
        a.size == b.size && a.keys.forall { aKey =>
          b.keys.find(bKey => compare(aKey, bKey)).exists(bKey => compare(a(aKey), b(bKey)))
        }
      case (a: Iterable[_], b: Iterable[_]) =>
        a.size == b.size && a.zip(b).forall { case (l, r) => compare(l, r) }
      case (a: Product, b: Product) =>
        compare(a.productIterator.toSeq, b.productIterator.toSeq)
      case (a: Row, b: Row) =>
        compare(a.toSeq, b.toSeq)
      // Note this returns 0.0 and -0.0 as same
      case (a: BigDecimal, b) => compare(a.bigDecimal, b)
      case (a, b: BigDecimal) => compare(a, b.bigDecimal)
      case (a: Float, b) => compare(a.toDouble, b)
      case (a, b: Float) => compare(a, b.toDouble)
      case (a: Double, b: Double) if a.isNaN && b.isNaN => true
      case (a: Double, b: Double) => (a - b).abs < 0.0001
      case (a: Double, b: java.math.BigDecimal) => (a - b.toString.toDouble).abs < 0.0001
      case (a: java.math.BigDecimal, b: Double) => (a.toString.toDouble - b).abs < 0.0001
      case (a: java.math.BigDecimal, b: java.math.BigDecimal) =>
        // BigDecimal(1.2) isn't equal to BigDecimal(1.200), so can't use function
        // equal to verify
        a.subtract(b).abs().compareTo(java.math.BigDecimal.valueOf(0.0001)) == -1
      case (a: Date, b: Date) => a.toString == b.toString
      case (a: Time, b: Time) => a.toString == b.toString
      case (a: Timestamp, b: Timestamp) => a.toString == b.toString
      case (a: Geography, b: Geography) =>
        // todo: improve Geography.equals method
        a.asGeoJSON().replaceAll("\\s", "").equals(b.asGeoJSON().replaceAll("\\s", ""))
      case (a: Geometry, b: Geometry) =>
        a.toString.replaceAll("\\s", "").equals(b.toString.replaceAll("\\s", ""))
      case (a, b) => a == b
    }
    if (!res) {
      // scalastyle:off println
      println(
        s"Find different elements: " +
          s"$obj1${if (obj1 != null) s":${obj1.getClass.getSimpleName}" else ""} != " +
          s"$obj2${if (obj2 != null) s":${obj2.getClass.getSimpleName}" else ""}")
      // scalastyle:on println
    }
    res
  }

  def checkResult(result: Array[Row], expected: Seq[Row], sort: Boolean = true): Unit = {
    val sorted = if (sort) result.sortBy(_.toString) else result
    val sortedExpected = if (sort) expected.sortBy(_.toString) else expected
    assert(
      compare(sorted, sortedExpected.toArray[Row]),
      s"${sorted.map(_.toString).mkString("[", ", ", "]")} != " +
        s"${sortedExpected.map(_.toString).mkString("[", ", ", "]")}")
  }

  def checkResult(result: Array[Row], expected: java.util.List[Row], sort: Boolean): Unit =
    checkResult(result, expected.asScala, sort)

  def runQueryInSession(session: Session, sql: String): Unit =
    session.runQuery(sql)

  def fileExists(fileName: String): Boolean = {
    new java.io.File(fileName).exists()
  }

  def removeFile(fileName: String, session: Session): Unit = {
    val file = new java.io.File(fileName)
    if (file.exists()) {
      if (file.isDirectory) {
        // skip to remove directory in SP to avoid to call rmdir in sandbox
        if (!session.conn.isStoredProc) {
          new Directory(file).deleteRecursively()
        }
      } else {
        file.delete()
      }
    }
  }

  def writeFile(fileName: String, content: String, directory: File): Unit = {
    writeFile(fileName, content, directory.getAbsolutePath)
  }

  def writeFile(fileName: String, content: String, directory: String): Unit = {
    val file = new File(directory + "/" + fileName)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(content)
    bw.close()
  }

  def createTempJarFile(
      packageName: String,
      className: String,
      pathPrefix: String,
      jarFileName: String): String = {
    val dummyCode =
      s"""
         | package $packageName;
         |
         | public class $className {
         |   public static void test() {
         |   }
         | }
         |""".stripMargin

    val compiledClasses = (new JavaCodeCompiler).compile(Map(className -> dummyCode))
    val jarBuilder = new FatJarBuilder
    val tempDir = Files.createTempDirectory(pathPrefix).toFile.getCanonicalPath
    val jarFilePath = tempDir + fileSeparator + jarFileName
    val jarStream = new JarOutputStream(new FileOutputStream(jarFilePath))
    jarBuilder.createFatJar(compiledClasses, List.empty, List.empty, Map.empty, jarStream)
    jarStream.close()
    jarFilePath
  }

  private[snowpark] def getVersionProperty(property: String): Option[String] = {
    try {
      val properties = new Properties()
      properties.load(Utils.getClass.getClassLoader.getResourceAsStream("version.properties"))
      Option(properties.getProperty(property))
    } catch {
      case _: Exception =>
        None
    }
  }

  private[snowpark] def createJDBCConnection(propertyFile: String): SnowflakeConnectionV1 = {
    val options = loadConfFromFile(propertyFile).map { case (key, value) =>
      key.toLowerCase(Locale.ENGLISH) -> value
    }

    val connURL = ServerConnection.connectionString(options)
    val connParam = ParameterUtils.jdbcConfig(options, isScalaAPI = true)
    val connStr = SnowflakeConnectString.parse(connURL, connParam)
    new SnowflakeConnectionV1(new DefaultSFConnectionHandler(connStr), connURL, connParam)
  }

  // Snowflake JDBC FIPS requires client manually load bouncy-castle fips provider
  def tryToLoadFipsProvider(): Unit = {
    val isFipsTest = {
      System.getProperty("FIPS_TEST") match {
        case null => false
        case value if value.toLowerCase() == "true" => true
        case _ => false
      }
    }

    if (isFipsTest) {
      val provider = Class
        .forName("org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider")
        .getDeclaredConstructor()
        .newInstance()
        .asInstanceOf[Provider]
      java.security.Security.addProvider(provider)
      logInfo("Loaded BouncyCastleFipsProvider")
    } else {
      logInfo("FIPS_TEST != true, skip loading BouncyCastleFipsProvider")
    }
  }

  def containIgnoreCaseAndWhiteSpaces(data: String, keyword: String): Boolean = {
    data
      .replaceAll("\\W", "")
      .toLowerCase(Locale.ROOT)
      .contains(keyword.replaceAll("\\W", "").toLowerCase(Locale.ROOT))
  }

  def closureCleanerDisabled(session: Session): Boolean =
    session.closureCleanerMode == ClosureCleanerMode.never

  // Wrapper functions in order to do test in package: com.snowflake.snowpark_test
  def treeString(schema: StructType, layer: Int): String = schema.treeString(layer)
  def isAtomicType(tpe: DataType): Boolean = tpe.isInstanceOf[AtomicType]
  def isNumericType(tpe: DataType): Boolean = tpe.isInstanceOf[NumericType]
  def isIntegralType(tpe: DataType): Boolean = tpe.isInstanceOf[IntegralType]
  def isFractionalType(tpe: DataType): Boolean = tpe.isInstanceOf[FractionalType]
  def treeString(schema: JavaStructType, layer: Int): String =
    InternalUtils.toScalaStructType(schema).treeString(layer)
}

// Used to mark test as unstable and run in a separate github action test
object UnstableTest extends Tag("UnstableTest")

// Tests using SNOWFLAKE_SAMPLE_DATA, it may be not available on some test deployments
object SampleDataTest extends Tag("SampleDataTest")

// Run in stored procs tests only on AWS
object JavaStoredProcAWSOnly extends Tag("JavaStoredProcAWSOnly")

// Used to exclude test in Java Stored Proc in both caller's right sp and owner's right sp
object JavaStoredProcExclude extends Tag("JavaStoredProcExclude")

// Used to exclude test in Java Stored Proc in owner's right sp
object JavaStoredProcExcludeOwner extends Tag("JavaStoredProcExcludeOwner")
