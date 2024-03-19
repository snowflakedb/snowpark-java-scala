package com.snowflake.snowpark

import java.io.{BufferedReader, OutputStreamWriter, StringReader, PrintWriter => JPrintWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardCopyOption}
import com.snowflake.snowpark.internal.Utils

import scala.tools.nsc.Settings
import scala.tools.nsc.util.stringFromStream
import scala.sys.process._
import scala.tools.nsc.interpreter.shell.{ILoop, ShellConfig}

@UDFTest
class ReplSuite extends TestData {

  val replClassesInMemoryMessage = "Found REPL classes in memory"
  val replClassesOnDiskMessage = "Found REPL classes on disk"

  // scalastyle:off
  val preLoad =
    s"""
       |import com.snowflake.snowpark._
       |import com.snowflake.snowpark.functions._
       |import com.snowflake.snowpark.internal.UDFClassPath
       |import scala.reflect.internal.util.AbstractFileClassLoader
       |import scala.reflect.io.{AbstractFile, VirtualDirectory}
       |val classLoader = this.getClass.getClassLoader
       |if (classLoader.isInstanceOf[AbstractFileClassLoader]) {
       |  val rootDirectory = classLoader.asInstanceOf[AbstractFileClassLoader].root
       |  if (rootDirectory.isInstanceOf[VirtualDirectory]) {
       |    println("$replClassesInMemoryMessage")
       |  } else {
       |    println("$replClassesOnDiskMessage")
       |  }
       |}
       |val session = Session.builder.configFile("$defaultProfile").create
       |session.udf
       |val snClassDir = UDFClassPath.getPathForClass(classOf[Session]).get
       |session.removeDependency(snClassDir)
       |session.addDependency(snClassDir.replace("scoverage-", ""))
       |""".stripMargin
  // scalastyle:on

  // Use run(code) to run test with scala REPL.
  private def run(code: String, inMemory: Boolean = false): String = {
    stringFromStream { outputStream =>
      Console.withOut(outputStream) {
        val input = new BufferedReader(new StringReader(preLoad + code))
        val output = new JPrintWriter(new OutputStreamWriter(outputStream))
        val settings = new Settings()
        if (inMemory) {
          settings.processArgumentString("-Yrepl-class-based")
        } else {
          settings.processArgumentString("-Yrepl-class-based -Yrepl-outdir repl_classes")
        }
        settings.classpath.value = sys.props("java.class.path")
        val repl = new ILoop(ShellConfig(settings), input, output)
        repl.run(settings)
      }
    }.replaceAll("scala> ", "")
  }

  // Compile only once for this suite
  private lazy val compileAndGenerateWorkDir = {
    val workDir = s"./target/snowpark-${Utils.Version}"
    assert("mvn package -DskipTests -Dgpg.skip".! == 0)
    assert(s"tar -xf $workDir-bundle.tar.gz -C ./target".! == 0)
    workDir
  }

  // Use runWithCompiledJar(code) to compile the project and run test with the built Snowflake jar.
  private def runWithCompiledJar(code: String) = {
    val workDir = compileAndGenerateWorkDir
    Files.copy(
      Paths.get(defaultProfile),
      Paths.get(s"$workDir/$defaultProfile"),
      StandardCopyOption.REPLACE_EXISTING)
    Files.write(
      Paths.get(s"$workDir/file.txt"),
      (preLoad + code + "sys.exit\n").getBytes(StandardCharsets.UTF_8))
    s"cat $workDir/file.txt ".#|(s"$workDir/run.sh").!!.replaceAll("scala> ", "")
  }

  test("basic udf test") {
    val table1 = randomName()
    val lines =
      s"""
         |session.sql("create or replace temp table ${table1}(a int)").show()
         |val df = session.table("$table1")
         |val doubleUDF = udf((x: Int) => x + x)
         |df.select(doubleUDF(col("a"))).show()
         |""".stripMargin

    val result = run(lines)
    assert(!result.contains("Exception"))
    assert(!result.contains("error"))
    assert(result.contains(replClassesOnDiskMessage))
  }

  test("basic udf test in memory") {
    val table1 = randomName()
    val lines =
      s"""
         |session.sql("create or replace temp table ${table1}(a int)").show()
         |val df = session.table("$table1")
         |val doubler = (x: Int) => x + x
         |val doubleUDF = udf(doubler)
         |df.select(doubleUDF(col("a"))).show()
         |val doubleUDF2 = udf((x: Int) => x + x)
         |df.select(doubleUDF2(col("a"))).show()
         |""".stripMargin

    val result = run(lines, true)
    assert(!result.contains("Exception"))
    assert(!result.contains("error"))
    assert(result.contains(replClassesInMemoryMessage))
  }

  test("UDF with multiple args of type map, array etc") {
    val table = randomName()
    // scalastyle:off
    val code =
      s"""
         |import scala.collection.mutable
         |session.sql("create or replace temp table $table (o1 object, o2 object, id varchar)").show()
         |session.sql("insert into $table (select object_construct('1','one','2','two'), object_construct('one', '10', 'two', '20'), 'ID1')").show()
         |session.sql("insert into $table (select object_construct('3','three','4','four'), object_construct('three', '30', 'four', '40'), 'ID2')").show()
         |val df = session.table("$table")
         |val mapUdf = udf((map1: mutable.Map[String, String], map2: mutable.Map[String, String], id: String) => {
         |val values = map1.values.map(v => map2.get(v))
         |val res = values.filter(_.isDefined).map(_.get.toInt).reduceLeft(_ + _)
         |mutable.Map(id -> res.toString)
         |})
         |val res = df.select(mapUdf(col("o1"), col("o2"), col("id"))).collect
         |assert(res.size == 2)
         |assert(res(0).getString(0).contains(\"\"\"\"ID1\": \"30\"\"\"\"))
         |assert(res(1).getString(0).contains(\"\"\"\"ID2\": \"70\"\"\"\"))
         |""".stripMargin
    // scalastyle:on
    val result = run(code)
    assert(!result.contains("Exception"))
    assert(!result.contains("error"))
  }

  // the following tests are unstable on Github action,
  // because they re-download dependencies from Maven,
  // this process may be failed due to network issue.

  test("UDF with Geography", UnstableTest) {
    val table1 = randomName()
    // scalastyle:off
    val lines =
      s"""
        |import com.snowflake.snowpark.types.{Geography, Variant}
        |import org.apache.commons.codec.binary.Hex
        |val geographyUDF = udf((g: Geography) => {if (g == null) {null} else {if (g.asGeoJSON().equals("{\\"coordinates\\":[50,60],\\"type\\":\\"Point\\"}")){Geography.fromGeoJSON(g.asGeoJSON())} else {Geography.fromGeoJSON(g.asGeoJSON().replace("0", ""))}}})
        |session.sql("create or replace table $table1(geo geography)").show()
        |session.sql("insert into $table1 values ('POINT(30 10)'), ('POINT(50 60)'), (null)").show()
        |val df = session.table("$table1")
        |df.select(geographyUDF(col("geo"))).show()
        |""".stripMargin
    // scalastyle:on
    val result = runWithCompiledJar(lines)
    print(result)
    assert(!result.contains("Exception"))
    assert(!result.contains("error"))
  }

  test("UDF with Variant", UnstableTest) {
    val table1 = randomName()
    // scalastyle:off
    val lines =
      s"""
         |import java.sql.{Date, Time, Timestamp}
         |import com.snowflake.snowpark.types.Variant
         |lazy val variant1: DataFrame = session.sql("select to_variant(to_timestamp_ntz('2017-02-24 12:00:00.456')) as timestamp_ntz1")
         |val variantTimestampUDF = udf((v: Variant) => {new Timestamp(v.asTimestamp().getTime + 5000)})
         |variant1.select(variantTimestampUDF(col("timestamp_ntz1"))).show()
         |""".stripMargin
    // scalastyle:on
    val result = runWithCompiledJar(lines)
    print(result)
    assert(!result.contains("Exception"))
    assert(!result.contains("error"))
  }

  test("UDF with Closure Cleaner", UnstableTest) {
    // scalastyle:off
    val lines =
      s"""
         |class NonSerializable(val id: Int = -1) {
         |  override def hashCode(): Int = id
         |  override def equals(other: Any): Boolean = {
         |    other match {
         |      case o: NonSerializable => id == o.id
         |      case _ => false
         |    }
         |  }
         |}
         |object TestClassWithoutFieldAccess extends Serializable {
         |  val nonSer = new NonSerializable
         |  val x = 5
         |  val run = (a: String) => {
         |    x
         |  }
         |}
         |val myDf = session.sql("select 'Raymond' NAME")
         |val readFileUdf = udf(TestClassWithoutFieldAccess.run)
         |myDf.withColumn("CONCAT", readFileUdf(col("NAME"))).show()
         |""".stripMargin
    // scalastyle:on
    val result = runWithCompiledJar(lines)
    print(result)
    assert(!result.contains("Exception"))
    assert(!result.contains("error"))
  }
}
